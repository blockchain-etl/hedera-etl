package com.hedera.etl;

/*-
 * ‌
 * Hedera ETL
 * ​
 * Copyright (C) 2020 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

import com.hedera.etl.diff.HistoryUtil;
import com.hedera.etl.recordfile.RecordFileTransform;

@Log4j2
@RequiredArgsConstructor
public class StreamingStorageToBigQueryPipeline {

  private final StreamingStorageToBigQueryPipelineOptions options;

  void run() {
    var pipeline = Pipeline.create(options);

    final var inputBucket = options.getInputBucket();
    final var nodes = options.getInputNodes();
    final var startingTimestamp = options.getStartingTimestamp();
    final var rewindToTimestamp = options.getRewindToTimestamp();

    final var delay = new Duration(rewindToTimestamp, Instant.now());

    var files =
        pipeline
            .apply(
                "Tick every minute",
                GenerateSequence.from(0)
                    .withRate(1, Duration.standardMinutes(1))
                    .withTimestampFn(
                        i -> {
                          if (i == 0) {
                            return startingTimestamp
                                .toDateTime()
                                .minuteOfDay()
                                .roundFloorCopy()
                                .toInstant();
                          } else {
                            var now = DateTime.now().minuteOfDay().roundFloorCopy().toInstant();
                            if (rewindToTimestamp == null) {
                              return now;
                            } else {
                              return now.minus(delay);
                            }
                          }
                        }))
            .apply("Add fake KV", WithKeys.of(0))
            .apply(
                "Generate file prefixes by timestamp",
                ParDo.of(new GenerateFileNamesPrefixesByTimestamp(startingTimestamp)))
            .apply(
                "Build GCS prefix",
                FlatMapElements.into(TypeDescriptors.strings())
                    .via(
                        prefix ->
                            nodes.stream()
                                .map(
                                    node ->
                                        "gs://"
                                            + inputBucket
                                            + "/recordstreams/record"
                                            + node
                                            + "/"
                                            + prefix
                                            + '*')
                                .peek(glob -> log.warn("Listening for files matching {}", glob))
                                .toList()))
            .apply(
                "List files",
                FileIO.matchAll()
                    .continuously(
                        Duration.standardSeconds(60),
                        Watch.Growth.afterTotalOf(
                            Duration.standardMinutes(options.getFilePollingTimeout()))))
            .apply(
                "Read files",
                new RecordFileTransform(options.getStartAboveFile(), options.getLastValidHash()));

    var entityCollection = EntitiesExtractor.extract(files);

    saveAllToBigQuery(entityCollection.openAccess(), options.getOpenAccessDataset());
    saveAllToBigQuery(entityCollection.restrictedAccess(), options.getRestrictedAccessDataset());
    saveAllToBigQuery(entityCollection.technical(), options.getTechnicalDataset());

    pipeline.run();
  }

  <T> PTransform<PCollection<T>, PCollection<T>> globalWindowWithEarliestTimestamps() {
    return Window.<T>into(new GlobalWindows()).withTimestampCombiner(TimestampCombiner.EARLIEST);
  }

  void saveAllToBigQuery(Map<String, PCollection<Row>> input, String dataset) {
    for (var entry : input.entrySet()) {
      var name = entry.getKey();
      var tableId = name.toLowerCase();
      var rowPCollection = entry.getValue();

      var outputTable = new TableReference().setDatasetId(dataset).setTableId(tableId);

      final var isDiffTable =
          outputTable.getTableId().endsWith("_diffs")
              || Set.of("native_token_transfer", "token_transfer").contains(tableId);

      var createDisposition =
          Set.of("hedera_public", "hedera_restricted", "hedera_technical").contains(dataset)
              ? (isDiffTable
                  ? BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
                  : BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
              : BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

      var writeDisposition =
          outputTable.getTableId().endsWith("_latest")
              ? BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
              : BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

      if (outputTable.getTableId().endsWith("_latest")) {
        continue;
      }

      var writer =
          BigQueryIO.<Row>write()
              .ignoreUnknownValues()
              .withCreateDisposition(createDisposition)
              .withWriteDisposition(writeDisposition)
              .withoutValidation()
              .withExtendedErrorInfo()
              //              .withTriggeringFrequency(Duration.standardMinutes(1))
              .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);

      if (isDiffTable) {
        final var partitioningField =
            rowPCollection.getSchema().getFieldNames().contains("modified")
                ? "modified"
                : "created";

        writer =
            writer
                .to(
                    value -> {
                      var timestamp = value.getValue().getString(partitioningField);

                      var table = HistoryUtil.getTableForYear(dataset, tableId, timestamp);

                      var partitioning =
                          new TimePartitioning()
                              .setField(partitioningField)
                              .setType("HOUR")
                              .setRequirePartitionFilter(true);

                      return new TableDestination(
                          table, "Technical table containing " + tableId + " data", partitioning);
                    })
                .withFormatFunction(BigQueryUtils::toTableRow)
                .withJsonSchema(
                    "{\"fields\":"
                        + Utility.getResource("schemas/" + outputTable.getTableId() + ".json")
                        + "}");
      } else {
        writer = writer.to(outputTable).useBeamSchema();
      }

      WriteResult writeResult =
          rowPCollection
              .apply("Filter out nulls", Filter.by(Objects::nonNull))
              .apply("Save %s.%s to BigQuery".formatted(dataset, tableId), writer);
      writeResult
          .getFailedInsertsWithErr()
          .apply(
              "Log errors",
              Filter.by(
                  err -> {
                    log.error(
                        "Failed to insert into table row: {}\nReason:\n{}",
                        err.getRow(),
                        err.getError());
                    return true;
                  }));
    }
  }

  public static class GenerateFileNamesPrefixesByTimestamp extends DoFn<KV<Integer, Long>, String> {
    private final Instant lastTimestamp;

    @StateId("last_timestamp")
    private final StateSpec<ValueState<Instant>> lastTimestampState = StateSpecs.value();

    public GenerateFileNamesPrefixesByTimestamp(Instant lastTimestamp) {
      this.lastTimestamp = lastTimestamp;
    }

    @ProcessElement
    public void processElement(
        @Timestamp Instant elementTs,
        @StateId("last_timestamp") ValueState<Instant> lastTsState,
        OutputReceiver<String> out) {
      var lastTs = lastTsState.read();
      lastTs = lastTs == null ? lastTimestamp : lastTs;

      var duration = new Duration(lastTs, elementTs);
      var minutes = duration.getStandardMinutes();
      if (minutes > 0) {
        for (int i = 0; i < minutes; i++) {
          var ts = lastTs.plus(Duration.standardMinutes(i));
          out.outputWithTimestamp(ts.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH_mm")), ts);
        }
        lastTsState.write(elementTs);
      }
    }

    @NotNull @Override
    public Duration getAllowedTimestampSkew() {
      return new Duration(Long.MAX_VALUE);
    }
  }
}
