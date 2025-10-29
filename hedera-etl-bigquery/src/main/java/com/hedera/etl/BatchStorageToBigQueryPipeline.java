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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;

import com.hedera.etl.diff.HistoryUtil;
import com.hedera.etl.reader.recordfile.domain.StreamFilename;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.reader.record.RecordFileReader;
import com.hedera.etl.recordfile.RecordFileTransform;

import static com.hedera.etl.recordfile.SignedRecordFilesHandler.getNodeFromUrl;

@RequiredArgsConstructor
public class BatchStorageToBigQueryPipeline {

  private final BatchStorageToBigQueryPipelineOptions options;

  void run() {
    var pipeline = Pipeline.create(options);

    PCollection<RecordFile> files;

    if (!StringUtils.isBlank(options.getInputFileList())) {
      var lines =
          pipeline
              .apply("Read input file list", TextIO.read().from(options.getInputFileList()))
              .apply("Reshuffle input list", Reshuffle.viaRandomKey());
      files =
          lines
              .apply(
                  "List files",
                  FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
              .apply("Rewind watermark to earliest", globalWindowWithEarliestTimestamps())
              .apply("Read files", FileIO.readMatches())
              .apply(
                  "Map to record files",
                  MapElements.into(TypeDescriptor.of(RecordFile.class))
                      .via(
                          file -> {
                            RecordFile recordFile = null;
                            try {
                              recordFile =
                                  RecordFileReader.INSTANCE.read(
                                      StreamFilename.from(
                                          file.getMetadata().resourceId().toString()),
                                      file.readFullyAsBytes());
                            } catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                            recordFile.setNodeId(
                                EntityId.of(
                                        getNodeFromUrl(file.getMetadata().resourceId().toString()))
                                    .getId());
                            return recordFile;
                          }));
    } else {
      files =
          pipeline
              .apply("Get patterns", Create.of(options.getInputPathPatterns()))
              .apply("List files", FileIO.matchAll())
              .apply("Rewind watermark to earliest", globalWindowWithEarliestTimestamps())
              .apply(
                  "Read files",
                  new RecordFileTransform(options.getStartAboveFile(), options.getLastValidHash()));
    }

    var entityCollection = EntitiesExtractor.extract(files, options.getEnabledOutputs());

    saveAllToBigQuery(entityCollection.restrictedAccess(), options.getRestrictedAccessDataset());
    saveAllToBigQuery(entityCollection.openAccess(), options.getOpenAccessDataset());
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
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS);

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

      WriteResult writeResult = rowPCollection.apply("Save %s to BigQuery".formatted(name), writer);
    }
  }
}
