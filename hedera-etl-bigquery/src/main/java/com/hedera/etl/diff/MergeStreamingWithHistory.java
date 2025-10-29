package com.hedera.etl.diff;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Strings;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import com.hedera.etl.BatchStorageToBigQueryPipelineOptions;
import com.hedera.etl.StreamingStorageToBigQueryPipelineOptions;
import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.diff.Merge.DIFFS;
import static com.hedera.etl.diff.Merge.LATEST;
import static com.hedera.etl.diff.Merge.UPDATED;

public class MergeStreamingWithHistory extends PTransform<PCollection<Row>, PCollectionTuple> {

  private static final LocalDate FIRST_ENTRIES_DATE = LocalDate.of(2019, 9, 13);

  private final String entityName;

  private final String timestampField;

  private final MergeStreaming merger;

  public static MergeStreamingWithHistory diffs(
      String entityName, String idField, String timestampField) {
    return new MergeStreamingWithHistory(
        entityName, timestampField, MergeStreaming.diffs(idField, timestampField));
  }

  public static MergeStreamingWithHistory sum(
      String entityName, String idField, String timestampField, String summableField) {
    return new MergeStreamingWithHistory(
        entityName, timestampField, MergeStreaming.sum(idField, timestampField, summableField));
  }

  public static MergeStreamingWithHistory append(
      String entityName,
      String idField,
      String timestampField,
      String appendableField,
      String actionField) {
    return new MergeStreamingWithHistory(
        entityName,
        timestampField,
        MergeStreaming.append(idField, timestampField, appendableField, actionField));
  }

  private MergeStreamingWithHistory(
      String entityName, String timestampField, MergeStreaming merger) {
    this.entityName = entityName;
    this.timestampField = timestampField;
    this.merger = merger;
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    var options =
        Optional.ofNullable(input.getPipeline().getOptions())
            .filter(opts -> opts instanceof StreamingStorageToBigQueryPipelineOptions)
            .map(opts -> (StreamingStorageToBigQueryPipelineOptions) opts);

    var latestValues =
        options
            .map(
                opts -> {
                  final var ingestionDate = opts.getIngestionDate();

                  return loadLatestValues(input, opts)
                      .apply(
                          "Add timestamps to latest data",
                          WithTimestamps.<Row>of(
                                  _row -> FormatUtils.jodaInstantFromLocalDate(ingestionDate))
                              .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)));
                })
            .orElseGet(() -> input.getPipeline().apply(Create.empty(input.getCoder())));

    var rowsWithUpdates =
        input
            .apply(
                "Add timestamps to current data",
                WithTimestamps.<Row>of(
                        row -> FormatUtils.jodaInstantFromNanos(row.getInt64(timestampField)))
                    .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
            .apply("Union of current and historical data", Flatten.with(latestValues))
            .apply("Merge diffs", merger);

    return options
        .map(
            opts -> {
              var filteredRows = filterRowsBeforeIngestionDate(rowsWithUpdates, opts);
              saveLatestValues(rowsWithUpdates.get(LATEST), opts);
              return filteredRows;
            })
        .orElse(rowsWithUpdates);
  }

  private PCollectionTuple filterRowsBeforeIngestionDate(
      PCollectionTuple tuple, BatchStorageToBigQueryPipelineOptions opts) {
    final var ingestionDate = opts.getIngestionDate();

    var timestampField = "created";
    if (tuple.get(UPDATED).getSchema().hasField("modified")) {
      timestampField = "modified";
    }

    return PCollectionTuple.empty(tuple.getPipeline())
        .and(
            UPDATED,
            tuple
                .get(UPDATED)
                .apply(
                    "Filter out updated rows before ingestion start",
                    filterRowsBeforeIngestionDatePredicate(ingestionDate, timestampField)))
        .and(
            DIFFS,
            tuple
                .get(DIFFS)
                .apply(
                    "Filter out diff rows before ingestion start",
                    filterRowsBeforeIngestionDatePredicate(ingestionDate, timestampField)))
        .and(LATEST, tuple.get(LATEST));
  }

  private Filter<Row> filterRowsBeforeIngestionDatePredicate(
      LocalDate ingestionDate, String timestampField) {
    final var ingestionAtStartOfDay = ingestionDate.atStartOfDay().toInstant(ZoneOffset.UTC);

    return Filter.by(
        row -> {
          var instant =
              Optional.ofNullable(row.getString(timestampField))
                  .map(Instant::parse)
                  .orElse(Instant.EPOCH);

          return ingestionAtStartOfDay.isBefore(instant);
        });
  }

  private PCollection<Row> loadLatestValues(
      PCollection<Row> input, BatchStorageToBigQueryPipelineOptions options) {
    var pipeline = input.getPipeline();

    LocalDate previousIngestDate;

    if (Strings.isNullOrEmpty(options.getStartAboveFile())) {
      previousIngestDate = options.getIngestionDate().minusDays(1);
    } else {
      // if start above file is not null it means that latest data is for current day
      previousIngestDate = options.getIngestionDate();
    }
    if (previousIngestDate.isBefore(FIRST_ENTRIES_DATE) || options.getDisableMergeHistoryInput()) {
      return pipeline.apply("Load latest values", Create.empty(input.getCoder()));
    }

    final var schema = input.getSchema();

    return pipeline
        .apply(
            "Load latest values for %s".formatted(entityName),
            BigQueryIO.readTableRowsWithSchema()
                .from(
                    HistoryUtil.getTableFor(
                        options.getTechnicalDataset(),
                        "%s_latest".formatted(entityName),
                        previousIngestDate)))
        .apply(
            "Convert to rows",
            MapElements.into(TypeDescriptors.rows())
                .via(tableRow -> BigQueryUtils.toBeamRow(schema, tableRow)))
        .setCoder(input.getCoder());
  }

  private void saveLatestValues(
      PCollection<Row> input, BatchStorageToBigQueryPipelineOptions options) {
    final var dataset = options.getTechnicalDataset();

    input
        .apply("Filter out nulls", Filter.by(Objects::nonNull))
        .apply(
            "Save latest to BigQuery",
            BigQueryIO.<Row>write()
                .to(
                    element -> {
                      var tableRef =
                          HistoryUtil.getTableFor(
                              dataset,
                              "%s_latest".formatted(entityName),
                              LocalDate.ofInstant(
                                  java.time.Instant.ofEpochMilli(
                                      element.getTimestamp().getMillis()),
                                  ZoneId.systemDefault()));

                      return new TableDestination(
                          tableRef, "Table for %s".formatted(tableRef.getTableId()));
                    })
                .ignoreUnknownValues()
                .useBeamSchema()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withoutValidation()
                //            .withTriggeringFrequency(Duration.standardHours(1))
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));
  }
}
