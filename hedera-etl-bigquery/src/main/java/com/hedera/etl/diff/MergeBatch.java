package com.hedera.etl.diff;

import com.hedera.etl.BatchStorageToBigQueryPipelineOptions;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import java.time.LocalDate;

public class MergeBatch extends PTransform<PCollection<Row>, PCollectionTuple> {

  private final LocalDate FIRST_ENTRIES_DATE = LocalDate.of(2019, 9, 13);

  private final String entityName;

  private final Merge merger;

  public static MergeBatch diffs(String entityName, String idField, String timestampField) {
    return new MergeBatch(entityName, Merge.diffs(idField, timestampField));
  }

  public static MergeBatch sum(String entityName, String idField, String timestampField, String summableField) {
    return new MergeBatch(entityName, Merge.sum(idField, timestampField, summableField));
  }

  private MergeBatch(String entityName, Merge merger) {
    this.entityName = entityName;
    this.merger = merger;
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    var options = (BatchStorageToBigQueryPipelineOptions) input.getPipeline().getOptions();

    var latestValues = loadLatestValues(input, options);

    var rowsWithUpdates = input
            .apply("Union of current and historical data", Flatten.with(latestValues))
            .apply("Merge diffs", merger);

    saveLatestValues(rowsWithUpdates.get(Merge.LATEST), options);

    return rowsWithUpdates;
  }

  private PCollection<Row> loadLatestValues(PCollection<Row> input, BatchStorageToBigQueryPipelineOptions options) {
    var pipeline = input.getPipeline();

    var previousIngestDate = options.getIngestionDate().minusDays(1);
    if (previousIngestDate.isBefore(FIRST_ENTRIES_DATE) || options.getDisableMergeHistoryInput()) {
      return pipeline.apply("Load latest values", Create.empty(input.getCoder()));
    }

    return pipeline
            .apply("Load latest values", BigQueryIO
                    .readTableRowsWithSchema()
                    .from(HistoryUtil.getTableFor(options.getOutputDataset(), "%s_latest".formatted(entityName), previousIngestDate)))
            .apply("Convert to Row", Convert.toRows());
  }

  private void saveLatestValues(PCollection<Row> input, BatchStorageToBigQueryPipelineOptions options) {
    var outputTable = HistoryUtil.getTableFor(options.getOutputDataset(), "%s_latest".formatted(entityName), options.getIngestionDate());
    input.apply("Save latest to BigQuery", BigQueryIO.<Row>write()
            .to(outputTable)
            .ignoreUnknownValues()
            .useBeamSchema()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
            .withoutValidation()
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
    );
  }
}
