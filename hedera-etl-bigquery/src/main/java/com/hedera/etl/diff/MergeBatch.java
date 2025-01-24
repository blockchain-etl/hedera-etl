package com.hedera.etl.diff;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import java.time.LocalDate;

@RequiredArgsConstructor
public class MergeBatch extends PTransform<PCollection<Row>, PCollection<Row>> {

  private final LocalDate FIRST_ENTRIES_DATE = LocalDate.of(2019, 9, 13);

  private final LocalDate ingestDate;
  private final String entityName;

  private final String tempDataset;

  private final String timestampField;
  private final String idField;

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    var latestValues = loadLatestValues(input);

    var rowsWithUpdates = input
            .apply("Union of current and historical data", Flatten.with(latestValues))
            .apply("Merge diffs", Merge.diffs(idField, timestampField));

    saveLatestValues(rowsWithUpdates.get(Merge.LATEST));

    return rowsWithUpdates.get(Merge.UPDATED);
  }

  private PCollection<Row> loadLatestValues(PCollection<Row> input) {
    var pipeline = input.getPipeline();

    var previousIngestDate = ingestDate.minusDays(1);
    if (previousIngestDate.isBefore(FIRST_ENTRIES_DATE)) {
      return pipeline.apply("Load latest values", Create.empty(input.getCoder()));
    }

    return pipeline
            .apply("Load latest values", BigQueryIO
                    .readTableRowsWithSchema()
                    .from(HistoryUtil.getTableFor(tempDataset, entityName, previousIngestDate)))
            .apply("Convert to Row", Convert.toRows());
  }

  private void saveLatestValues(PCollection<Row> input) {
    var outputTable = HistoryUtil.getTableFor(tempDataset, entityName, ingestDate);
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
