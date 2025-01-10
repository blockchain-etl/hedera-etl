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

import com.google.api.services.bigquery.model.TableReference;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import java.util.Map;

@RequiredArgsConstructor
public class BatchStorageToBigQueryPipeline {

  private final BatchStorageToBigQueryPipelineOptions options;

  void run() {
    var pipeline = Pipeline.create(options);

    var files = pipeline
            .apply("List files", FileIO.match().filepattern(options.getInputPathPattern()))
            .apply("Read files", FileIO.readMatches());

    var entityCollection = EntitiesExtractor.extract(files);

    saveAllToBigQuery(entityCollection);

    pipeline.run();
  }

  void saveAllToBigQuery(Map<String, PCollection<Row>> input) {
    for (var entry : input.entrySet()) {
      var name = entry.getKey();
      var rowPCollection = entry.getValue();

      var outputTable = new TableReference()
              .setDatasetId(options.getOutputDataset())
              .setTableId(name.toLowerCase());

      WriteResult writeResult = rowPCollection
              .apply("Save %s to BigQuery".formatted(name), BigQueryIO.<Row>write()
                      .to(outputTable)
                      .ignoreUnknownValues()
                      .useBeamSchema()
                      // TODO: set to CREATE_NEVER after table management is getAll()done and schema is agreed upon
                      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                      .withoutValidation()
                      .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
              );
    }
  }
}
