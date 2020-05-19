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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes TableRow (serialized to json) and InsertErrors (serialized to json) in BigQueryInsertError to errors
 * table in BigQuery.
 *
 * Failed retry policy is set to always retry since this is last step in failure handling and no failure should be
 * silently ignored.
 * Throwing exception in case of any failure would prompt the runner to retry the whole bundle, and can lead to
 * duplicate inserts. So it is best to let BigQueryIO retry only the failed inserts.
 */
@RequiredArgsConstructor
class BigQueryErrorsSink extends PTransform<PCollection<BigQueryInsertError>, PDone> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ValueProvider<String> errorsTable;
    private final String errorsTableJsonSchema;

    @Override
    public PDone expand(PCollection<BigQueryInsertError> input) {
       input.apply("ConvertErrorToTableRow", ParDo.of(new BigQueryInsertErrorToTableRow()))
                .apply("WriteErrorsToTable", BigQueryIO.writeTableRows()
                        .to(errorsTable)
                        .withJsonSchema(errorsTableJsonSchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry()));
       return PDone.in(input.getPipeline());
    }

    static class BigQueryInsertErrorToTableRow extends DoFn<BigQueryInsertError, TableRow> {

        private static final Logger LOG = LoggerFactory.getLogger(BigQueryErrorsSink.class);
        private final Counter bigQueryInsertErrors = Utility.getCounter("bigQueryInsertErrors");
        private final Counter bigQueryInsertErrorToTableRowErrors =
                Utility.getCounter("bigQueryInsertErrorToTableRowErrors");

        @ProcessElement
        public void processElement(ProcessContext context) {
            bigQueryInsertErrors.inc();
            BigQueryInsertError error = context.element();
            try {
                TableRow tableRow = new TableRow()
                        .set("tableRow", MAPPER.writeValueAsString(error.getRow()))
                        .set("errors", MAPPER.writeValueAsString(error.getError()));
                LOG.error(tableRow.toPrettyString());
                context.output(tableRow);
            } catch (Exception e) {
                bigQueryInsertErrorToTableRowErrors.inc();
                LOG.error("Error converting BigQueryInsertError to TableRow", e);
                throw new RuntimeException(e);
            }
        }
    }
}
