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

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

@RequiredArgsConstructor
public class PubSubToBigQueryPipeline {

    private final PubSubToBigQueryPipelineOptions options;
    private final String jsonTableSchema;

    void run() {
        Pipeline pipeline = Pipeline.create(options);
        WriteResult writeResult = pipeline
                .apply("PubSubListener", PubsubIO.readStrings()
                        .fromSubscription(options.getInputSubscription())
                        .withIdAttribute("consensusTimestamp"))
                .apply("WriteToBigQuery", BigQueryIO.<String>write()
                        .to(options.getOutputTransactionsTable())
                        .withFormatFunction(new TransactionJsonToTableRow())
                        .withJsonSchema(jsonTableSchema)
                        .ignoreUnknownValues()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withoutValidation()
                        .withExtendedErrorInfo()
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));
        writeResult.getFailedInsertsWithErr()
                .apply(new BigQueryErrorsSink(
                        options.getOutputErrorsTable(), Utility.getResource("errors-schema.json")));
        pipeline.run();
    }
}
