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

import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubToBigQueryPipelineOptions extends PubsubOptions {

    @Description("The Cloud Pub/Sub subscription to consume from. The name should be in the format of " +
            "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();
    void setInputSubscription(ValueProvider<String> value);

    @Description("BigQuery table to output transactions to. The name should be in the format of " +
            "<project-id>:<dataset-id>.<table-name>.")
    ValueProvider<String> getOutputTransactionsTable();
    void setOutputTransactionsTable(ValueProvider<String> value);

    @Description("BigQuery table to output errors to. The name should be in the format of " +
            "<project-id>:<dataset-id>.<table-name>.")
    ValueProvider<String> getOutputErrorsTable();
    void setOutputErrorsTable(ValueProvider<String> value);
}
