package com.hedera.dedupe.query;

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

import com.google.cloud.bigquery.BigQuery;
import io.micrometer.core.instrument.MeterRegistry;

import com.hedera.dedupe.DedupeType;

public class UpdateDedupeColumnTemplateQuery extends TemplateQuery {
    private static final String QUERY = "UPDATE `%s` SET dedupe = 1 " +
            "WHERE UNIX_SECONDS(consensusTimestampTruncated) BETWEEN %d AND %d";

    private final String transactionsTable;

    public UpdateDedupeColumnTemplateQuery(
            String projectId, DedupeType dedupeType, String transactionTable,
            BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, dedupeType + "_update_dedupe_column", QUERY, bigQuery, meterRegistry);
        this.transactionsTable = transactionTable;
    }

    public void inTimeWindow(long startTimestamp, long endTimestamp) throws InterruptedException {
        runWith(transactionsTable, startTimestamp, endTimestamp);
    }
}
