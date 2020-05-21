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

public class RemoveDuplicatesTemplateQuery extends TemplateQuery {
    private static final String QUERY = "MERGE INTO `%s` AS dest \n" +
            "USING ( \n" +
            "  SELECT k.* \n" +
            "  FROM ( \n" +
            "    SELECT ARRAY_AGG(original_data LIMIT 1)[OFFSET(0)] k \n" +
            "    FROM `%s` AS original_data \n" +
            "    WHERE UNIX_SECONDS(consensusTimestampTruncated) BETWEEN %d AND %d \n" +
            "    GROUP BY consensusTimestamp \n" +
            "  ) \n" +
            ") AS src \n" +
            "ON FALSE \n" +
            "WHEN NOT MATCHED BY SOURCE" +
            "    AND UNIX_SECONDS(dest.consensusTimestampTruncated) BETWEEN %d AND %d \n" +
            "    THEN DELETE -- remove all data in partition range\n" +
            "WHEN NOT MATCHED BY TARGET THEN INSERT ROW";

    private final String transactionsTable;

    public RemoveDuplicatesTemplateQuery(
            String projectId, DedupeType dedupeType, String transactionsTable,
            BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, dedupeType + "_remove_duplicates", QUERY, bigQuery, meterRegistry);
        this.transactionsTable = transactionsTable;
    }

    public void inTimeWindow(long startTimestamp, long endTimestamp) throws InterruptedException {
        runWith(transactionsTable, transactionsTable, startTimestamp, endTimestamp, startTimestamp, endTimestamp);
    }
}
