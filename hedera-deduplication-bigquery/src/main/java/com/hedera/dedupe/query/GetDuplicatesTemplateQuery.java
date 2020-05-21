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
import com.google.cloud.bigquery.TableResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.log4j.Log4j2;

import com.hedera.dedupe.DedupeType;

@Log4j2
public class GetDuplicatesTemplateQuery extends TemplateQuery {
    private static final String QUERY = "SELECT count(*) AS num, consensusTimestamp \n" +
            "  FROM `%s` \n" +
            "  WHERE UNIX_SECONDS(consensusTimestampTruncated) BETWEEN %d AND %d \n" +
            "  GROUP BY consensusTimestamp HAVING num > 1";

    private final Counter duplicatesCounter;
    private final String transactionsTable;

    public GetDuplicatesTemplateQuery(
            String projectId, DedupeType dedupeType, String transactionsTable,
            BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, dedupeType + "_get_duplicates", QUERY, bigQuery, meterRegistry);
        this.transactionsTable = transactionsTable;
        this.duplicatesCounter = Counter.builder("dedupe.duplicates.count")
                .tag("name", dedupeType.toString())
                .description("Count of duplicates found in deduplication")
                .register(meterRegistry);
    }

    public TableResult inTimeWindow(long startTimestamp, long endTimestamp) throws InterruptedException {
        var tableResult = runWith(transactionsTable, startTimestamp, endTimestamp);
        if (tableResult.getTotalRows() == 0) {
            log.info("No duplicates found");
        } else {
            logDuplicates(tableResult);
        }
        return tableResult;
    }

    private void logDuplicates(TableResult tableResult) {
        log.info("Duplicates found");
        log.info("consensusTimestamp, count");
        long numDuplicates = 0;
        for (var row : tableResult.iterateAll()) {
            long num = row.get("num").getLongValue();
            log.info("{}, {}", row.get("consensusTimestamp").getLongValue(), num);
            numDuplicates += num - 1; // -1 for original
        }
        duplicatesCounter.increment(numDuplicates);
    }
}
