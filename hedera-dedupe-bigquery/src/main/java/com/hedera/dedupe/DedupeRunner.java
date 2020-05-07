package com.hedera.dedupe;

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

import static com.hedera.dedupe.DedupeState.STATE_NAME_LAST_VALID_TIMESTAMP;
import static com.hedera.dedupe.Utility.toBigQueryTimestamp;

import java.time.Duration;
import java.time.Instant;
import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Dedupe runner periodically executes BigQuery job to deduplicate rows.
 * State across runs is maintained using a separate BigQuery table.
 * If a run exceeds the configured 'fixedRate', next run will be queued and started only after previous one finishes.
 *
 * Deduplication algorithm:
 * (consensusTimestamp is unique for each transaction)
 * 1. Get state: startTimestamp = lastValidTimestamp + 1 (from stateTable). 0 if not set.
 * 2. Flag rows not in the streaming buffer: UPDATE table SET dedupe = 1 WHERE ...;
 * 3. Get endTimestamp: SELECT MAX(consensusTimestamp) FROM table WHERE dedupe IS NOT NULL ....
 * 4. Check for duplicates in [startTimestamp , endTimestamp] window
 * 5. Remove duplicates (if present)
 * 6. Save state: Set lastValidTimestamp in state table to endTimestamp
 */
@Log4j2
@Component
public class DedupeRunner {
    private static final String JOB_NAME_FLAG_ROW = "flag_dml_accessible_rows";
    private static final String JOB_NAME_GET_END_TIMESTAMP = "get_end_timestamp";
    private static final String JOB_NAME_GET_DUPLICATES = "get_duplicates";
    private static final String JOB_NAME_REMOVE_DUPLICATES = "remove_duplicates";

    private final BigQueryHelper bigQueryHelper;
    private final String transactionsTable;
    private final DedupeState dedupeState;
    private final DedupeMetrics metrics;

    public DedupeRunner(DedupeProperties properties, BigQueryHelper bigQueryHelper, DedupeState dedupeState,
                        DedupeMetrics dedupeMetrics) {
        this.bigQueryHelper = bigQueryHelper;
        this.dedupeState = dedupeState;
        this.metrics = dedupeMetrics;
        String datasetName = properties.getDatasetName();
        bigQueryHelper.ensureTableExists(datasetName, properties.getTableName());
        transactionsTable = properties.getProjectId() + "." + datasetName + "." + properties.getTableName();
    }

    @Scheduled(fixedRateString = "${hedera.dedupe.fixedRate:300000}") // default: 5 min
    public void run() {
        try {
            Instant dedupeStart = Instant.now();
            metrics.getInvocationsCounter().increment();
            metrics.resetJobMetrics();
            runDedupe();
            metrics.getRuntimeGauge().set(Duration.between(dedupeStart, Instant.now()).getSeconds());
        } catch (Exception e) {
            log.error("Failed deduplication", e);
            metrics.getFailuresCounter().increment();
        }
    }

    private void runDedupe() throws Exception {
        var state = dedupeState.getState();

        // 1. Get state: startTimestamp = lastValidTimestamp + 1 (from stateTable). 0 if not set.
        long startTimestamp = 0; // in nanos
        if (state.containsKey(STATE_NAME_LAST_VALID_TIMESTAMP)) {
            startTimestamp = state.get(STATE_NAME_LAST_VALID_TIMESTAMP).getLongValue() + 1;
        }
        metrics.getStartTimestampGauge().set(startTimestamp);

        // 2. Flag rows not in the streaming buffer: UPDATE table SET dedupe = 1 WHERE ...;
        setDedupeState(startTimestamp);

        // 3. Get endTimestamp: SELECT MAX(consensusTimestamp) FROM table WHERE dedupe IS NOT NULL ....
        long endTimestamp = getEndTimestamp(startTimestamp);
        if (endTimestamp == startTimestamp) {
            return;
        }

        // 4. Check for duplicates in [startTimestamp , endTimestamp] window
        boolean hasDuplicates = hasDuplicates(startTimestamp, endTimestamp);

        // 5. Remove duplicates (if present)
        if (hasDuplicates) {
            removeDuplicates(startTimestamp, endTimestamp);
        }

        // 6. Save state: Set lastValidTimestamp in state table to endTimestamp
        dedupeState.setState(endTimestamp);

        metrics.getDelayGauge().set(
                Duration.between(Instant.ofEpochSecond(0L, startTimestamp), Instant.now()).getSeconds());
    }

    private void setDedupeState(long startTimestamp) throws Exception {
        String query = String.format("UPDATE %s SET dedupe = 1 WHERE %s",
                transactionsTable, consensusTimestampGte(startTimestamp));
        bigQueryHelper.runQuery(query, JOB_NAME_FLAG_ROW);
    }

    private long getEndTimestamp(long startTimestamp) throws Exception {
        String query = String.format("SELECT MAX(consensusTimestamp) AS ts \n" +
                        "FROM %s \n" +
                        "WHERE %s AND dedupe IS NOT NULL",
                transactionsTable, consensusTimestampGte(startTimestamp));
        var tableResult = bigQueryHelper.runQuery(query, JOB_NAME_GET_END_TIMESTAMP);
        long endTimestamp = 0L;
        for (var fvl : tableResult.iterateAll()) {
            if (fvl.get("ts").getValue() == null) {
                log.info("No new rows");
                return startTimestamp;
            }
            endTimestamp = fvl.get("ts").getLongValue();
        }
        metrics.getEndTimestampGauge().set(endTimestamp);
        log.info("endTimestamp = {}", endTimestamp);
        return endTimestamp;
    }

    // SELECT count(*) AS num, consensusTimestamp FROM table
    //   WHERE consensusTimestamp BETWEEN startTimestamp AND endTimestamp
    //   GROUP BY consensusTimestamp HAVING num > 1;
    private boolean hasDuplicates(long startTimestamp, long endTimestamp) throws Exception {
        String query = String.format("SELECT count(*) AS num, consensusTimestamp \n" +
                "  FROM %s \n" +
                "  WHERE %s \n" +
                "  GROUP BY consensusTimestamp HAVING num > 1",
                transactionsTable, consensusTimestampBetween(startTimestamp, endTimestamp, ""));
        var tableResult = bigQueryHelper.runQuery(query, JOB_NAME_GET_DUPLICATES);
        if (tableResult.getTotalRows() == 0) {
            log.info("No duplicates found");
            return false;
        }
        log.info("Duplicates found");
        log.info("consensusTimestamp, count");
        long numDuplicates = 0;
        for (var fvl : tableResult.iterateAll()) {
            long num = fvl.get("num").getLongValue();
            log.info("{}, {}", fvl.get("consensusTimestamp").getLongValue(), num);
            numDuplicates += num - 1; // -1 for original
        }
        metrics.getDuplicatesCounter().increment(numDuplicates);
        return true;
    }

    private void removeDuplicates(long startTimestamp, long endTimestamp) throws Exception {
        String query = String.format("MERGE INTO %s AS dest \n" +
                        "USING ( \n" +
                        "  SELECT k.* \n" +
                        "  FROM ( \n" +
                        "    SELECT ARRAY_AGG(original_data LIMIT 1)[OFFSET(0)] k \n" +
                        "    FROM %s AS original_data \n" +
                        "    WHERE %s \n" +
                        "    GROUP BY consensusTimestamp \n" +
                        "  ) \n" +
                        ") AS src \n" +
                        "ON FALSE \n" +
                        "WHEN NOT MATCHED BY SOURCE AND %s -- remove all data in partition range \n" +
                        "    THEN DELETE \n" +
                        "WHEN NOT MATCHED BY TARGET THEN INSERT ROW",
                transactionsTable, transactionsTable, consensusTimestampBetween(startTimestamp, endTimestamp, ""),
                consensusTimestampBetween(startTimestamp, endTimestamp, "dest"));
        bigQueryHelper.runQuery(query, JOB_NAME_REMOVE_DUPLICATES);
    }

    private String consensusTimestampGte(long timestamp) {
        return String.format("(consensusTimestamp >= %d AND consensusTimestampTruncated >= '%s')", timestamp,
                toBigQueryTimestamp(timestamp));
    }

    private String consensusTimestampBetween(long startTimestamp, long endTimestamp, String alias) {
        alias = alias.isEmpty() ? alias : alias + ".";
        return String.format(
                "(%sconsensusTimestamp BETWEEN %d AND %d) AND (%sconsensusTimestampTruncated BETWEEN '%s' AND '%s')",
                alias, startTimestamp, endTimestamp,
                alias, toBigQueryTimestamp(startTimestamp), toBigQueryTimestamp(endTimestamp));
    }
}
