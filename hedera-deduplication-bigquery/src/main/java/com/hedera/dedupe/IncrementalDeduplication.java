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

import static com.hedera.dedupe.query.SetStateQuery.STATE_NAME_LAST_VALID_TIMESTAMP;

import com.google.cloud.bigquery.BigQuery;
import com.hedera.dedupe.query.GetDuplicatesTemplateQuery;
import com.hedera.dedupe.query.GetStateQuery;
import com.hedera.dedupe.query.RemoveDuplicatesTemplateQuery;
import com.hedera.dedupe.query.SetStateQuery;
import io.micrometer.core.instrument.MeterRegistry;
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
public class IncrementalDeduplication {
    private final DedupeMetrics metrics;

    private final GetDuplicatesTemplateQuery getDuplicatesTemplateQuery;
    private final RemoveDuplicatesTemplateQuery removeDuplicatesTemplateQuery;
    private final GetStateQuery getStateQuery;
    private final SetStateQuery setStateQuery;

    public IncrementalDeduplication(DedupeProperties properties, BigQuery bigQuery,
                                    DedupeMetrics dedupeMetrics, MeterRegistry meterRegistry) {
        this.metrics = dedupeMetrics;
        Utility.ensureTableExists(bigQuery, properties.getTransactionsTableId());
        Utility.ensureTableExists(bigQuery, properties.getStateTableId());
        String projectId = properties.getProjectId();
        getDuplicatesTemplateQuery = new GetDuplicatesTemplateQuery(
                projectId, DedupeType.INCREMENTAL, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        removeDuplicatesTemplateQuery = new RemoveDuplicatesTemplateQuery(
                projectId, DedupeType.INCREMENTAL, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        getStateQuery = new GetStateQuery(projectId, properties.getStateTableFullName(), bigQuery, meterRegistry);
        setStateQuery = new SetStateQuery(projectId, properties.getStateTableFullName(), bigQuery, meterRegistry);
    }

    // TODO: lookup
    @Scheduled(fixedRateString = "${hedera.dedupe.fixedRate:300000}") // default: 5 min
    public void run() {
        try {
            Instant dedupeStart = Instant.now();
            metrics.getInvocationsCounter().increment();
            runDedupe();
            metrics.getRuntimeGauge().set(Duration.between(dedupeStart, Instant.now()).getSeconds());
        } catch (Exception e) {
            log.error("Failed deduplication", e);
            metrics.getFailuresCounter().increment();
        }
    }

    private void runDedupe() throws Exception {
        var state = getStateQuery.run();

        // 1. Get state: startTimestamp = lastValidTimestamp + 1 (from stateTable). 0 if not set.
        long startTimestamp = 0; // in nanos
        if (state.containsKey(STATE_NAME_LAST_VALID_TIMESTAMP)) {
            startTimestamp = state.get(STATE_NAME_LAST_VALID_TIMESTAMP).getLongValue() + 1;
        }
        metrics.getStartTimestampGauge().set(startTimestamp);

        // 2. Flag rows not in the streaming buffer: UPDATE table SET dedupe = 1 WHERE ...;
//        setDedupeState(startTimestamp);

        // 3. Get endTimestamp: SELECT MAX(consensusTimestamp) FROM table WHERE dedupe IS NOT NULL ....
//        long endTimestamp = getEndTimestamp(startTimestamp);
//        if (endTimestamp == startTimestamp) {
//            return;
//        }
        long endTimestamp = 0; // TODO

        // Check for duplicates in [startTimestamp , endTimestamp] window
        var result = getDuplicatesTemplateQuery.runWith(startTimestamp, endTimestamp);

        // Remove duplicates (if present)
        if (result.getTotalRows() != 0) {
            removeDuplicatesTemplateQuery.runWith(startTimestamp, endTimestamp);
        }

        // 6. Save state: Set lastValidTimestamp in state table to endTimestamp
        setStateQuery.run(endTimestamp);

        metrics.getDelayGauge().set(
                Duration.between(Instant.ofEpochSecond(0L, startTimestamp), Instant.now()).getSeconds());
    }
}
