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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.hedera.dedupe.query.UpdateDedupeColumnTemplateQuery;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class IncrementalDeduplication extends AbstractDeduplication {

    private final AtomicLong delayGauge = new AtomicLong(0L);
    private final UpdateDedupeColumnTemplateQuery updateDedupeColumnTemplateQuery;

    public IncrementalDeduplication(DedupeProperties properties, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(DedupeType.INCREMENTAL, properties, bigQuery, meterRegistry);
        updateDedupeColumnTemplateQuery = new UpdateDedupeColumnTemplateQuery(properties.getProjectId(),
                properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        Gauge.builder("dedupe.incremental.delay", delayGauge, AtomicLong::get)
                .description("Delay in deduplication (now - startTimestamp)")
                .baseUnit("sec")
                .register(meterRegistry);
    }

    // TODO: lookup
    @Scheduled(fixedRateString = "${hedera.dedupe.fixedRate:300000}") // default: 5 min
    public void run() {
        runDedupe();
    }

    @Override
    TimestampWindow getTimestampWindow(Map<String, FieldValue> state) {
        long startTimestamp = 0;
        if (state.containsKey(INCREMENTAL_LATEST_END_TIMESTAMP)) {
            // no +1 since since we filter on non-unique column
            startTimestamp = state.get(INCREMENTAL_LATEST_END_TIMESTAMP).getLongValue();
        }
        delayGauge.set(Duration.between(Instant.ofEpochSecond(0L, startTimestamp), Instant.now()).getSeconds());

        long endTimestmap = 0; // TODO: iterate using updateDedupeColumnTemplateQuery
        return new TimestampWindow(startTimestamp, endTimestmap);
    }

    @Override
    void saveState(TimestampWindow timestampWindow) throws InterruptedException {
        setStateQuery.run(INCREMENTAL_LATEST_END_TIMESTAMP, String.valueOf(timestampWindow.getEndTimestamp()));
    }
}
