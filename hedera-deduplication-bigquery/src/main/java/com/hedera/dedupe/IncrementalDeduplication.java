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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hedera.dedupe.query.GetLatestDedupeRowTemplateQuery;
import com.hedera.dedupe.query.GetNextTimestampTemplateQuery;
import com.hedera.dedupe.query.UpdateDedupeColumnTemplateQuery;

@Component
public class IncrementalDeduplication extends AbstractDeduplication {

    private final AtomicLong delayGauge = new AtomicLong(0L);
    private final UpdateDedupeColumnTemplateQuery updateDedupeColumnTemplateQuery;
    private final GetLatestDedupeRowTemplateQuery getLatestDedupeRowTemplateQuery;
    private final GetNextTimestampTemplateQuery getNextTimestamp;
    private final int catchupProbeIntervalSec;
    private final int steadyStateProbeIntervalSec;

    public IncrementalDeduplication(DedupeProperties properties, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(DedupeType.INCREMENTAL, properties, bigQuery, meterRegistry);
        updateDedupeColumnTemplateQuery = new UpdateDedupeColumnTemplateQuery(properties.getProjectId(),
                DedupeType.INCREMENTAL, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        getLatestDedupeRowTemplateQuery = new GetLatestDedupeRowTemplateQuery(properties.getProjectId(),
                DedupeType.INCREMENTAL, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        getNextTimestamp = new GetNextTimestampTemplateQuery(properties.getProjectId(),
                DedupeType.INCREMENTAL, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        catchupProbeIntervalSec = properties.getCatchupProbeIntervalSec();
        steadyStateProbeIntervalSec = properties.getSteadyStateProbeIntervalSec();
        Gauge.builder("dedupe.delay", delayGauge, AtomicLong::get)
                .tag("name", DedupeType.INCREMENTAL.toString()) // to be consistent with other metrics
                .description("Delay in deduplication (now - startTimestamp)")
                .baseUnit("s")
                .register(meterRegistry);
    }

    @Scheduled(fixedRateString = "${hedera.dedupe.incrementalFixedRate:300000}") // default: 5 min
    public void run() {
        runDedupe();
    }

    @Override
    TimestampWindow getTimestampWindow(Map<String, FieldValue> state) throws InterruptedException {
        long startTimestamp = 0;
        if (state.containsKey(INCREMENTAL_LATEST_END_TIMESTAMP)) {
            // no +1 since since we filter on non-unique column
            startTimestamp = state.get(INCREMENTAL_LATEST_END_TIMESTAMP).getLongValue();
        }
        delayGauge.set(Duration.between(Instant.ofEpochSecond(startTimestamp), Instant.now()).getSeconds());

        long endTimestamp = probeEndTimestamp(startTimestamp);
        return new TimestampWindow(startTimestamp, endTimestamp);
    }

    @Override
    void saveState(TimestampWindow timestampWindow) throws InterruptedException {
        setState.set(INCREMENTAL_LATEST_END_TIMESTAMP, String.valueOf(timestampWindow.getEndTimestamp()));
    }

    private long probeEndTimestamp(long startTimestamp) throws InterruptedException {
        long baseTimestamp = getNextTimestamp.afterTimestamp(startTimestamp); // can be in streaming buffer
        log.info("Next timestamp greater than startTimestamp ({}) is {}", startTimestamp, baseTimestamp);
        try {
            // Try smaller steady state interval first
            long probeTimestamp = baseTimestamp + steadyStateProbeIntervalSec;
            log.info("Probing for endTimestamp = {} (interval : {})", probeTimestamp, steadyStateProbeIntervalSec);
            updateDedupeColumnTemplateQuery.inTimeWindow(startTimestamp, probeTimestamp);
            log.info("successful");

            probeTimestamp = baseTimestamp + catchupProbeIntervalSec;
            log.info("Probing for endTimestamp = {} (interval : {})", probeTimestamp, catchupProbeIntervalSec);
            updateDedupeColumnTemplateQuery.inTimeWindow(startTimestamp, probeTimestamp);
            log.info("successful");
        } catch (BigQueryException e) {
            log.info("failed");
        }
        return getLatestDedupeRowTemplateQuery.fromTimestamp(startTimestamp);
    }
}
