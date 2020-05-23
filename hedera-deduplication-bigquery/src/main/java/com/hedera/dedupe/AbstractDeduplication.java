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
import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.Value;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.hedera.dedupe.query.GetDuplicatesTemplateQuery;
import com.hedera.dedupe.query.GetStateQuery;
import com.hedera.dedupe.query.RemoveDuplicatesTemplateQuery;
import com.hedera.dedupe.query.SetStateQuery;

/**
 * Common logic between Full and Incremental deduplication.
 * Implementation should call {@link #runDedupe()} to execute a deduplication run.
 * State across runs is maintained using a separate BigQuery table.
 * If a run exceeds the configured 'fixedRate', next run will be queued and started only after previous one finishes.
 *
 * Deduplication algorithm:
 * (consensusTimestamp is unique for each transaction)
 * 1. Get state
 * 2. Call {@link #getTimestampWindow(Map)} to get the time window for which deduplication would be run.
 * 4. Check for duplicates in [startTimestamp , endTimestamp] window
 * 5. Remove duplicates (if present)
 * 6. Save state
 */
public abstract class AbstractDeduplication {
    public static final String INCREMENTAL_LATEST_END_TIMESTAMP = "incrementalLatestEndTimestamp";
    public static final String FULL_LATEST_END_TIMESTAMP = "fullLatestEndTimestamp";

    protected final Logger log = LogManager.getLogger(getClass());
    protected final GetStateQuery getState;
    protected final SetStateQuery setState;
    protected final DedupeMetrics metrics;

    private final GetDuplicatesTemplateQuery getDuplicates;
    private final RemoveDuplicatesTemplateQuery removeDuplicates;

    /**
     * Implementations should override this to return a time window. Rows with consensusTimestamp in that window will
     * be deduplicated.
     */
    abstract TimestampWindow getTimestampWindow(Map<String, FieldValue> state) throws InterruptedException;

    /** Implementations should override this to do all end-of-job state saving. */
    abstract void saveState(TimestampWindow timestampWindow) throws InterruptedException;

    public AbstractDeduplication(
            DedupeType dedupeType, DedupeProperties properties, BigQuery bigQuery, MeterRegistry meterRegistry) {
        String projectId = properties.getProjectId();
        getDuplicates = new GetDuplicatesTemplateQuery(
                projectId, dedupeType, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        removeDuplicates = new RemoveDuplicatesTemplateQuery(
                projectId, dedupeType, properties.getTransactionsTableFullName(), bigQuery, meterRegistry);
        getState = new GetStateQuery(
                projectId, dedupeType, properties.getStateTableFullName(), bigQuery, meterRegistry);
        setState = new SetStateQuery(
                projectId, dedupeType, properties.getStateTableFullName(), bigQuery, meterRegistry);
        this.metrics = new DedupeMetrics(dedupeType, meterRegistry);
    }

    protected void runDedupe() {
        var stopwatch = Stopwatch.createStarted();
        try {
            metrics.getInvocationsCounter().increment();

            var state = getState.get();
            var timestampWindow = getTimestampWindow(state);
            metrics.getStartTimestampGauge().set(timestampWindow.startTimestamp);
            log.info("startTimestamp = {}", timestampWindow.startTimestamp);
            log.info("endTimestamp = {}", timestampWindow.endTimestamp);
            if (timestampWindow.startTimestamp == timestampWindow.endTimestamp) {
                log.info("endTimestamp is same as startTimestamp");
                return;
            }

            // Check for duplicates in [startTimestamp , endTimestamp] window
            var result = getDuplicates.inTimeWindow(
                    timestampWindow.startTimestamp, timestampWindow.endTimestamp);
            // Remove duplicates (if present)
            if (result.getTotalRows() != 0) {
                removeDuplicates.inTimeWindow(
                        timestampWindow.startTimestamp, timestampWindow.endTimestamp);
            }
            saveState(timestampWindow);
            metrics.endTimestampGauge.set(timestampWindow.endTimestamp);
        } catch (Exception e) {
            log.error("Failed deduplication", e);
            metrics.getFailuresCounter().increment();
        } finally {
            log.info("End of deduplication");
            metrics.getRuntimeGauge().set(stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    @Getter
    private static class DedupeMetrics {
        private final AtomicLong startTimestampGauge = new AtomicLong(0L);
        private final AtomicLong endTimestampGauge = new AtomicLong(0L);
        private final AtomicLong runtimeGauge = new AtomicLong(0L);
        private final Counter invocationsCounter;
        private final Counter failuresCounter;

        public DedupeMetrics(DedupeType dedupeType, MeterRegistry meterRegistry) {
            Collection<Tag> tags = List.of(Tag.of("name", dedupeType.toString()));
            this.invocationsCounter = Counter.builder("dedupe.invocations")
                    .tags(tags)
                    .register(meterRegistry);
            this.failuresCounter = Counter.builder("dedupe.failures")
                    .tags(tags)
                    .register(meterRegistry);
            Gauge.builder("dedupe.start.timestamp", startTimestampGauge, AtomicLong::get)
                    .description("Start of dedupe window. Last endTimestamp + 1")
                    .baseUnit("ns")
                    .tags(tags)
                    .register(meterRegistry);
            Gauge.builder("dedupe.end.timestamp", endTimestampGauge, AtomicLong::get)
                    .description("consensusTimestamp of last row in dedupe window.")
                    .baseUnit("ns")
                    .tags(tags)
                    .register(meterRegistry);
            Gauge.builder("dedupe.runtime", runtimeGauge, AtomicLong::get)
                    .description("Total time taken by single dedupe run")
                    .baseUnit("s")
                    .tags(tags)
                    .register(meterRegistry);
        }
    }

    @Value
    protected static class TimestampWindow {
        private final long startTimestamp;
        private final long endTimestamp;
    }
}
