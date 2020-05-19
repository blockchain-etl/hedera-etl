package com.hedera.dedupe;

import com.google.cloud.bigquery.JobStatistics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

@Log4j2
@Getter
@Component
class DedupeMetrics {
    private final MeterRegistry meterRegistry;
    private final Map<String, JobMetrics> jobMetrics;
    private final AtomicLong startTimestampGauge;
    private final AtomicLong endTimestampGauge;
    private final AtomicLong delayGauge;
    private final AtomicLong runtimeGauge;
    private final Counter duplicatesCounter;
    private final Counter invocationsCounter;
    private final Counter failuresCounter;

    public DedupeMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.jobMetrics = new HashMap<>();
        this.startTimestampGauge = new AtomicLong(0L);
        Gauge.builder("dedupe.start.timestamp", startTimestampGauge, AtomicLong::get)
                .description("Start of dedupe window. Last endTimestamp + 1")
                .baseUnit("ns")
                .register(meterRegistry);
        this.endTimestampGauge = new AtomicLong(0L);
        Gauge.builder("dedupe.end.timestamp", endTimestampGauge, AtomicLong::get)
                .description("consensusTimestamp of last row in dedupe window.")
                .baseUnit("ns")
                .register(meterRegistry);
        this.runtimeGauge = new AtomicLong(0L);
        Gauge.builder("dedupe.runtime", runtimeGauge, AtomicLong::get)
                .description("Total time taken by single dedupe run")
                .baseUnit("sec")
                .register(meterRegistry);
        this.delayGauge = new AtomicLong(0L);
        Gauge.builder("dedupe.delay", delayGauge, AtomicLong::get)
                .description("Delay in deduplication (now - startTimestamp)")
                .baseUnit("sec")
                .register(meterRegistry);
        this.duplicatesCounter = Counter.builder("dedupe.duplicates.count")
                .description("Count of duplicates found")
                .register(meterRegistry);
        this.invocationsCounter = Counter.builder("dedupe.invocations").register(meterRegistry);
        this.failuresCounter = Counter.builder("dedupe.failures").register(meterRegistry);
    }

    public void recordMetrics(String jobName, JobStatistics.QueryStatistics queryStatistics) {
        long runTime = queryStatistics.getEndTime() - queryStatistics.getStartTime();
        Long affectedRows = queryStatistics.getNumDmlAffectedRows();
        log.info("Job stats: runtime = {}ms, affected rows = {}", runTime, affectedRows);
        jobMetrics.computeIfAbsent(jobName, k -> new JobMetrics(meterRegistry, k));
        var metrics = jobMetrics.get(jobName);
        metrics.getRuntimeGauge().set(runTime);
        if (affectedRows != null) {
            metrics.getAffectedRowsGauge().set(affectedRows);
        }
    }

    // Individuals jobs (queries) may not run, so reset their metrics' to 0.
    // All other metrics are always set on each invocation, so no need to reset them.
    public void resetJobMetrics() {
        jobMetrics.forEach((key, value) -> value.reset());
    }

    @Value
    static class JobMetrics {
        private final AtomicLong runtimeGauge;
        private final AtomicLong affectedRowsGauge;

        JobMetrics(MeterRegistry meterRegistry, String jobName) {
            Collection<Tag> tags = List.of(Tag.of("name", jobName));
            runtimeGauge = meterRegistry.gauge("dedupe.job.runtime", tags, new AtomicLong(0L));
            affectedRowsGauge = meterRegistry.gauge("dedupe.job.rows", tags, new AtomicLong(0L));
        }

        void reset() {
            runtimeGauge.set(0L);
            affectedRowsGauge.set(0L);
        }
    }
}
