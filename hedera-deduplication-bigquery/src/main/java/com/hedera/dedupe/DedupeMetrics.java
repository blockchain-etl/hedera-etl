package com.hedera.dedupe;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

@Log4j2
@Getter
@Component
class DedupeMetrics {
    private final MeterRegistry meterRegistry;
    private final AtomicLong startTimestampGauge;
    private final AtomicLong endTimestampGauge;
    private final AtomicLong delayGauge;
    private final AtomicLong runtimeGauge;
    private final Counter invocationsCounter;
    private final Counter failuresCounter;

    public DedupeMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
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
        this.invocationsCounter = Counter.builder("dedupe.invocations").register(meterRegistry);
        this.failuresCounter = Counter.builder("dedupe.failures").register(meterRegistry);
    }
}
