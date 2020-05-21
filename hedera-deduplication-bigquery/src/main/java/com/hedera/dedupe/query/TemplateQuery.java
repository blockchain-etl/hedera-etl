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

import com.google.cloud.bigquery.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TemplateQuery represents a query that can be executed with different set of parameters every time
 */
public abstract class TemplateQuery {
    protected final Logger log = LogManager.getLogger(getClass());

    private final String projectId;
    private final String name; // used for naming jobs, metrics, etc
    private final String templateQuery;
    private final Metrics metrics;
    private final BigQuery bigQuery;

    public TemplateQuery(String projectId, String name, String templateQuery, BigQuery bigQuery, MeterRegistry meterRegistry) {
        this.projectId = projectId;
        this.name = name;
        this.templateQuery = templateQuery;
        this.bigQuery = bigQuery;
        this.metrics = new Metrics(meterRegistry, name);
    }

    // protected since only child class knows the query and hence the order of parameters. Child classes should have
    // a public run(...) method with typed parameters (which would call this). See other implementations for reference.
    protected TableResult runWith(Object... parameters) throws InterruptedException {
        // timestamp is just for uniqueness
        String query = String.format(templateQuery, parameters);
        JobId jobId = JobId.of(projectId, "dedupe_" + name + "_" + Instant.now().getEpochSecond());
        log.info("### Starting job {}", jobId.getJob());
        log.info("Query: {}", query);
        TableResult tableResult = bigQuery.query(QueryJobConfiguration.newBuilder(query).build(), jobId);
        recordMetrics(jobId);
        return tableResult;
    }

    private void recordMetrics(JobId jobId) {
        Job job = bigQuery.getJob(jobId);
        JobStatistics.QueryStatistics queryStatistics = job.getStatistics();
        long runTime = queryStatistics.getEndTime() - queryStatistics.getStartTime();
        Long affectedRows = queryStatistics.getNumDmlAffectedRows();
        log.info("Query Stats: runtime = {}ms, affected rows = {}", runTime, affectedRows);
        metrics.getRuntimeGauge().set(runTime);
        if (affectedRows != null) {
            metrics.getAffectedRowsGauge().set(affectedRows);
        }
    }

    @Value
    static class Metrics {
        private final AtomicLong runtimeGauge;
        private final AtomicLong affectedRowsGauge;

        Metrics(MeterRegistry meterRegistry, String jobName) {
            Collection<Tag> tags = List.of(Tag.of("name", jobName));
            runtimeGauge = meterRegistry.gauge("dedupe.job.runtime", tags, new AtomicLong(0L));
            affectedRowsGauge = meterRegistry.gauge("dedupe.job.rows", tags, new AtomicLong(0L));
        }
    }
}
