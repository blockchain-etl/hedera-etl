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

import com.google.cloud.bigquery.*;
import java.time.Instant;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class BigQueryHelper {
    @Getter
    private final BigQuery bigQuery;
    private final DedupeProperties properties;
    private final DedupeMetrics dedupeMetrics;

    public TableResult runQuery(String query, String jobName) throws InterruptedException {
        // timestamp is just for uniqueness
        JobId jobId = JobId.of(properties.getProjectId(), "dedupe_" + jobName + "_" + Instant.now().getEpochSecond());
        log.info("### Starting job {}", jobId.getJob());
        log.info("Query: {}", query);
        TableResult tableResult = bigQuery.query(QueryJobConfiguration.newBuilder(query).build(), jobId);
        Job job = bigQuery.getJob(jobId);
        dedupeMetrics.recordMetrics(jobName, job.getStatistics());
        return tableResult;
    }

    public void ensureTableExists(String dataset, String tableName) {
        Table table = bigQuery.getTable(dataset, tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table does not exist : " + dataset + "." + tableName);
        }
    }
}
