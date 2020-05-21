package com.hedera.dedupe.testhelper;

/*-
 * ‌
 * Hedera ETL
 * ​k
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
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import com.hedera.dedupe.testhelper.query.InsertTemplateQuery;

// Generates fake transaction
public class TransactionsGenerator {

    private final BigQuery bigQuery;
    private final String transactionsTable;
    private final InsertTemplateQuery insertTemplateQuery;
    private final TableId transactionsTableId;

    public TransactionsGenerator(
            String projectId, String transactionsTable, TableId transactionsTableId,
            BigQuery bigQuery, MeterRegistry meterRegistry) {
        this.bigQuery = bigQuery;
        this.insertTemplateQuery = new InsertTemplateQuery(projectId, bigQuery, meterRegistry);
        this.transactionsTable = transactionsTable;
        this.transactionsTableId = transactionsTableId;
    }

    // consensusTimestamp of generated transactions would be greater than 'startTimestampAfter' (in nanos).
    // Every 5th row is duplicated. Returns timestamp of the last row.
    public long insert(long startTimestampAfter, int numRows) throws InterruptedException {
        List<String> rows = new ArrayList<>();
        long endTimestamp = generateTxns(startTimestampAfter, numRows, t -> rows.add(makeRow(t)));
        insertTemplateQuery.insert(
                transactionsTable, List.of("consensusTimestampTruncated", "consensusTimestamp"), rows);
        return endTimestamp;
    }

    public long insertStreaming(long startTimestampAfter, int numRows) {
        InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(transactionsTableId);
        long endTimestamp = generateTxns(startTimestampAfter, numRows, t -> requestBuilder.addRow(makeInsertRow(t)));
        bigQuery.insertAll(requestBuilder.build());
        return endTimestamp;
    }

    private long generateTxns(long startTimestampAfter, int numRows, Consumer<Long> addTxn) {
        Random rand = new Random();
        long timestampNs = startTimestampAfter * 1000_000_000; // convert to nanos
        for (int i = 0; i < numRows; i++) {
            timestampNs += rand.nextInt(1_000_000_000); // add nanos
            addTxn.accept(timestampNs);
            if (i % 5 == 0) {  // duplicate every 5th row
                addTxn.accept(timestampNs);
            }
        }
        return Instant.ofEpochSecond(0L, timestampNs).getEpochSecond();
    }

    private String makeRow(long timestamp) {
        return String.format("( '%s', %d )",
                Instant.ofEpochSecond(0L, (timestamp / 1000) * 1000).toString(), timestamp);
    }

    private Map<String, Object> makeInsertRow(long timestampNs) {
        return Map.of("consensusTimestampTruncated", Instant.ofEpochSecond(0L, (timestampNs / 1000) * 1000).toString(),
                "consensusTimestamp", timestampNs);
    }
}
