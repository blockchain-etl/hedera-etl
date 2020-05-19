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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.QueryJobConfiguration;
import java.time.Instant;
import java.util.Random;
import javax.annotation.Resource;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Due to lack of any fake/mock/emulator for BigQuery, this test requires GCP BigQuery.
 * Setup:
 * - Create transactions and state table. See documentation for more details.
 * - Fill the properties in resources/application-default.yml
 *
 * Test is not run as part of 'mvn test'. To run the test, use following command:
 * - mvn test -PgcpBigquery
 */
@Log4j2
@SpringBootTest
@Tag("gcpBigquery")
public class DedupeIntegrationTest {
    static final long NUM_ROWS = 100;

    @Resource
    protected DedupeRunner dedupeRunner;
    @Resource
    protected BigQueryHelper bigQueryHelper;
    @Resource
    protected DedupeProperties properties;
    @Resource
    private DedupeState dedupeState;

    private String transactionsTable;

    @BeforeEach
    void beforeEach() throws Exception {
        transactionsTable = properties.getProjectId() + "." + properties.getDatasetName()
                + "." + properties.getTransactionsTableName();
        bigQueryHelper.runQuery("DELETE FROM " +  transactionsTable + " WHERE 1 = 1", "reset_table");
        dedupeState.setState(0);
    }

    @Test
    void testDeduplication() throws Exception {
        // add data
        long expectedEndTimestamp = generateDuplicatedData(1, NUM_ROWS);

        // run dedupe, check num rows and state.
        dedupeRunner.run();
        long actualNumRows = getNumRows(transactionsTable);
        var state = dedupeState.getState();
        assertEquals(NUM_ROWS, actualNumRows);
        assertEquals(expectedEndTimestamp, state.get(DedupeState.STATE_NAME_LAST_VALID_TIMESTAMP).getLongValue());

        // add more data
        expectedEndTimestamp = generateDuplicatedData(expectedEndTimestamp + 1, NUM_ROWS);

        // run dedupe, check num rows and state.
        dedupeRunner.run();
        actualNumRows = getNumRows(transactionsTable);
        state = dedupeState.getState();
        assertEquals(2 * NUM_ROWS, actualNumRows);
        assertEquals(expectedEndTimestamp, state.get(DedupeState.STATE_NAME_LAST_VALID_TIMESTAMP).getLongValue());

        // No new data.
        // Run dedupe, check num rows and state.
        dedupeRunner.run();
        actualNumRows = getNumRows(transactionsTable);
        state = dedupeState.getState();
        assertEquals(2 * NUM_ROWS, actualNumRows);
        assertEquals(expectedEndTimestamp, state.get(DedupeState.STATE_NAME_LAST_VALID_TIMESTAMP).getLongValue());
    }

    // Generates fake transaction rows starting with startTimestamp (in nanos). Every 5th row is duplicated.
    // Returns timestamp of last row.
    private long generateDuplicatedData(long startTimestamp, long numRows) throws Exception {
        String query = "INSERT INTO " +  transactionsTable
                + " (consensusTimestampTruncated, consensusTimestamp) VALUES " +  makeRow(startTimestamp);
        long timestamp = startTimestamp;
        Random rand = new Random();
        for (int i = 1; i < numRows; i++) {
            timestamp += rand.nextInt(1_000_000_000); // add nanos
            String row = makeRow(timestamp);
            query += ", " + row;
            if (i % 5 == 0) {  // duplicate every 5th row
                query += ", " + row;
            }
        }
        log.info("Inserting duplicated data: {}", query);
        bigQueryHelper.runQuery(query, "insert_data");
        return timestamp;
    }

    private String makeRow(long timestamp) {
        return String.format("( '%s', %d )", toBigQueryTimestamp(timestamp), timestamp);
    }

    private long getNumRows(String tableName) throws Exception {
        String query = "SELECT count(*) AS count FROM " + tableName;
        var tableResult = bigQueryHelper.getBigQuery().query(QueryJobConfiguration.newBuilder(query).build());
        for (var row : tableResult.iterateAll()) {
            return row.get("count").getLongValue();
        }
        return 0;
    }

    static String toBigQueryTimestamp(long timestamp) {
        return Instant.ofEpochSecond(0L, (timestamp / 1000) * 1000).toString();
    }
}
