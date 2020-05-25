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

import static com.hedera.dedupe.AbstractDeduplication.INCREMENTAL_LATEST_END_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.*;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Resource;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import com.hedera.dedupe.query.GetStateQuery;
import com.hedera.dedupe.testhelper.TransactionsGenerator;
import com.hedera.dedupe.testhelper.query.GetRowCountTemplateQuery;
import com.hedera.dedupe.testhelper.query.TruncateTableTemplateQuery;

/**
 * Due to lack of an emulator for BigQuery, this test requires GCP BigQuery.
 * See docs on how to run deduplication tests.
 */
@Log4j2
@SpringBootTest(properties = {
        "hedera.dedupe.datasetName=${random.string}",
        "hedera.dedupe.catchupProbeIntervalSec=1000",
        "hedera.dedupe.steadyStateProbeIntervalSec=10",
        "hedera.dedupe.scheduling.enabled=false" // Dedupe runs are manually invoked in tests
})
@Tag("gcpBigquery")
public class IncrementalIntegrationTest {
    private static final int CATCH_PROBE_INTERVAL = 1000;
    private static final int STEADY_STATE_PROBE_INTERVAL = 10;

    @Resource
    protected BigQuery bigQuery;
    @Resource
    MeterRegistry meterRegistry;
    @Resource
    protected DedupeProperties properties;
    @Resource
    IncrementalDeduplication deduplication;
    @Resource
    ApplicationContext context;

    private TransactionsGenerator transactionsGenerator;
    private GetRowCountTemplateQuery getRowCount;
    private GetStateQuery getState;
    private String transactionsTable;

    @BeforeEach
    void beforeEach() throws Exception {
        // create dataset and table
        var env = context.getEnvironment();
        bigQuery.create(DatasetInfo.of(properties.getDatasetName()));
        createTable(bigQuery, properties.getTransactionsTableId(),
                env.getProperty("hedera.dedupe.transactionsSchemaLocation"));
        createTable(bigQuery, properties.getStateTableId(), env.getProperty("hedera.dedupe.stateSchemaLocation"));

        transactionsTable = properties.getTransactionsTableFullName();
        String projectId = properties.getProjectId();
        TruncateTableTemplateQuery truncateTableQuery =
                new TruncateTableTemplateQuery(projectId, bigQuery, meterRegistry);
        truncateTableQuery.truncate(transactionsTable);
        truncateTableQuery.truncate(properties.getStateTableFullName());
        transactionsGenerator = new TransactionsGenerator(
                projectId, transactionsTable, properties.getTransactionsTableId(), bigQuery, meterRegistry);
        getRowCount = new GetRowCountTemplateQuery(projectId, bigQuery, meterRegistry);
        getState = new GetStateQuery(
                projectId, DedupeType.INCREMENTAL, properties.getStateTableFullName(), bigQuery, meterRegistry);
    }

    @AfterEach
    void afterEach() {
        bigQuery.delete(DatasetId.of(properties.getDatasetName()), BigQuery.DatasetDeleteOption.deleteContents());
    }

    @Test
    void testDeduplication() throws Exception {
        final int numRows = 100;

        // add data
        long endTimestamp = transactionsGenerator.insert(0, numRows);
        long expectedState = endTimestamp;

        // run dedupe with no streaming data
        deduplication.run();
        long actualNumRows = getRowCount.forTable(transactionsTable);
        assertEquals(numRows, actualNumRows);
        assertLatestEndTimestamp(expectedState);

        // add more data + streaming data
        endTimestamp = transactionsGenerator.insert(endTimestamp + 1, numRows);
        transactionsGenerator.insertStreaming(endTimestamp + 1, numRows);

        // run dedupe with streaming data
        deduplication.run();
        // +1 for getNextTimestamp after startTimestamp
        expectedState = expectedState + 1 + STEADY_STATE_PROBE_INTERVAL;
        assertLatestEndTimestamp(expectedState);

        // No new data.
        deduplication.run();
        expectedState = expectedState + 1 + STEADY_STATE_PROBE_INTERVAL;
        assertLatestEndTimestamp(expectedState);
    }

    // tests for the case when there is large gap in consensusTimestamps
    @Test
    void testGap() throws Exception {
        final int numRows = 100;

        // Add chunk 1 data
        long endTimestamp1 = transactionsGenerator.insert(0, numRows);
        // Add chunk 2 data with huge gap in timestamps, bigger even than catchup interval
        long endTimestamp2 = transactionsGenerator.insert(endTimestamp1 + 10 * CATCH_PROBE_INTERVAL, numRows);

        deduplication.run(); // will deduplicate all of chunk 1
        assertLatestEndTimestamp(endTimestamp1);

        deduplication.run();
        assertLatestEndTimestamp(endTimestamp2);
    }

    private void assertLatestEndTimestamp(long timestamp) throws InterruptedException {
        var state = getState.get();
        assertEquals(timestamp, state.get(INCREMENTAL_LATEST_END_TIMESTAMP).getLongValue());
    }

    private static void createTable(BigQuery bigQuery, TableId tableId, String jsonSchemaPath) throws Exception {
        String jsonSchema = Files.readString(Path.of(jsonSchemaPath));
        TableSchema tableSchema = JacksonFactory.getDefaultInstance()
                .fromString("{ \"fields\": " + jsonSchema + " }", TableSchema.class);
        // There's no other way to convert json to Schema.
        Method fromPb = Schema.class.getDeclaredMethod("fromPb", TableSchema.class);
        fromPb.setAccessible(true);
        Schema schema = (Schema) fromPb.invoke(null, tableSchema);
        TableInfo tableInfo = TableInfo.of(tableId, StandardTableDefinition.of(schema));
        log.info("Creating table {}", tableId);
        bigQuery.create(tableInfo);
    }
}
