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
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

/**
 * Reads/writes deduplication job's state from/to BigQuery table.
 */
@Log4j2
@Component
public class DedupeState {
    public static final String STATE_NAME_LAST_VALID_TIMESTAMP = "lastValidTimestamp";
    public static final String JOB_NAME_SAVE_STATE = "save_state";

    private final BigQueryHelper bigQueryHelper;
    private final TableId stateTableId;
    private final String stateTable;

    public DedupeState(DedupeProperties properties, BigQueryHelper bigQueryHelper) {
        this.bigQueryHelper = bigQueryHelper;
        String datasetName = properties.getDatasetName();
        String tableName = properties.getStateTableName();
        stateTableId = TableId.of(datasetName, tableName);
        stateTable = properties.getProjectId() + "." + datasetName + "." + tableName;
        bigQueryHelper.ensureTableExists(datasetName, tableName);
    }

    /**
     * @return state read from the given table. Map's key is state field's name and map's value is state field's value.
     */
    public Map<String, FieldValue> getState() {
        TableResult tableResult = bigQueryHelper.getBigQuery().listTableData(stateTableId,
                Schema.of(Field.of("name", LegacySQLTypeName.STRING), Field.of("value", StandardSQLTypeName.STRING)));
        if (tableResult.getTotalRows() == 0) { // state not initialized
            return Map.of();
        }
        Map<String, FieldValue> state = new HashMap<>();
        // okay to iterateAll since state will contain couple rows at max.
        tableResult.iterateAll().forEach(fvl -> {
            state.put(fvl.get("name").getStringValue(), fvl.get("value"));
        });
        return state;
    }

    public void setState(long endTimestamp) throws Exception {
        // State variable may not be present in stateTable already (for UPDATE). For example, adding new state
        // variables. Also, deprecating state variable would require deleting it.
        // Although the query *looks* complicated, this is just atomically resetting state table to the given values.
        String query = String.format("MERGE INTO %s \n" +
                        "USING ( \n" +
                        "  SELECT '%s' AS name, '%d' AS value \n" +
                        ") \n" +
                        "ON FALSE \n" +
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE \n" +
                        "WHEN NOT MATCHED BY TARGET THEN INSERT ROW \n",
                stateTable, STATE_NAME_LAST_VALID_TIMESTAMP, endTimestamp);
        bigQueryHelper.runQuery(query, JOB_NAME_SAVE_STATE);
    }
}
