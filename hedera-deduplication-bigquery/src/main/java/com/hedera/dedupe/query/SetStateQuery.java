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

import com.google.cloud.bigquery.BigQuery;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Saves state to the given table.
 */
public class SetStateQuery extends TemplateQuery {
    public static final String STATE_NAME_LAST_VALID_TIMESTAMP = "lastValidTimestamp";
    // State variable may not be present in stateTable already (so UPDATE won't always work). For example, adding new
    // state variables. Also, deprecating state variable would require deleting it.
    // Although the query *looks* complicated, this is just atomically resetting state table to the given values.
    private static final String QUERY = "MERGE INTO %s \n" +
            "USING ( \n" +
            "  SELECT '%s' AS name, '%d' AS value \n" +
            ") \n" +
            "ON FALSE \n" +
            "WHEN NOT MATCHED BY SOURCE THEN DELETE \n" +
            "WHEN NOT MATCHED BY TARGET THEN INSERT ROW";

    private final String stateTableName;

    public SetStateQuery(String projectId, String stateTableName, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, "set_state", QUERY, bigQuery, meterRegistry);
        this.stateTableName = stateTableName;
    }

    public void run(long lastValidTimestamp) throws InterruptedException {
        runWith(stateTableName, STATE_NAME_LAST_VALID_TIMESTAMP, lastValidTimestamp);
    }
}
