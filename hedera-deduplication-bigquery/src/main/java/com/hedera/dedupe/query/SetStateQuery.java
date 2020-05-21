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

import com.hedera.dedupe.DedupeType;

/**
 * Saves state to the given table.
 */
public class SetStateQuery extends TemplateQuery {
    // State variable may not be present in stateTable already, so simple UPDATE won't work. This may happen on first
    // run of the job, or when adding new state variables.
    private static final String QUERY = "MERGE INTO `%s` AS state \n" +
            "USING ( \n" +
            "  SELECT '%s' AS name, '%s' AS value \n" +
            ") AS new_state  \n" +
            "ON state.name  = new_state.name \n" +
            "WHEN MATCHED THEN UPDATE SET state.value = new_state.value \n" +
            "WHEN NOT MATCHED BY TARGET THEN INSERT ROW";

    private final String stateTableName;

    public SetStateQuery(String projectId, DedupeType dedupeType, String stateTableName,
                         BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, dedupeType + "_set_state", QUERY, bigQuery, meterRegistry);
        this.stateTableName = stateTableName;
    }

    public void set(String name, String value) throws InterruptedException {
        runWith(stateTableName, name, value);
    }
}
