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
import com.google.cloud.bigquery.FieldValue;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.Map;

import com.hedera.dedupe.DedupeType;

/**
 * Reads state from the given table. Returns Map where key is state field's name and value is state field's value.
 */
public class GetStateQuery extends TemplateQuery {
    private static final String QUERY = "SELECT name, value FROM `%s`";

    public GetStateQuery(String projectId, DedupeType dedupeType, String stateTableName,
                         BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, dedupeType + "_get_state", String.format(QUERY, stateTableName), bigQuery, meterRegistry);
    }

    public Map<String, FieldValue> get() throws InterruptedException {
        var result = runWith();
        Map<String, FieldValue> state = new HashMap<>();
        // okay to iterateAll since state will contain couple rows at max.
        result.iterateAll().forEach(fvl -> {
            state.put(fvl.get("name").getStringValue(), fvl.get("value"));
        });
        return state;
    }
}
