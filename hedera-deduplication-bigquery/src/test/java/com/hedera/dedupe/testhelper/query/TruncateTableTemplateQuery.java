package com.hedera.dedupe.testhelper.query;

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

import com.hedera.dedupe.query.TemplateQuery;

public class TruncateTableTemplateQuery extends TemplateQuery {
    private static final String QUERY = "DELETE FROM `%s` WHERE 1 = 1";

    public TruncateTableTemplateQuery(
            String projectId, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, "truncate_table", QUERY, bigQuery, meterRegistry);
    }

    public void truncate(String tableName) throws InterruptedException {
        runWith(tableName);
    }
}
