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

import com.hedera.dedupe.query.TemplateQuery;

import io.micrometer.core.instrument.MeterRegistry;

public class GetRowCountTemplateQuery extends TemplateQuery {
    private static final String QUERY = "SELECT count(*) AS count FROM `%s`";

    public GetRowCountTemplateQuery(
            String projectId, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, "get_row_count", QUERY, bigQuery, meterRegistry);
    }

    public Long forTable(String tableName) throws InterruptedException {
        var result = runWith(tableName);
        for (var row : result.iterateAll()) {
            return row.get("count").getLongValue();
        }
        return null; // should never reach here if the query doesn't fail
    }
}
