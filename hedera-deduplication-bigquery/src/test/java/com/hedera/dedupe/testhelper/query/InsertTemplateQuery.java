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
import org.assertj.core.util.Strings;

public class InsertTemplateQuery extends TemplateQuery {
    private static final String QUERY = "INSERT INTO `%s` (%s) VALUES %s";

    public InsertTemplateQuery(String projectId, BigQuery bigQuery, MeterRegistry meterRegistry) {
        super(projectId, "insert_values", QUERY, bigQuery, meterRegistry);
    }

    public void insert(String tableName, Iterable<String> columns, Iterable<String> values)
            throws InterruptedException {
        runWith(tableName,
                Strings.join(columns).with(", "),
                Strings.join(values).with(", "));
    }
}
