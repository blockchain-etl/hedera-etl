package com.hedera.etl;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Instant;

/**
 * Converts Hedera transaction json string to TableRow.
 * Uses transaction's consensusTimestamp too add an extra field - consensusTimestampTruncated.
 */
class TransactionJsonToTableRow implements SerializableFunction<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionJsonToTableRow.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    // Distribution is reported as four sub-metrics suffixed with _MAX, _MIN, _MEAN, and _COUNT.
    // Piggy backing on '_MAX' to track latest consensus timestamp across all messages.
    private final Distribution latestConsensusTimestamp = Utility.getDistribution("latestConsensusTimestamp");
    private final Counter jsonToTableRowErrors = Utility.getCounter("jsonToTableRowErrors");

    @Override
    public TableRow apply(String json) {
        try {
            TableRow tableRow = MAPPER.readValue(json, TableRow.class);
            long consensusTimestamp =((Long) tableRow.get("consensusTimestamp"));
            latestConsensusTimestamp.update(consensusTimestamp);
            tableRow.put("consensusTimestampTruncated", Instant.ofEpochSecond(0L,
                            (consensusTimestamp / 1000) * 1000L).toString()); // change granularity from nanos to micros
            LOG.trace("Table Row: {}", tableRow.toPrettyString());
            return tableRow;
        } catch (Exception e) {
            jsonToTableRowErrors.inc();
            LOG.error("Error converting json to TableRow. Json: " + json, e);
            throw new IllegalArgumentException("Error converting json to TableRow", e);
        }
    }
}
