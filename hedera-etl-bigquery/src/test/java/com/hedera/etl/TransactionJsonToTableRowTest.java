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

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TransactionJsonToTableRowTest {
    private final TransactionJsonToTableRow converter = new TransactionJsonToTableRow();

    @Test
    void testConversion() throws Exception {
        // Given
        List<String> jsonTransactions = readFileLines("data/TransactionJsonToTableRowTest/transactions.txt");
        List<String> expected = readFileLines("data/TransactionJsonToTableRowTest/expectedTableRows.txt");

        // when
        List<String> actual = jsonTransactions.stream()
                .map(converter::apply)
                .map(TableRow::toString)
                .collect(Collectors.toList());

        // then
        assertLinesMatch(expected, actual);
    }


    @Test
    void testThrowsExceptionForBadJson() throws Exception {
        // given
        String badJson = "{\"consensusTimestamp\":1570802944412586000,\"entity\":{\"shardNum\":0,";

        // then
        assertThrows(IllegalArgumentException.class, () -> {
            converter.apply(badJson);
        });
    }

    private List<String> readFileLines(String fileName) throws IOException  {
        return Files.readAllLines(Path.of(this.getClass().getClassLoader().getResource(fileName).getPath()));
    }
}