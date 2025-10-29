package com.hedera.etl.errata.transaction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import lombok.SneakyThrows;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.reader.ValidatedDataInputStream;

import static com.hedera.etl.reader.recordfile.reader.record.RecordFileReader.MAX_TRANSACTION_LENGTH;

public class ErrataRepository {

  public static final Map<Long, RecordItem> ERRATA_CACHE =
      listErratas().stream()
          .map(ErrataRepository::readErrata)
          .collect(Collectors.toMap(RecordItem::getConsensusTimestamp, Function.identity()));

  public static List<RecordItem> readErratasForRange(Long start, Long end) {
    return ERRATA_CACHE.keySet().stream()
        .filter(k -> k >= start && k <= end)
        .map(ERRATA_CACHE::get)
        .toList();
  }

  @SneakyThrows
  private static List<String> listErratas() {
    try (var resource =
            ErrataRepository.class.getResourceAsStream(
                "/errata/missingtransaction/missing-transaction-index.txt");
        InputStreamReader inputStreamReader =
            new InputStreamReader(resource, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
      return bufferedReader.lines().collect(Collectors.toList());
    }
  }

  @SneakyThrows
  private static RecordItem readErrata(String path) {
    var pathElements = path.split("/");
    var name = pathElements[pathElements.length - 1];

    try (var resource = ErrataRepository.class.getResourceAsStream(path)) {

      try (var in = new ValidatedDataInputStream(resource, name)) {
        byte[] recordBytes = in.readLengthAndBytes(1, MAX_TRANSACTION_LENGTH, false, "record");
        byte[] transactionBytes =
            in.readLengthAndBytes(1, MAX_TRANSACTION_LENGTH, false, "transaction");
        var transactionRecord = TransactionRecord.parseFrom(recordBytes);
        var transaction = Transaction.parseFrom(transactionBytes);
        return RecordItem.builder()
            .transactionRecord(transactionRecord)
            .transaction(transaction)
            .build();
      }
    }
  }
}
