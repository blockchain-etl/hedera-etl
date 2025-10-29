package com.hedera.etl.errata.historical;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.CryptoGetInfoResponse;
import lombok.SneakyThrows;

import com.hedera.etl.entity.account.Account;
import com.hedera.etl.entity.smartcontracts.Contract;
import com.hedera.etl.reader.recordfile.entity.EntityId;

public class HistoricalAccountAndContractRepository {

  public static List<Account> getAccounts() {
    // accounts are created for each contract
    return readAccountInfos().stream().map(Account::fromAccountInfo).toList();
  }

  public static List<Contract> getContracts() {
    var contractIds = listContractIds();

    return readAccountInfos().stream()
        .filter(
            accountOrContract -> {
              var id = EntityId.of(accountOrContract.getAccountID()).getId();
              return contractIds.contains(id);
            })
        .map(Contract::fromAccountInfo)
        .toList();
  }

  @SneakyThrows
  private static Set<Long> listContractIds() {
    try (var resource =
            HistoricalAccountAndContractRepository.class.getResourceAsStream(
                "/accountInfoContracts.txt");
        InputStreamReader inputStreamReader =
            new InputStreamReader(resource, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
      return bufferedReader.lines().map(Long::parseLong).collect(Collectors.toSet());
    }
  }

  @SneakyThrows
  private static List<CryptoGetInfoResponse.AccountInfo> readAccountInfos() {
    try (var resource =
            HistoricalAccountAndContractRepository.class.getResourceAsStream(
                "/accountInfo.txt.gz");
        var inputStreamReader =
            new InputStreamReader(new GZIPInputStream(resource), StandardCharsets.UTF_8);
        var bufferedReader = new BufferedReader(inputStreamReader)) {
      return bufferedReader
          .lines()
          .map(line -> Base64.getDecoder().decode(line))
          .map(
              line -> {
                try {
                  return CryptoGetInfoResponse.AccountInfo.parseFrom(line);
                } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(e);
                }
              })
          .toList();
    }
  }
}
