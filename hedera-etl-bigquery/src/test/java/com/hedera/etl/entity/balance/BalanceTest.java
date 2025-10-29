package com.hedera.etl.entity.balance;

import static org.junit.Assert.*;

public class BalanceTest {
  //
  //  @Test
  //  public void testFlattenBalancesFromSingleTransaction() {
  //    var balances =
  //        Stream.of(
  //            Balance.builder()
  //                .account_id("0.0.1")
  //                .amount(BigDecimal.valueOf(1))
  //                .consensus_timestamp(1L)
  //                .created(FormatUtils.timestampFromNanos(1L))
  //                .build(),
  //            Balance.builder()
  //                .account_id("0.0.1")
  //                .amount(BigDecimal.valueOf(0))
  //                .consensus_timestamp(1L)
  //                .created(FormatUtils.timestampFromNanos(1L))
  //                .build(),
  //            Balance.builder()
  //                .account_id("0.0.2")
  //                .amount(BigDecimal.valueOf(4))
  //                .consensus_timestamp(1L)
  //                .created(FormatUtils.timestampFromNanos(1L))
  //                .build());
  //
  //    var result = new HashSet<>(Balance.flattenBalances(balances));
  //
  //    assertEquals(
  //        Set.of(
  //            Balance.builder()
  //                .account_id("0.0.1")
  //                .amount(BigDecimal.valueOf(1))
  //                .consensus_timestamp(1L)
  //                .created(FormatUtils.timestampFromNanos(1L))
  //                .tokens(
  //                    List.of(
  //                        Balance.Token.builder()
  //                            .token_id("0.0.100")
  //                            .amount(BigDecimal.valueOf(1))
  //                            .build()))
  //                .build(),
  //            Balance.builder()
  //                .account_id("0.0.2")
  //                .amount(BigDecimal.valueOf(4))
  //                .consensus_timestamp(1L)
  //                .created(FormatUtils.timestampFromNanos(1L))
  //                .tokens(List.of())
  //                .build()),
  //        result);
  //  }
}
