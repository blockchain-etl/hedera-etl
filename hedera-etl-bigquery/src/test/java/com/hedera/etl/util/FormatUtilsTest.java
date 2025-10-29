package com.hedera.etl.util;

import java.time.Instant;

import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.Timestamp;
import com.hederahashgraph.api.proto.java.TransactionID;
import org.junit.Test;

import static org.junit.Assert.*;

public class FormatUtilsTest {
  @Test
  public void testTimestampFromNanos() {
    var input = 1712251802023026003L;

    var result = FormatUtils.timestampFromNanos(input);

    assertEquals(Instant.parse("2024-04-04T17:30:02.023026Z").toString(), result);
  }

  @Test
  public void testFormatTransaction() {
    var input =
        TransactionID.newBuilder()
            .setAccountID(
                AccountID.newBuilder().setShardNum(0).setRealmNum(1).setAccountNum(2).build())
            .setTransactionValidStart(Timestamp.newBuilder().setSeconds(345).setNanos(678).build())
            .setNonce(9)
            .build();

    var result = FormatUtils.transactionId(input);

    assertEquals("0.1.2@345.000000678/9", result);
  }
}
