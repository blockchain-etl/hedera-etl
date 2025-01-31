package com.hedera.etl.util;

import java.time.Instant;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimeUtilsTest {
  @Test
  public void testFromNanos() {
    var result = TimeUtils.fromNanos(1712251802023026003L);

    assertEquals(Instant.parse("2024-04-04T17:30:02.023026Z").toString(), result);
  }
}
