package com.hedera.etl.util;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class TimeUtilsTest {
  @Test
  public void testFromNanos() {
    var result = TimeUtils.fromNanos(171225180223026003L);

    assertEquals(Instant.parse("2024-04-04T17:30:02.023026Z"), result);
  }
}
