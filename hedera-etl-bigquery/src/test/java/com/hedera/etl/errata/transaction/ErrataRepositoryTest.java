package com.hedera.etl.errata.transaction;

import java.io.IOException;
import java.sql.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

public class ErrataRepositoryTest {
  @Test
  public void testCanInitRepository() throws IOException {
    assertFalse(ErrataRepository.ERRATA_CACHE.isEmpty());
  }

  @Test
  public void testCanReadRangeRepository() throws IOException {
    var result =
        ErrataRepository.readErratasForRange(
            TimeUnit.MILLISECONDS.toNanos(Date.valueOf("2019-09-13").getTime()),
            TimeUnit.MILLISECONDS.toNanos(Date.valueOf("2019-09-30").getTime()));

    assertEquals(31, result.size());
  }
}
