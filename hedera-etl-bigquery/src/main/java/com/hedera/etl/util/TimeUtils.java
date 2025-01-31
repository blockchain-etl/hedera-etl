package com.hedera.etl.util;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TimeUtils {
  public String fromNanos(Long nanos) {
    if (nanos != null) {
      return Instant.ofEpochSecond(0, nanos).truncatedTo(ChronoUnit.MICROS).toString();
    } else {
      return null;
    }
  }
}
