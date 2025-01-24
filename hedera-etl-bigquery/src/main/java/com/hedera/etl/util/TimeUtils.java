package com.hedera.etl.util;

import lombok.experimental.UtilityClass;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@UtilityClass
public class TimeUtils {
  public String fromNanos(Long nanos) {
    if (nanos != null) {
      return Instant.ofEpochSecond(0, nanos).truncatedTo(ChronoUnit.MILLIS).toString();
    } else {
      return null;
    }
  }
}
