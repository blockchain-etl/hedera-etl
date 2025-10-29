package com.hedera.etl.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import com.hederahashgraph.api.proto.java.TransactionID;
import lombok.experimental.UtilityClass;

import com.hedera.etl.reader.recordfile.entity.EntityId;

@UtilityClass
public class FormatUtils {
  public static String timestampFromNanos(Long nanos) {
    if (nanos != null) {
      return Instant.ofEpochSecond(0, nanos).truncatedTo(ChronoUnit.MICROS).toString();
    } else {
      return null;
    }
  }

  public static org.joda.time.Instant jodaInstantFromNanos(Long nanos) {
    if (nanos != null) {
      var javaInstant = Instant.ofEpochSecond(0, nanos).truncatedTo(ChronoUnit.MILLIS);
      return org.joda.time.Instant.ofEpochMilli(javaInstant.toEpochMilli());
    } else {
      return null;
    }
  }

  public static org.joda.time.Instant jodaInstantFromLocalDate(LocalDate date) {
    if (date != null) {
      return org.joda.time.Instant.ofEpochSecond(date.atStartOfDay().toEpochSecond(ZoneOffset.UTC));
    } else {
      return null;
    }
  }

  public static LocalDate localDateFromJodaInstant(org.joda.time.Instant instant) {
    if (instant != null) {
      return LocalDate.ofInstant(Instant.ofEpochMilli(instant.getMillis()), ZoneOffset.UTC);
    } else {
      return null;
    }
  }

  public static LocalDate localDateFromTimestamp(String timestamp) {
    if (timestamp != null) {
      return LocalDate.ofInstant(Instant.parse(timestamp), ZoneOffset.UTC);
    } else {
      return null;
    }
  }

  public static Long nanosFromLocalDateTime(LocalDateTime localDateTime) {
    return (localDateTime.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L) + localDateTime.getNano();
  }

  public static Long nanosFromFileName(String filename) {
    var ts = filename.split("\\.")[0];
    return nanosFromLocalDateTime(
        LocalDateTime.parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss.SSSSSSSSS'Z'")));
  }

  public static String removeSuffixes(String string, String... suffixes) {
    return Arrays.stream(suffixes).reduce(string, FormatUtils::removeSuffixIfExists);
  }

  private static String removeSuffixIfExists(String key, String suffix) {
    return key.endsWith(suffix) ? key.substring(0, key.length() - suffix.length()) : key;
  }

  public String transactionId(TransactionID id) {
    return "%s@%d.%09d"
            .formatted(
                EntityId.of(id.getAccountID()).toString(),
                id.getTransactionValidStart().getSeconds(),
                id.getTransactionValidStart().getNanos())
        + (id.getNonce() == 0 ? "" : "/" + id.getNonce());
  }
}
