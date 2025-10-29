package com.hedera.etl.entity;

import java.sql.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

public class EntityUtils {
  public static final long DEFAULT_EXPIRY_TIMESTAMP =
      TimeUnit.MILLISECONDS.toNanos(Date.valueOf("2100-1-1").getTime());

  public static Long getEffectiveExpiration(
      @Nullable Long expirationTimestamp,
      @Nullable Long createdTimestamp,
      @Nullable Long autoRenewPeriod) {
    if (expirationTimestamp != null) {
      return expirationTimestamp;
    }

    if (createdTimestamp != null && autoRenewPeriod != null) {
      return createdTimestamp + TimeUnit.SECONDS.toNanos(autoRenewPeriod);
    }

    return DEFAULT_EXPIRY_TIMESTAMP;
  }
}
