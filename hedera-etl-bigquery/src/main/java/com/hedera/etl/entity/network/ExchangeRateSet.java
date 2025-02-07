package com.hedera.etl.entity.network;

import javax.annotation.Nullable;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.util.TimeUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Log4j2
public class ExchangeRateSet {
  public static final EntityId FILE_101 = EntityId.of(0, 0, 101);
  public static final EntityId FILE_102 = EntityId.of(0, 0, 102);

  @Nullable private String created;
  @Nullable private Long timestamp;
  @Nullable private ExchangeRate current_rate;
  @Nullable private ExchangeRate next_rate;

  @DefaultSchema(JavaBeanSchema.class)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  private static class ExchangeRate {
    private Integer cent_equivalent;
    private Integer hbar_equivalent;
    private Long expiration_time;

    public static ExchangeRate from(com.hederahashgraph.api.proto.java.ExchangeRate exchangeRate) {
      return builder()
          .cent_equivalent(exchangeRate.getCentEquiv())
          .hbar_equivalent(exchangeRate.getHbarEquiv())
          .expiration_time(exchangeRate.getExpirationTime().getSeconds())
          .build();
    }
  }

  public static ExchangeRateSet from(RecordItem recordItem) {
    if (recordItem.getTransactionBody().hasFileAppend()) {
      return null;
    }

    var txBody = recordItem.getTransactionBody().getFileAppend();
    var fileId = EntityId.of(txBody.getFileID());

    if (!fileId.equals(FILE_101) && !fileId.equals(FILE_102)) {
      return null;
    }

    var contents = txBody.getContents();

    try {
      var exchangeRateSet =
          com.hederahashgraph.api.proto.java.ExchangeRateSet.parseFrom(contents.toByteArray());

      return builder()
          .current_rate(ExchangeRate.from(exchangeRateSet.getCurrentRate()))
          .next_rate(ExchangeRate.from(exchangeRateSet.getNextRate()))
          .timestamp(recordItem.getConsensusTimestamp())
          .created(TimeUtils.fromNanos(recordItem.getConsensusTimestamp()))
          .build();
    } catch (InvalidProtocolBufferException e) {
      log.warn("Can't parse exchange file %s".formatted(fileId), e);
      return null;
    }
  }
}
