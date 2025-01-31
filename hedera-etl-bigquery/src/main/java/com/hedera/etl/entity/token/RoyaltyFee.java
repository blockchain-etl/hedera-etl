package com.hedera.etl.entity.token;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.CustomFee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.FractionalAmount;
import com.hedera.etl.reader.recordfile.entity.EntityId;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RoyaltyFee {
  public static RoyaltyFee from(CustomFee fee, EntityId tokenId) {

    FallbackFee fallbackFee = null;
    if (fee.getRoyaltyFee().hasFallbackFee()) {
      var _fallbackFee = fee.getRoyaltyFee().getFallbackFee();
      var denominatingTokenId = tokenId;
      if (_fallbackFee.hasDenominatingTokenId()) {
        denominatingTokenId = EntityId.of(_fallbackFee.getDenominatingTokenId());
      }

      fallbackFee =
          FallbackFee.builder()
              .amount(_fallbackFee.getAmount())
              .denominating_token_id(denominatingTokenId.toString())
              .build();
    }

    return RoyaltyFee.builder()
        .all_collectors_are_exempt(fee.getAllCollectorsAreExempt())
        .amount(FractionalAmount.from(fee.getRoyaltyFee().getExchangeValueFraction()))
        .collector_account_id(fee.getFeeCollectorAccountId().toString())
        .fallback_fee(fallbackFee)
        .build();
  }

  @Nullable private Boolean all_collectors_are_exempt;
  @Nullable private FractionalAmount amount;
  @Nullable private String collector_account_id;

  @Nullable private FallbackFee fallback_fee;

  @DefaultSchema(JavaBeanSchema.class)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  static class FallbackFee {
    @Nullable private Long amount;
    @Nullable private String denominating_token_id;
  }
}
