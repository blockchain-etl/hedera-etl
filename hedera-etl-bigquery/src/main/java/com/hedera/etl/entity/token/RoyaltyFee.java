package com.hedera.etl.entity.token;

import com.hedera.etl.recordfile.entity.EntityId;

import com.hederahashgraph.api.proto.java.CustomFee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import javax.annotation.Nullable;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RoyaltyFee {
  @Nullable
  private Boolean all_collectors_are_exempt;
  @Nullable
  private FractionalAmount amount;
  @Nullable
  private String collector_account_id;
  @Nullable
  private FallbackFee fallback_fee;

  public static RoyaltyFee from(CustomFee fee, EntityId tokenId) {

    FallbackFee fallbackFee = null;
    if (fee.getRoyaltyFee().hasFallbackFee()) {
      var _fallbackFee = fee.getRoyaltyFee().getFallbackFee();
      var denominatingTokenId = tokenId;
      if (_fallbackFee.hasDenominatingTokenId()) {
        denominatingTokenId = EntityId.of(_fallbackFee.getDenominatingTokenId());
      }

      fallbackFee = FallbackFee.builder()
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

  @DefaultSchema(JavaBeanSchema.class)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  static class FallbackFee {
    @Nullable
    private Long amount;
    @Nullable
    private String denominating_token_id;
  }
}
