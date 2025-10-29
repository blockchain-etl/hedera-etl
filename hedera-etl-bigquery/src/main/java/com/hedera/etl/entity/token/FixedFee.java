package com.hedera.etl.entity.token;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.CustomFee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.entity.EntityId;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FixedFee {
  public static FixedFee from(CustomFee fee, EntityId tokenId) {
    var denominatingTokenId = EntityId.of(fee.getFixedFee().getDenominatingTokenId());
    if (denominatingTokenId == EntityId.EMPTY) {
      denominatingTokenId = tokenId;
    }

    return FixedFee.builder()
        .all_collectors_are_exempt(fee.getAllCollectorsAreExempt())
        .collector_account_id(fee.getFeeCollectorAccountId().toString())
        .amount(fee.getFixedFee().getAmount())
        .denominating_token_id(denominatingTokenId.toString())
        .build();
  }

  @Nullable private Boolean all_collectors_are_exempt;
  @Nullable private Long amount;
  @Nullable private String collector_account_id;

  @Nullable private String denominating_token_id;
}
