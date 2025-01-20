package com.hedera.etl.entity.token;

import javax.annotation.Nullable;

import com.hedera.etl.recordfile.entity.EntityId;

import com.hederahashgraph.api.proto.java.CustomFee;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FractionalFee {
  @Nullable
  private Boolean all_collectors_are_exempt;
  @Nullable
  private FractionalAmount amount;
  @Nullable
  private String collector_account_id;
  @Nullable
  private String denominating_token_id;
  @Nullable
  private Long minimum;
  @Nullable
  private Long maximum;
  @Nullable
  private Boolean net_of_transfers;

  public static FractionalFee from(CustomFee fee, EntityId tokenId) {
    var denominatingTokenId = tokenId;

    return FractionalFee.builder()
            .all_collectors_are_exempt(fee.getAllCollectorsAreExempt())
            .collector_account_id(fee.getFeeCollectorAccountId().toString())
            .amount(FractionalAmount.from(fee.getFractionalFee().getFractionalAmount()))
            .denominating_token_id(denominatingTokenId.toString())
            .minimum(fee.getFractionalFee().getMinimumAmount())
            .maximum(fee.getFractionalFee().getMaximumAmount())
            .net_of_transfers(fee.getFractionalFee().getNetOfTransfers())
            .build();
  }
}
