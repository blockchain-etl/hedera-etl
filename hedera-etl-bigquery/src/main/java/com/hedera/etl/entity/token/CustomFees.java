package com.hedera.etl.entity.token;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.reader.recordfile.entity.EntityId;

@Log4j2
@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CustomFees {
  @Nullable private String created_timestamp;
  @Nullable private List<FixedFee> fixed_fees;
  @Nullable private List<FractionalFee> fractional_fees;
  @Nullable private List<RoyaltyFee> royalty_fees;

  public static CustomFees from(
      List<com.hederahashgraph.api.proto.java.CustomFee> customFees, EntityId tokenId) {
    var customFee =
        CustomFees.builder()
            .fixed_fees(new ArrayList<>())
            .fractional_fees(new ArrayList<>())
            .royalty_fees(new ArrayList<>())
            .build();

    for (var fee : customFees) {
      switch (fee.getFeeCase()) {
        case FIXED_FEE -> customFee.fixed_fees.add(FixedFee.from(fee, tokenId));
        case FRACTIONAL_FEE -> customFee.fractional_fees.add(FractionalFee.from(fee, tokenId));
        case ROYALTY_FEE -> customFee.royalty_fees.add(RoyaltyFee.from(fee, tokenId));
        default -> log.warn("Invalid CustomFee %s".formatted(fee.getFeeCase()));
      }
    }

    return customFee;
  }
}
