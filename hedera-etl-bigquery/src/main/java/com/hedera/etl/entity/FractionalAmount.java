package com.hedera.etl.entity;

import com.hederahashgraph.api.proto.java.Fraction;
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
public class FractionalAmount {
  @Nullable
  private Long numerator;
  @Nullable
  private Long denominator;

  public static FractionalAmount from(Fraction fraction) {
    return builder()
            .numerator(fraction.getNumerator())
            .denominator(fraction.getDenominator())
            .build();
  }
}
