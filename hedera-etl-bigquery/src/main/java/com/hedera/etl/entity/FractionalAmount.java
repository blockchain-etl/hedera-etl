package com.hedera.etl.entity;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.Fraction;
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
public class FractionalAmount {
  public static FractionalAmount from(Fraction fraction) {
    return builder()
        .numerator(fraction.getNumerator())
        .denominator(fraction.getDenominator())
        .build();
  }

  @Nullable private Long denominator;

  @Nullable private Long numerator;
}
