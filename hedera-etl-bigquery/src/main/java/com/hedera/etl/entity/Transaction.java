package com.hedera.etl.entity;

import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
@Data
public class Transaction {
    private int charged_tx_fee;
}
