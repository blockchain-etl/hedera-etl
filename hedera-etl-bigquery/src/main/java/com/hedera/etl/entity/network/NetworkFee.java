package com.hedera.etl.entity.network;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NetworkFee {
  @Nullable private Long gas;
  @Nullable private TransactionType transaction_type;
  @Nullable private Long timestamp;
  @Nullable private String created;

  public static NetworkFee from(RecordItem item) {
    var tx = item.getTransactionBody();

    var builder =
        builder()
            .timestamp(item.getConsensusTimestamp())
            .created(FormatUtils.timestampFromNanos(item.getConsensusTimestamp()));

    if (tx.hasContractCall()) {
      builder.gas(tx.getContractCall().getGas()).transaction_type(TransactionType.CONTRACTCALL);
    } else if (tx.hasContractCreateInstance()) {
      builder
          .gas(tx.getContractCreateInstance().getGas())
          .transaction_type(TransactionType.CONTRACTCREATEINSTANCE);
    } else if (tx.hasEthereumTransaction()) {
      builder
          .gas(0L) // seems to be hardcoded to zero in mirror node
          .transaction_type(TransactionType.ETHEREUMTRANSACTION);
    } else {
      return null;
    }

    return builder.build();
  }
}
