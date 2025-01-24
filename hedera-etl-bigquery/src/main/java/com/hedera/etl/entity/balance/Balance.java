package com.hedera.etl.entity.balance;

import com.hedera.etl.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.recordfile.entity.EntityId;
import com.hedera.etl.util.TimeUtils;

import com.hederahashgraph.api.proto.java.AccountAmount;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Balance {
  @Nullable
  String created;
  @Nullable
  Long consensus_timestamp;
  @Nullable
  String account_id;
  @Nullable
  Long amount;

  public static Iterable<Balance> from(RecordItem recordItem) {

    var body = recordItem.getTransactionBody();
    if (!body.hasCryptoTransfer()) {
      return List.of();
    }

    var payerAccount = recordItem.getPayerAccountId();
    var transfers = body.getCryptoTransfer().getTransfers().getAccountAmountsList();

    return transfers.stream()
            .filter(AccountAmount::getIsApproval)
            .filter(transfer -> !Objects.equals(EntityId.of(transfer.getAccountID()).toString(), payerAccount.toString()))
            .flatMap(transfer -> List.of(
                            builder()
                                    .account_id(EntityId.of(transfer.getAccountID()).toString())
                                    .amount(transfer.getAmount())
                                    .consensus_timestamp(recordItem.getConsensusTimestamp())
                                    .created(TimeUtils.fromNanos(recordItem.getConsensusTimestamp()))
                                    .build(),

                            builder()
                                    .account_id(payerAccount.toString())
                                    .amount(-transfer.getAmount())
                                    .consensus_timestamp(recordItem.getConsensusTimestamp())
                                    .created(TimeUtils.fromNanos(recordItem.getConsensusTimestamp()))
                                    .build()
                    ).stream()
            )
            .toList();
  }
}
