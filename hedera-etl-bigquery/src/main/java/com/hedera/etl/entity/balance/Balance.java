package com.hedera.etl.entity.balance;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.transaction.ErrataType;
import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Balance {

  @Nullable String account_id;
  @Nullable BigDecimal amount;
  @Nullable String transaction_id;
  @Nullable Long block_timestamp_to;
  @Nullable Long consensus_timestamp;
  @Nullable String created;

  public static Iterable<Balance> from(RecordItem recordItem) {
    return Iterables.concat(fromCreateAccount(recordItem), fromTransfer(recordItem));
  }

  private static List<Balance> fromCreateAccount(RecordItem recordItem) {
    var body = recordItem.getTransactionBody();
    if (!recordItem.isSuccessful() || !body.hasCryptoCreateAccount()) {
      return List.of();
    }

    return List.of(
        builder()
            .account_id(
                EntityId.of(recordItem.getTransactionRecord().getReceipt().getAccountID())
                    .toString())
            .transaction_id(
                FormatUtils.transactionId(recordItem.getTransactionBody().getTransactionID()))
            .block_timestamp_to(recordItem.getFileConsensusEnd())
            .amount(BigDecimal.valueOf(body.getCryptoCreateAccount().getInitialBalance()))
            .consensus_timestamp(recordItem.getConsensusTimestamp())
            .created(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
            .build());
  }

  private static List<Balance> fromTransfer(RecordItem recordItem) {
    var transaction = Transaction.from(recordItem);

    var transfers = transaction.getTransfers();
    if (transfers == null) {
      transfers = List.of();
    }

    final String createdAccountId =
        !recordItem.getTransactionBody().hasCryptoCreateAccount()
            ? null
            : EntityId.of(recordItem.getTransactionRecord().getReceipt().getAccountID()).toString();
    final BigDecimal initialAmount =
        !recordItem.getTransactionBody().hasCryptoCreateAccount()
            ? null
            : BigDecimal.valueOf(
                recordItem.getTransactionBody().getCryptoCreateAccount().getInitialBalance());

    return transfers.stream()
        .filter(transfer -> !ErrataType.DELETE.equals(transfer.getErrata()))
        .map(
            transfer ->
                builder()
                    .account_id(transfer.getAccount())
                    .amount(
                        Optional.ofNullable(transfer.getAmount())
                            .map(BigDecimal::valueOf)
                            .orElse(BigDecimal.ZERO))
                    .consensus_timestamp(recordItem.getConsensusTimestamp())
                    .transaction_id(
                        FormatUtils.transactionId(
                            recordItem.getTransactionBody().getTransactionID()))
                    .block_timestamp_to(recordItem.getFileConsensusEnd())
                    .created(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
                    .build())
        .filter(
            transfer ->
                !Objects.equals(transfer.account_id, createdAccountId)
                    && !Objects.equals(transfer.amount, initialAmount))
        .toList();
  }
}
