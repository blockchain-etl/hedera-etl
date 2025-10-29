package com.hedera.etl.entity.balance;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TokenTransfer {

  private static final String TECHNICAL_ACCOUNT = "0.0.0";

  @Nullable private String account_id;
  @Nullable private String token_id;
  @Nullable private String serial_number;
  @Nullable private BigDecimal amount;
  @Nullable private String transaction_id;
  @Nullable private Long block_timestamp_to;
  @Nullable private Long consensus_timestamp;
  @Nullable private String created;

  public static Iterable<TokenTransfer> from(RecordItem recordItem) {
    var transaction = Transaction.from(recordItem);
    if (transaction.getResultCode() != ResponseCodeEnum.SUCCESS_VALUE) {
      return List.of();
    }

    return fromTransaction(transaction);
  }

  public static List<TokenTransfer> fromTransaction(Transaction transaction) {
    var consensusTimestamp = transaction.getConsensus_timestamp();
    var tokenTransfers = transaction.getToken_transfers();
    var nftTransfers = transaction.getNft_transfers();

    return Stream.concat(
            Optional.ofNullable(tokenTransfers).orElse(List.of()).stream()
                .map(
                    transfer ->
                        builder()
                            .token_id(transfer.getToken_id())
                            .account_id(transfer.getAccount())
                            .amount(
                                Optional.ofNullable(transfer.getAmount())
                                    .map(BigDecimal::valueOf)
                                    .orElse(BigDecimal.ZERO))
                            .consensus_timestamp(consensusTimestamp)
                            .transaction_id(transaction.getTransaction_id())
                            .block_timestamp_to(transaction.getBlock_timestamp_to())
                            .created(FormatUtils.timestampFromNanos(consensusTimestamp))
                            .build()),
            Optional.ofNullable(nftTransfers).orElse(List.of()).stream()
                .flatMap(
                    nftTransfer ->
                        Stream.of(
                            builder()
                                .token_id(nftTransfer.getToken_id())
                                .account_id(nftTransfer.getReceiver_account_id())
                                .serial_number(String.valueOf(nftTransfer.getSerial_number()))
                                .amount(BigDecimal.valueOf(1))
                                .consensus_timestamp(consensusTimestamp)
                                .transaction_id(transaction.getTransaction_id())
                                .block_timestamp_to(transaction.getBlock_timestamp_to())
                                .created(FormatUtils.timestampFromNanos(consensusTimestamp))
                                .build(),
                            builder()
                                .token_id(nftTransfer.getToken_id())
                                .account_id(nftTransfer.getSender_account_id())
                                .serial_number(String.valueOf(nftTransfer.getSerial_number()))
                                .amount(BigDecimal.valueOf(-1L))
                                .consensus_timestamp(consensusTimestamp)
                                .transaction_id(transaction.getTransaction_id())
                                .block_timestamp_to(transaction.getBlock_timestamp_to())
                                .created(FormatUtils.timestampFromNanos(consensusTimestamp))
                                .build())))
        .filter(tokenTransfer -> !Objects.equals(tokenTransfer.account_id, TECHNICAL_ACCOUNT))
        .toList();
  }
}
