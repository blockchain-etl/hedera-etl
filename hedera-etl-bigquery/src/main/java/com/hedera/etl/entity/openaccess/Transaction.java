package com.hedera.etl.entity.openaccess;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.entity.jsonschema.NFTTransfer;
import com.hedera.etl.entity.jsonschema.StakingRewardTransfer;
import com.hedera.etl.entity.jsonschema.TransactionChainSpecificField;
import com.hedera.etl.entity.jsonschema.Transfer;
import com.hedera.etl.entity.transaction.ErrataType;
import com.hedera.etl.entity.transaction.TransactionTransfersInner;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@Data
@NoArgsConstructor
@AllArgsConstructor(onConstructor_ = @SchemaCreate)
@Builder
public class Transaction {
  String timestamp;
  @Nullable Long blockHeight;
  @Nullable String blockHash;
  @Nullable Integer transactionIndex;
  String transactionHash;
  String from;
  @Nullable String to;
  @Nullable Long sequenceNumber;
  Long value;
  @Nullable String data;
  @Nullable Long networkFeeLimit;
  @Nullable Long maxNetworkFeeUnitPrice;
  String chainSpecific;

  public static List<Transaction> from(RecordFile file) {
    return file.getItems().stream().map(item -> from(item, file)).toList();
  }

  private static Transaction from(RecordItem item, RecordFile file) {
    var transaction = com.hedera.etl.entity.transaction.Transaction.from(item);

    var value =
        transaction.getTransfers().stream()
            .filter(t -> !ErrataType.DELETE.equals(t.getErrata()))
            .mapToLong(TransactionTransfersInner::getAmount)
            .sum();

    var data =
        Set.of(
                    TransactionType.CONTRACTCREATEINSTANCE,
                    TransactionType.CONTRACTUPDATEINSTANCE,
                    TransactionType.CONTRACTCALL,
                    TransactionType.ETHEREUMTRANSACTION)
                .contains(TransactionType.of(transaction.getType()))
            ? DomainUtils.bytesToHex(DomainUtils.toBytes(item.getTransactionBody().getMemoBytes()))
            : null;

    return builder()
        .timestamp(FormatUtils.timestampFromNanos(transaction.getConsensus_timestamp()))
        .blockHeight(file.getIndex())
        .blockHash(file.getHash())
        .transactionIndex(item.getTransactionIndex())
        .transactionHash(DomainUtils.bytesToHex(item.getTransactionHash()))
        .from(item.getPayerAccountId().toString())
        .to(null) // recipient id is in chain specific
        .sequenceNumber(null) // set to null in hedera
        .value(value)
        .data(data)
        .maxNetworkFeeUnitPrice(null) // set to null in hedera
        .chainSpecific(JsonUtils.serialize(chainSpecificFrom(transaction)))
        .build();
  }

  public static Transaction from(RecordItem item) {
    var transaction = com.hedera.etl.entity.transaction.Transaction.from(item);

    var value =
        transaction.getTransfers().stream().mapToLong(TransactionTransfersInner::getAmount).sum();

    var data =
        Set.of(
                    TransactionType.CONTRACTCREATEINSTANCE,
                    TransactionType.CONTRACTUPDATEINSTANCE,
                    TransactionType.CONTRACTCALL,
                    TransactionType.ETHEREUMTRANSACTION)
                .contains(TransactionType.of(transaction.getType()))
            ? DomainUtils.bytesToHex(DomainUtils.toBytes(item.getTransactionBody().getMemoBytes()))
            : null;

    return builder()
        .timestamp(FormatUtils.timestampFromNanos(transaction.getConsensus_timestamp()))
        .blockHeight(null) // mising from erratas
        .blockHash(null) // missing from erratas
        .transactionIndex(item.getTransactionIndex())
        .transactionHash(DomainUtils.bytesToHex(item.getTransactionHash()))
        .from(item.getPayerAccountId().toString())
        .to(null) // recipient id is in chain specific
        .sequenceNumber(null) // set to null in hedera
        .value(value)
        .data(data)
        .maxNetworkFeeUnitPrice(null) // set to null in hedera
        .chainSpecific(JsonUtils.serialize(chainSpecificFrom(transaction)))
        .build();
  }

  private static TransactionChainSpecificField.TransactionStatus entityStatusToJsonStatus(
      Integer status) {
    return Arrays.stream(TransactionChainSpecificField.TransactionStatus.values())
        .filter(s -> Objects.equals(s.name(), ResponseCodeEnum.forNumber(status).name()))
        .findAny()
        .orElse(TransactionChainSpecificField.TransactionStatus.UNKNOWN);
  }

  private static Optional<TransactionChainSpecificField.TransactionType>
      entityTransactionTypeToJsonTransactionType(Integer type) {
    return Arrays.stream(TransactionChainSpecificField.TransactionType.values())
        .filter(s -> Objects.equals(s.name(), TransactionType.of(type)))
        .findAny();
  }

  private static TransactionChainSpecificField chainSpecificFrom(
      com.hedera.etl.entity.transaction.Transaction transaction) {
    var chainSpecific = new TransactionChainSpecificField();
    chainSpecific.setIsScheduled(transaction.getScheduled());
    chainSpecific.setType(
        entityTransactionTypeToJsonTransactionType(transaction.getType()).orElse(null));
    chainSpecific.setStatus(entityStatusToJsonStatus(transaction.getResultCode()));

    chainSpecific.setTransfers(
        Optional.ofNullable(transaction.getTransfers())
            .map(
                transfers ->
                    transfers.stream()
                        .map(
                            t -> {
                              var transfer = new Transfer();
                              transfer.setAccount(t.getAccount());
                              transfer.setAmount((double) t.getAmount());
                              transfer.setIsApproval(t.getIs_approval());
                              return transfer;
                            })
                        .toList())
            .orElse(null));

    chainSpecific.setNftTransfers(
        Optional.ofNullable(transaction.getNft_transfers())
            .map(
                nfts ->
                    nfts.stream()
                        .map(
                            t -> {
                              var transfer = new NFTTransfer();
                              transfer.setReceiverAccountId(t.getReceiver_account_id().toString());
                              transfer.setSerialNumber((double) t.getSerial_number());
                              transfer.setSenderAccountId(t.getSender_account_id().toString());
                              transfer.setIsApproval(t.getIs_approval());
                              transfer.setTokenId(t.getToken_id().toString());
                              return transfer;
                            })
                        .toList())
            .orElse(null));

    chainSpecific.setStakingRewardTransfers(
        Optional.ofNullable(transaction.getStaking_reward_transfers())
            .map(
                rewards ->
                    rewards.stream()
                        .map(
                            t -> {
                              var transfer = new StakingRewardTransfer();
                              transfer.setAccount(t.getAccount_id());
                              transfer.setAmount((double) t.getAmount());
                              return transfer;
                            })
                        .toList())
            .orElse(null));

    return chainSpecific;
  }

  @DefaultSchema(JavaBeanSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class ChainSpecific {
    @Nullable private String recordFileName;
    @Nullable private String hapiVersion;
  }
}
