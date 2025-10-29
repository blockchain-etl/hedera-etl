package com.hedera.etl.entity.smartcontracts;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.ContractDeleteTransactionBody;
import com.hederahashgraph.api.proto.java.ContractNonceInfo;
import com.hederahashgraph.api.proto.java.ContractUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoGetInfoResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import com.hedera.etl.BytecodeUtils;
import com.hedera.etl.entity.EntityUtils;
import com.hedera.etl.entity.Key;
import com.hedera.etl.entity.Lookup;
import com.hedera.etl.entity.TimestampRange;
import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.ParentRecordItemProjection;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Contract {
  @Nullable private Key admin_key;
  @Nullable private String auto_renew_account;
  @Nullable private Long auto_renew_period;
  @Nullable private String contract_id;
  @Nullable private String created;
  @Nullable private Long created_timestamp;
  @Nullable private String modified;
  @Nullable private Long modified_timestamp;
  @Nullable private String evm_address;
  @Nullable private Long expiration_timestamp;
  @Nullable private String file_id;
  @Nullable private Integer max_automatic_token_associations;
  @Nullable private String memo;
  @Nullable private Long nonce;
  @Nullable private String obtainer_id;
  @Nullable private Boolean permanent_removal;
  @Nullable private String proxy_account_id;
  @Nullable private TimestampRange timestamp;
  @Nullable private Boolean deleted;
  @Nullable private String bytecode;
  @Nullable private String runtime_bytecode;

  public static Contract from(RecordItem recordItem) {
    if (!recordItem.isSuccessful()) {
      return null;
    }

    if (recordItem.getTransactionRecord().hasContractCreateResult()
        || recordItem.getTransactionBody().hasContractCreateInstance()) {
      return fromCreate(recordItem, recordItem.getTransactionBody().getContractCreateInstance());
    }

    if (recordItem.getTransactionBody().hasContractUpdateInstance()) {
      return fromUpdate(recordItem, recordItem.getTransactionBody().getContractUpdateInstance());
    }

    if (recordItem.getTransactionBody().hasContractDeleteInstance()) {
      return fromDelete(recordItem, recordItem.getTransactionBody().getContractDeleteInstance());
    }

    return null;
  }

  private static Contract fromUpdate(
      RecordItem recordItem, ContractUpdateTransactionBody transactionBody) {
    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();

    var builder = builder();

    if (transactionBody.hasAutoRenewAccountId()) {
      // TODO: get hold of lookup
      // var autoRenewAccount = entityIdService
      // .lookup(transactionBody.getAutoRenewAccountId())
      // .orElse(EntityId.EMPTY);
      // if (!EntityId.isEmpty(autoRenewAccount)) {
      // builder.auto_renew_account(autoRenewAccount);
      // }
      // TODO: replace with lookup once done
      builder.auto_renew_account(EntityId.of(transactionBody.getAutoRenewAccountId()).toString());
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (contractCreateResult.hasEvmAddress()) {
      builder.evm_address(
          DomainUtils.bytesToHexWithPrefix(
              contractCreateResult.getEvmAddress().getValue().toByteArray()));
    }

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasProxyAccountID()) {
      var proxyAccountId = EntityId.of(transactionBody.getProxyAccountID());
      builder.proxy_account_id(proxyAccountId.toString());
    }

    if (transactionBody.hasExpirationTime()) {
      builder.expiration_timestamp(
          DomainUtils.timestampInNanosMax(transactionBody.getExpirationTime()));
    }

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    var contractId = recordItem.getTransactionRecord().getReceipt().getContractID();
    builder
        .contract_id(EntityId.of(contractId).toString())
        .memo(transactionBody.getMemo())
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp);

    if (transactionBody.hasMaxAutomaticTokenAssociations()) {
      builder.max_automatic_token_associations(
          transactionBody.getMaxAutomaticTokenAssociations().getValue());
    }

    switch (transactionBody.getMemoFieldCase()) {
      case MEMOWRAPPER:
        builder.memo(transactionBody.getMemoWrapper().getValue());
        break;
      case MEMO:
        if (!transactionBody.getMemo().isEmpty()) {
          builder.memo(transactionBody.getMemo());
        }
        break;
      default:
        break;
    }

    var sidecarRecords = recordItem.getSidecarRecords();

    for (var sidecar : sidecarRecords) {
      if (sidecar.hasBytecode() && !sidecar.getMigration()) {
        var bytecode = sidecar.getBytecode();
        if (contractId.equals(bytecode.getContractId())) {
          if (builder.bytecode == null) {
            builder.bytecode(
                DomainUtils.bytesToHexWithPrefix(bytecode.getInitcode().toByteArray()));
          }

          builder.runtime_bytecode(
              DomainUtils.bytesToHexWithPrefix(bytecode.getRuntimeBytecode().toByteArray()));
          break;
        }
      }
    }

    // for child transactions FileID is located in parent
    // ContractCreate/EthereumTransaction types
    // and initcode is located in the sidecar
    updateChildFromParent(builder, recordItem);

    return builder.build();
  }

  private static Contract fromDelete(
      RecordItem recordItem, ContractDeleteTransactionBody transactionBody) {
    var consensusTimestamp = recordItem.getConsensusTimestamp();

    String obtainerId = null;

    if (transactionBody.hasTransferAccountID()) {
      obtainerId = EntityId.of(transactionBody.getTransferAccountID()).toString();
    } else if (transactionBody.hasTransferContractID()) {
      obtainerId =
          Lookup.lookup(transactionBody.getTransferContractID()).orElse(EntityId.EMPTY).toString();
    }

    return builder()
        .contract_id(EntityId.of(transactionBody.getContractID()).toString())
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .permanent_removal(transactionBody.getPermanentRemoval())
        .obtainer_id(obtainerId)
        .deleted(true)
        .build();
  }

  private static Contract fromCreate(
      RecordItem recordItem, ContractCreateTransactionBody transactionBody) {
    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    var contractId = recordItem.getTransactionRecord().getReceipt().getContractID();

    var builder =
        builder()
            .contract_id(EntityId.of(contractId).toString())
            .evm_address(DomainUtils.bytesToHexWithPrefix(DomainUtils.toEvmAddress(contractId)))
            .max_automatic_token_associations(transactionBody.getMaxAutomaticTokenAssociations())
            .created(FormatUtils.timestampFromNanos(consensusTimestamp))
            .created_timestamp(consensusTimestamp)
            .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
            .modified_timestamp(consensusTimestamp)
            .memo(transactionBody.getMemo())
            .nonce(
                recordItem
                    .getTransactionRecord()
                    .getContractCreateResult()
                    .getContractNoncesList()
                    .stream()
                    .filter(info -> Objects.equals(info.getContractId(), contractId))
                    .map(ContractNonceInfo::getNonce)
                    .findFirst()
                    .orElse(null))
            .timestamp(TimestampRange.builder().from(consensusTimestamp).to(null).build())
            .deleted(false);

    if (transactionBody.hasAutoRenewAccountId()) {
      // TODO: get hold of lookup
      // var autoRenewAccount = entityIdService
      // .lookup(transactionBody.getAutoRenewAccountId())
      // .orElse(EntityId.EMPTY);
      // if (!EntityId.isEmpty(autoRenewAccount)) {
      // builder.auto_renew_account(autoRenewAccount);
      // }
      // TODO: replace with lookup once done
      builder.auto_renew_account(EntityId.of(transactionBody.getAutoRenewAccountId()).toString());
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (contractCreateResult.hasEvmAddress()) {
      builder.evm_address(
          DomainUtils.bytesToHexWithPrefix(
              DomainUtils.toBytes(contractCreateResult.getEvmAddress().getValue())));
    }

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasProxyAccountID()) {
      var proxyAccountId = EntityId.of(transactionBody.getProxyAccountID());
      builder.proxy_account_id(proxyAccountId.toString());
    }

    var sidecarRecords = recordItem.getSidecarRecords();

    for (var sidecar : sidecarRecords) {
      if (sidecar.hasBytecode() && !sidecar.getMigration()) {
        var bytecode = sidecar.getBytecode();
        if (contractId.equals(bytecode.getContractId())) {
          if (builder.bytecode == null) {
            builder.bytecode(
                DomainUtils.bytesToHexWithPrefix(bytecode.getInitcode().toByteArray()));
          }
          builder.runtime_bytecode(
              DomainUtils.bytesToHexWithPrefix(bytecode.getRuntimeBytecode().toByteArray()));
          break;
        }
      }
    }

    switch (transactionBody.getInitcodeSourceCase()) {
      case FILEID:
        var fileId = EntityId.of(transactionBody.getFileID());
        builder.file_id(fileId.toString());
        break;
      case INITCODE:
        builder.bytecode(
            DomainUtils.bytesToHexWithPrefix(transactionBody.getInitcode().toByteArray()));
        break;
      default:
        break;
    }

    // for child transactions FileID is located in parent
    // ContractCreate/EthereumTransaction types
    // and initcode is located in the sidecar
    updateChildFromParent(builder, recordItem);

    return builder
        .expiration_timestamp(
            EntityUtils.getEffectiveExpiration(
                null, builder.created_timestamp, builder.auto_renew_period))
        .build();
  }

  public static Contract fromAccountInfo(CryptoGetInfoResponse.AccountInfo accountInfo) {

    var accountId = EntityId.of(accountInfo.getAccountID());
    var builder =
        builder()
            .created(
                FormatUtils.timestampFromNanos(
                    1568411400000000000L)) // set everything to 2019-09-13 21:50:00
            .created_timestamp(1568411400000000000L)
            .modified(FormatUtils.timestampFromNanos(1568411400000000000L))
            .modified_timestamp(1568411400000000000L)
            .contract_id(accountId.toString())
            .deleted(accountInfo.getDeleted())
            .memo(accountInfo.getMemo())
            .evm_address(DomainUtils.bytesToHexWithPrefix(DomainUtils.toEvmAddress(accountId)));

    if (accountInfo.hasAutoRenewPeriod()) {
      builder.auto_renew_period(accountInfo.getAutoRenewPeriod().getSeconds());
    }

    if (accountInfo.hasExpirationTime()) {
      builder.expiration_timestamp(
          DomainUtils.timestampInNanosMax(accountInfo.getExpirationTime()));
    }

    if (accountInfo.hasKey()) {
      builder.admin_key(com.hedera.etl.entity.Key.from(accountInfo.getKey()));
    }

    if (accountInfo.hasProxyAccountID()) {
      var stakedAccount = EntityId.of(accountInfo.getProxyAccountID());
      builder.proxy_account_id(stakedAccount.toString());
    }

    return builder.build();
  }

  private static void updateChildFromParent(ContractBuilder builder, RecordItem recordItem) {
    if (!recordItem.isChild() || recordItem.getParent() == null) {
      return;
    }

    // Parents may be either ContractCreate or EthereumTransaction
    var parentRecordItem = recordItem.getParent();
    var type = TransactionType.of(parentRecordItem.getTransactionType());

    switch (type) {
      case CONTRACTCREATEINSTANCE -> updateChildFromContractCreateParent(builder, parentRecordItem);
      case ETHEREUMTRANSACTION ->
          updateChildFromEthereumTransactionParent(builder, parentRecordItem);
      default -> {
        // no-op
      }
    }
  }

  private static void updateChildFromContractCreateParent(
      ContractBuilder contract, ParentRecordItemProjection recordItem) {
    switch (recordItem.getSourceCase()) {
      case FILEID:
        if (contract.file_id == null) {
          var fileId = recordItem.getFileID();
          contract.file_id(fileId.toString());
        }
        break;
      case INITCODE:
        if (contract.file_id == null) {
          contract.bytecode(
              DomainUtils.bytesToHexWithPrefix(recordItem.getBodyCallData().toByteArray()));
        }
        break;
      default:
        break;
    }
  }

  private static void updateChildFromEthereumTransactionParent(
      ContractBuilder contract, ParentRecordItemProjection recordItem) {
    // use callData FileID if present
    if (recordItem.getBodyCallData() != null && contract.file_id == null) {
      var fileId = EntityId.of(recordItem.getBodyCallData());
      contract.file_id(fileId.toString());
      return;
    }

    if (contract.bytecode == null && recordItem.getEthereumCallData() != null) {
      contract.bytecode(DomainUtils.bytesToHexWithPrefix(recordItem.getEthereumCallData()));
    }
  }

  @RequiredArgsConstructor
  @Log4j2
  public static class JoinWithFileByteCode extends PTransform<PCollectionTuple, PCollection<Row>> {

    public static final TupleTag<Row> INPUT_TAG = new TupleTag<>("input");
    public static final TupleTag<Row> FILES_TAG = new TupleTag<>("files");

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
      var bytecodeFiles =
          input
              .get(FILES_TAG)
              .apply(
                  "Filter non-bytecode files",
                  Filter.by(
                      row ->
                          Optional.ofNullable(row.getBytes("content"))
                              .map(String::new)
                              .map(BytecodeUtils::isInitBytecode)
                              .orElse(false)))
              .apply(
                  "Add window",
                  Window.<Row>into(FixedWindows.of(Duration.standardMinutes(1)))
                      .discardingFiredPanes()
                      .withAllowedLateness(
                          Duration.standardHours(1), Window.ClosingBehavior.FIRE_IF_NON_EMPTY)
                      .withTimestampCombiner(TimestampCombiner.EARLIEST));

      return input
          .get(INPUT_TAG)
          .apply(
              "Add window",
              Window.<Row>into(FixedWindows.of(Duration.standardMinutes(1)))
                  .discardingFiredPanes()
                  .withAllowedLateness(
                      Duration.standardHours(1), Window.ClosingBehavior.FIRE_IF_NON_EMPTY)
                  .withTimestampCombiner(TimestampCombiner.EARLIEST))
          .apply("Join files", Join.<Row, Row>leftOuterJoin(bytecodeFiles).using("file_id"))
          .apply("Reify window to global", Window.into(new GlobalWindows()))
          .apply(
              "Merge contracts and files",
              MapElements.into(TypeDescriptors.rows())
                  .via(
                      row -> {
                        var rhs = row.getRow("rhs");
                        var newRow = Row.fromRow(row.getRow("lhs"));
                        if (rhs != null) {
                          Optional.ofNullable(rhs.getBytes("content"))
                              .map(String::new)
                              .ifPresent(
                                  bytecode -> {
                                    try {
                                      newRow.withFieldValue(
                                          "bytecode",
                                          bytecode.startsWith("0x") ? bytecode : "0x" + bytecode);
                                      newRow.withFieldValue(
                                          "runtime_bytecode",
                                          BytecodeUtils.extractRuntimeBytecode(bytecode));
                                    } catch (Exception e) {
                                      log.warn(
                                          "There was an exception during extraction of bytecode from file %s"
                                              .formatted(rhs.getString("file_id")),
                                          e);
                                    }
                                  });
                        }

                        return newRow.build();
                      }))
          .setCoder(input.get(INPUT_TAG).getCoder());
    }
  }
}
