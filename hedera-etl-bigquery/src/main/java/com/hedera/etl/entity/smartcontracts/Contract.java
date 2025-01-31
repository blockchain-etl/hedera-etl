package com.hedera.etl.entity.smartcontracts;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.ContractDeleteTransactionBody;
import com.hederahashgraph.api.proto.java.ContractUpdateTransactionBody;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.TimestampRange;
import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.TimeUtils;

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
  @Nullable private String obtainer_id;
  @Nullable private Boolean permanent_removal;
  @Nullable private String proxy_account_id;
  @Nullable private TimestampRange timestamp;
  @Nullable private Boolean removed;
  @Nullable private byte[] bytecode;
  @Nullable private byte[] runtime_bytecode;

  public static Contract from(RecordItem recordItem) {
    if (recordItem.getTransactionRecord().hasContractCreateResult()
        || recordItem.getTransactionBody().hasContractCreateInstance()) {
      return from(recordItem, recordItem.getTransactionBody().getContractCreateInstance());
    }

    if (recordItem.getTransactionBody().hasContractUpdateInstance()) {
      return from(recordItem, recordItem.getTransactionBody().getContractUpdateInstance());
    }

    if (recordItem.getTransactionBody().hasContractDeleteInstance()) {
      return from(recordItem, recordItem.getTransactionBody().getContractDeleteInstance());
    }

    return null;
  }

  private static Contract from(
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
          DomainUtils.bytesToHex(
              DomainUtils.toBytes(contractCreateResult.getEvmAddress().getValue())));
    }

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasProxyAccountID()) {
      var proxyAccountId = EntityId.of(transactionBody.getProxyAccountID());
      builder.proxy_account_id(proxyAccountId.toString());
    }

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    builder
        .memo(transactionBody.getMemo())
        .removed(false)
        .modified(TimeUtils.fromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp);

    var contractId = recordItem.getTransactionRecord().getReceipt().getContractID();
    var sidecarRecords = recordItem.getSidecarRecords();

    for (var sidecar : sidecarRecords) {
      if (sidecar.hasBytecode() && !sidecar.getMigration()) {
        var bytecode = sidecar.getBytecode();
        if (contractId.equals(bytecode.getContractId())) {
          if (builder.bytecode == null) {
            builder.bytecode(DomainUtils.toBytes(bytecode.getInitcode()));
          }

          builder.runtime_bytecode(DomainUtils.toBytes(bytecode.getRuntimeBytecode()));
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

  private static Contract from(
      RecordItem recordItem, ContractDeleteTransactionBody transactionBody) {
    var consensusTimestamp = recordItem.getConsensusTimestamp();

    return builder()
        .contract_id(EntityId.of(transactionBody.getContractID()).toString())
        .modified(TimeUtils.fromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .removed(transactionBody.getPermanentRemoval())
        .build();
  }

  private static Contract from(
      RecordItem recordItem, ContractCreateTransactionBody transactionBody) {
    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    var builder =
        builder()
            .created(TimeUtils.fromNanos(consensusTimestamp))
            .created_timestamp(consensusTimestamp)
            .modified(TimeUtils.fromNanos(consensusTimestamp))
            .modified_timestamp(consensusTimestamp);

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
          DomainUtils.bytesToHex(
              DomainUtils.toBytes(contractCreateResult.getEvmAddress().getValue())));
    }

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasProxyAccountID()) {
      var proxyAccountId = EntityId.of(transactionBody.getProxyAccountID());
      builder.proxy_account_id(proxyAccountId.toString());
    }

    builder.memo(transactionBody.getMemo()).removed(false);

    switch (transactionBody.getInitcodeSourceCase()) {
      case FILEID:
        var fileId = EntityId.of(transactionBody.getFileID());
        builder.file_id(fileId.toString());
        break;
      case INITCODE:
        builder.bytecode(DomainUtils.toBytes(transactionBody.getInitcode()));
        break;
      default:
        break;
    }

    // for child transactions FileID is located in parent
    // ContractCreate/EthereumTransaction types
    // and initcode is located in the sidecar
    updateChildFromParent(builder, recordItem);

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
      ContractBuilder contract, RecordItem recordItem) {
    var transactionBody = recordItem.getTransactionBody().getContractCreateInstance();

    switch (transactionBody.getInitcodeSourceCase()) {
      case FILEID:
        if (contract.file_id == null) {
          var fileId = EntityId.of(transactionBody.getFileID());
          contract.file_id(fileId.toString());
          recordItem.addEntityId(fileId);
        }
        break;
      case INITCODE:
        if (contract.file_id == null) {
          contract.bytecode(DomainUtils.toBytes(transactionBody.getInitcode()));
        }
        break;
      default:
        break;
    }
  }

  private static void updateChildFromEthereumTransactionParent(
      ContractBuilder contract, RecordItem recordItem) {
    var body = recordItem.getTransactionBody().getEthereumTransaction();

    // use callData FileID if present
    if (body.hasCallData() && contract.file_id == null) {
      var fileId = EntityId.of(body.getCallData());
      contract.file_id(fileId.toString());
      return;
    }

    if (contract.bytecode == null && recordItem.getEthereumTransaction() != null) {
      contract.bytecode(recordItem.getEthereumTransaction().getCallData());
    }
  }
}
