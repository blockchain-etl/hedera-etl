package com.hedera.etl.entity.smartcontracts;

import com.hedera.etl.entity.TimestampRange;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.recordfile.domain.transaction.RecordItem;

import com.hedera.etl.recordfile.entity.EntityId;
import com.hedera.etl.recordfile.utils.DomainUtils;

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
public class Contract {
  @Nullable private Key admin_key;
  @Nullable private String auto_renew_account;
  @Nullable private Long auto_renew_period;
  @Nullable private String contract_id;
  @Nullable private Long created_timestamp;
  @Nullable private String evm_address;
  @Nullable private Long expiration_timestamp;
  @Nullable private String file_id;
  @Nullable private Integer max_automatic_token_associations;
  @Nullable private String memo;
  @Nullable private String obtainer_id;
  @Nullable private Boolean permanent_removal;
  @Nullable private String proxy_account_id;
  @Nullable private TimestampRange timestampRange;
  @Nullable private byte[] bytecode;
  @Nullable private byte[] runtime_bytecode;

  public static Contract from(RecordItem recordItem) {
    if (recordItem.getTransactionRecord().hasContractCreateResult()
            || recordItem.getTransactionBody().hasContractCreateInstance()) {

    }

    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();
    var transactionBody = recordItem.getTransactionBody().getContractCreateInstance();

    var builder = builder();

    if (transactionBody.hasAutoRenewAccountId()) {
      // TODO: get hold of lookup
//      var autoRenewAccount = entityIdService
//              .lookup(transactionBody.getAutoRenewAccountId())
//              .orElse(EntityId.EMPTY);
//      if (!EntityId.isEmpty(autoRenewAccount)) {
//        entity.setAutoRenewAccountId(autoRenewAccount.getId());
//        recordItem.addEntityId(autoRenewAccount);
//      } else {
//        Utility.handleRecoverableError("Invalid autoRenewAccountId at {}", recordItem.getConsensusTimestamp());
//      }
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (contractCreateResult.hasEvmAddress()) {
      builder.evm_address(
              DomainUtils.bytesToHex(DomainUtils.toBytes(contractCreateResult.getEvmAddress().getValue())));
    }

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasProxyAccountID()) {
      var proxyAccountId = EntityId.of(transactionBody.getProxyAccountID());
      builder.proxy_account_id(proxyAccountId.toString());
    }

    builder
            .memo(transactionBody.getMemo());

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

    // for child transactions FileID is located in parent ContractCreate/EthereumTransaction types
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
      case ETHEREUMTRANSACTION -> updateChildFromEthereumTransactionParent(builder, parentRecordItem);
      default -> {
        // no-op
      }
    }
  }

  private static void updateChildFromContractCreateParent(ContractBuilder contract, RecordItem recordItem) {
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

  private static void updateChildFromEthereumTransactionParent(ContractBuilder contract, RecordItem recordItem) {
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
