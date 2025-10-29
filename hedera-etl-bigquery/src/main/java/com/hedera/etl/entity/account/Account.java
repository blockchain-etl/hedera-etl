package com.hedera.etl.entity.account;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.ContractDeleteTransactionBody;
import com.hederahashgraph.api.proto.java.ContractNonceInfo;
import com.hederahashgraph.api.proto.java.ContractUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoCreateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoGetInfoResponse;
import com.hederahashgraph.api.proto.java.CryptoUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.Key;
import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1;

import com.hedera.etl.entity.EntityUtils;
import com.hedera.etl.reader.recordfile.domain.transaction.EthereumTransaction;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.AbstractEntity;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

import static com.hederahashgraph.api.proto.java.CryptoUpdateTransactionBody.StakedIdCase.STAKEDID_NOT_SET;
import static org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.CONTEXT;
import static org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.SECP256K1_EC_UNCOMPRESSED;

import static com.hedera.etl.reader.recordfile.domain.transaction.RecordFile.HAPI_VERSION_0_27_0;
import static com.hedera.etl.reader.recordfile.utils.DomainUtils.EVM_ADDRESS_LENGTH;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Log4j2
public class Account {
  @Nullable private String account;
  @Nullable private String alias;
  @Nullable private Long auto_renew_period;
  // TODO Account Balance will be added in other entity/table
  // @Nullable  private Balance balance;
  @Nullable private String created;
  @Nullable private Long created_timestamp;
  @Nullable private String modified;
  @Nullable private Long modified_timestamp;
  @Nullable private Boolean decline_reward;
  @Nullable private Boolean deleted;
  @Nullable private Long ethereum_nonce;
  @Nullable private String evm_address;
  @Nullable private Long expiry_timestamp;
  @Nullable private com.hedera.etl.entity.Key key;
  @Nullable private Long max_automatic_token_associations;
  @Nullable private String memo;
  @Nullable private Long pending_reward;
  @Nullable private Boolean receiver_sig_required;
  @Nullable private String staked_account_id;
  @Nullable private Long staked_node_id;
  @Nullable private Long stake_period_start;

  private static final int ECDSA_SECP256K1_COMPRESSED_KEY_LENGTH = 33;

  public static Account from(RecordItem recordItem) {
    if (!recordItem.isSuccessful()) {
      return null;
    }

    if (recordItem.getTransactionBody().hasCryptoCreateAccount()) {
      return fromCreateAccount(
          recordItem, recordItem.getTransactionBody().getCryptoCreateAccount());
    }

    if (recordItem.getTransactionBody().hasCryptoUpdateAccount()) {
      return fromUpdateAccount(
          recordItem, recordItem.getTransactionBody().getCryptoUpdateAccount());
    }

    if (recordItem.getEthereumTransaction() != null) {
      return fromEthereumTransaction(recordItem, recordItem.getEthereumTransaction());
    }

    if (recordItem.getTransactionRecord().hasContractCreateResult()
        || recordItem.getTransactionBody().hasContractCreateInstance()) {
      return fromCreateContract(
          recordItem, recordItem.getTransactionBody().getContractCreateInstance());
    }

    if (recordItem.getTransactionBody().hasContractUpdateInstance()) {
      return fromUpdateContract(
          recordItem, recordItem.getTransactionBody().getContractUpdateInstance());
    }

    if (recordItem.getTransactionBody().hasContractDeleteInstance()) {
      return fromDeleteContract(
          recordItem, recordItem.getTransactionBody().getContractDeleteInstance());
    }

    return null;
  }

  public static Account fromAccountInfo(CryptoGetInfoResponse.AccountInfo accountInfo) {

    var accountId = EntityId.of(accountInfo.getAccountID());
    var builder =
        builder()
            .created(
                FormatUtils.timestampFromNanos(
                    1568411400000000000L)) // set everything to 2019-09-13 21:50:00
            .created_timestamp(1568411400000000000L)
            .modified(FormatUtils.timestampFromNanos(1568411400000000000L))
            .modified_timestamp(1568411400000000000L)
            .account(accountId.toString())
            .decline_reward(false)
            .deleted(accountInfo.getDeleted())
            .memo(accountInfo.getMemo())
            .max_automatic_token_associations((long) accountInfo.getMaxAutomaticTokenAssociations())
            .receiver_sig_required(accountInfo.getReceiverSigRequired())
            .evm_address(DomainUtils.bytesToHexWithPrefix(DomainUtils.toEvmAddress(accountId)));

    if (accountInfo.hasAutoRenewPeriod()) {
      builder.auto_renew_period(accountInfo.getAutoRenewPeriod().getSeconds());
    }

    if (accountInfo.hasExpirationTime()) {
      builder.expiry_timestamp(DomainUtils.timestampInNanosMax(accountInfo.getExpirationTime()));
    }

    if (accountInfo.hasKey()) {
      builder.key(com.hedera.etl.entity.Key.from(accountInfo.getKey()));
    }

    if (accountInfo.hasStakingInfo()) {
      switch (accountInfo.getStakingInfo().getStakedIdCase()) {
        case STAKEDID_NOT_SET:
          break;
        case STAKED_NODE_ID:
          builder.staked_node_id(accountInfo.getStakingInfo().getStakedNodeId());
          builder.staked_account_id(EntityId.of(AbstractEntity.ACCOUNT_ID_CLEARED).toString());
          break;
        case STAKED_ACCOUNT_ID:
          builder.staked_account_id(
              EntityId.of(accountInfo.getStakingInfo().getStakedAccountId()).toString());
          builder.staked_node_id(AbstractEntity.NODE_ID_CLEARED);
          break;
      }
    }

    return builder.build();
  }

  private static Account fromCreateAccount(
      RecordItem recordItem, CryptoCreateTransactionBody transactionBody) {
    var transactionRecord = recordItem.getTransactionRecord();

    var alias =
        DomainUtils.toBytes(
            transactionRecord.getAlias() != ByteString.EMPTY
                ? transactionRecord.getAlias()
                : transactionBody.getAlias());
    boolean emptyAlias = ArrayUtils.isEmpty(alias);
    var key = transactionBody.hasKey() ? transactionBody.getKey() : null;
    boolean emptyKey = key == null || ArrayUtils.isEmpty(key.toByteArray());

    AccountBuilder accountBuilder = Account.builder();

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    accountBuilder.created(FormatUtils.timestampFromNanos(consensusTimestamp));
    accountBuilder.created_timestamp(consensusTimestamp);
    accountBuilder.modified(FormatUtils.timestampFromNanos(consensusTimestamp));
    accountBuilder.modified_timestamp(consensusTimestamp);
    accountBuilder.ethereum_nonce(0L);
    // it seems that mirror-node doesn't set this field
    accountBuilder.pending_reward(0L);

    var accountId = EntityId.of(recordItem.getTransactionRecord().getReceipt().getAccountID());

    accountBuilder.account(accountId.toString());

    accountBuilder.deleted(recordItem.getTransactionBody().hasCryptoDelete());

    if (!emptyAlias) {
      accountBuilder.alias(DomainUtils.toBase32(alias));
      if (emptyKey && alias.length > EVM_ADDRESS_LENGTH) {
        try {
          accountBuilder.key(com.hedera.etl.entity.Key.from(Key.parseFrom(alias)));
        } catch (InvalidProtocolBufferException e) {
          log.warn(
              "Key %s can't be parsed as com.hederahashgraph.api.proto.java"
                  .formatted(DomainUtils.toBase64(alias)),
              e);
        }
      }
    }

    if (!emptyKey) {
      accountBuilder.key(com.hedera.etl.entity.Key.from(key));
    }

    var evmAddress = transactionRecord.getEvmAddress();
    if (!Objects.equals(evmAddress, ByteString.EMPTY)) {
      accountBuilder.evm_address(DomainUtils.bytesToHexWithPrefix(DomainUtils.toBytes(evmAddress)));
    } else if (!emptyAlias) {
      accountBuilder.evm_address(DomainUtils.bytesToHexWithPrefix(aliasToEvmAddress(alias)));
    }

    if (accountBuilder.evm_address == null) {
      accountBuilder.evm_address(
          DomainUtils.bytesToHexWithPrefix(DomainUtils.toEvmAddress(accountId)));
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      accountBuilder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    accountBuilder.max_automatic_token_associations(
        Optional.of(transactionBody.getMaxAutomaticTokenAssociations())
            .map(Integer::longValue)
            .orElse(null));
    accountBuilder.memo(transactionBody.getMemo());
    accountBuilder.receiver_sig_required(transactionBody.getReceiverSigRequired());

    accountBuilder.expiry_timestamp(
        EntityUtils.getEffectiveExpiration(
            null, accountBuilder.created_timestamp, accountBuilder.auto_renew_period));

    if (recordItem.getHapiVersion().isLessThan(HAPI_VERSION_0_27_0)) {
      return accountBuilder.build();
    }
    accountBuilder.decline_reward(transactionBody.getDeclineReward());

    switch (transactionBody.getStakedIdCase()) {
      case STAKEDID_NOT_SET -> {
        return accountBuilder.build();
      }
      case STAKED_NODE_ID -> accountBuilder.staked_node_id(transactionBody.getStakedNodeId());
      case STAKED_ACCOUNT_ID -> {
        var stakedAccountId = EntityId.of(transactionBody.getStakedAccountId());
        accountBuilder.staked_account_id(stakedAccountId.toString());
      }
    }

    accountBuilder.stake_period_start(getEpochDay(recordItem.getConsensusTimestamp()));

    return accountBuilder.build();
  }

  private static Account fromUpdateAccount(
      RecordItem recordItem, CryptoUpdateTransactionBody transactionBody) {
    Account account = new Account();

    var consensusTimestamp = recordItem.getConsensusTimestamp();
    account.setModified(FormatUtils.timestampFromNanos(consensusTimestamp));
    account.setModified_timestamp(consensusTimestamp);

    account.setAccount(EntityId.of(transactionBody.getAccountIDToUpdate()).toString());

    if (transactionBody.hasAutoRenewPeriod()) {
      account.setAuto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (transactionBody.hasExpirationTime()) {
      account.setExpiry_timestamp(
          DomainUtils.timestampInNanosMax(transactionBody.getExpirationTime()));
    }

    if (transactionBody.hasKey()) {
      account.setKey(com.hedera.etl.entity.Key.from(transactionBody.getKey()));
    }

    if (transactionBody.hasMaxAutomaticTokenAssociations()) {
      account.setMax_automatic_token_associations(
          (long) transactionBody.getMaxAutomaticTokenAssociations().getValue());
    }

    if (transactionBody.hasMemo()) {
      account.setMemo(transactionBody.getMemo().getValue());
    }

    if (transactionBody.hasReceiverSigRequiredWrapper()) {
      account.setReceiver_sig_required(transactionBody.getReceiverSigRequiredWrapper().getValue());
    } else if (transactionBody.getReceiverSigRequired()) {
      // support old transactions
      account.setReceiver_sig_required(transactionBody.getReceiverSigRequired());
    }

    if (recordItem.getHapiVersion().isLessThan(HAPI_VERSION_0_27_0)) {
      return account;
    }

    if (transactionBody.hasDeclineReward()) {
      account.setDecline_reward(transactionBody.getDeclineReward().getValue());
    }

    switch (transactionBody.getStakedIdCase()) {
      case STAKEDID_NOT_SET:
        break;
      case STAKED_NODE_ID:
        account.setStaked_node_id(transactionBody.getStakedNodeId());
        account.setStaked_account_id(EntityId.of(AbstractEntity.ACCOUNT_ID_CLEARED).toString());
        break;
      case STAKED_ACCOUNT_ID:
        var accountId = EntityId.of(transactionBody.getStakedAccountId());
        account.setStaked_account_id(accountId.toString());
        account.setStaked_node_id(AbstractEntity.NODE_ID_CLEARED);
        break;
    }

    // If the stake node id or the decline reward value has changed, we start a new stake period.
    if (transactionBody.getStakedIdCase() != STAKEDID_NOT_SET
        || transactionBody.hasDeclineReward()) {
      account.setStake_period_start(getEpochDay(recordItem.getConsensusTimestamp()));
    }

    return account;
  }

  private static Account fromEthereumTransaction(
      RecordItem recordItem, EthereumTransaction ethereumTransaction) {

    var transactionRecord = recordItem.getTransactionRecord();
    if (!transactionRecord.hasContractCallResult()
        && !transactionRecord.hasContractCreateResult()) {
      return null;
    }

    var functionResult =
        transactionRecord.hasContractCreateResult()
            ? transactionRecord.getContractCreateResult()
            : transactionRecord.getContractCallResult();
    var senderId = EntityId.of(functionResult.getSenderId());
    if (EntityId.isEmpty(senderId)) {
      return null;
    }

    Long nonce = null;
    if (functionResult.hasSignerNonce()) {
      nonce = functionResult.getSignerNonce().getValue();
    } else if (recordItem.getHapiVersion().isLessThan(RecordFile.HAPI_VERSION_0_47_0)) {
      var status = transactionRecord.getReceipt().getStatus();
      if (!recordItem.isSuccessful()
          && status != ResponseCodeEnum.CONTRACT_REVERT_EXECUTED
          && status != ResponseCodeEnum.MAX_CHILD_RECORDS_EXCEEDED) {
        return null;
      }

      // Increment the nonce for backwards compatibility
      nonce = Optional.ofNullable(ethereumTransaction.getNonce()).orElse(0L) + 1L;
    }

    if (nonce != null) {
      return builder()
          .modified(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
          .modified_timestamp(recordItem.getConsensusTimestamp())
          .account(senderId.toString())
          .ethereum_nonce(nonce)
          .build();
    }

    return null;
  }

  private static Account fromCreateContract(
      RecordItem recordItem, ContractCreateTransactionBody transactionBody) {
    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    var contractId = recordItem.getTransactionRecord().getReceipt().getContractID();

    var builder =
        builder()
            .account(EntityId.of(contractId).toString())
            .evm_address(DomainUtils.bytesToHexWithPrefix(DomainUtils.toEvmAddress(contractId)))
            .max_automatic_token_associations(
                (long) transactionBody.getMaxAutomaticTokenAssociations())
            .created(FormatUtils.timestampFromNanos(consensusTimestamp))
            .created_timestamp(consensusTimestamp)
            .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
            .modified_timestamp(consensusTimestamp)
            .memo(transactionBody.getMemo())
            .decline_reward(transactionBody.getDeclineReward())
            .pending_reward(0L)
            .ethereum_nonce(
                recordItem
                    .getTransactionRecord()
                    .getContractCreateResult()
                    .getContractNoncesList()
                    .stream()
                    .filter(info -> Objects.equals(info.getContractId(), contractId))
                    .map(ContractNonceInfo::getNonce)
                    .findFirst()
                    .orElse(null))
            .deleted(false);

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (contractCreateResult.hasEvmAddress()) {
      builder.evm_address(
          DomainUtils.bytesToHexWithPrefix(
              DomainUtils.toBytes(contractCreateResult.getEvmAddress().getValue())));
    }

    if (transactionBody.hasAdminKey()) {
      builder.key(com.hedera.etl.entity.Key.from(transactionBody.getAdminKey()));
    }

    switch (transactionBody.getStakedIdCase()) {
      case STAKEDID_NOT_SET:
        break;
      case STAKED_NODE_ID:
        builder.staked_node_id(transactionBody.getStakedNodeId());
        builder.staked_account_id(EntityId.of(AbstractEntity.ACCOUNT_ID_CLEARED).toString());
        break;
      case STAKED_ACCOUNT_ID:
        var accountId = EntityId.of(transactionBody.getStakedAccountId());
        builder.staked_account_id(accountId.toString());
        builder.staked_node_id(AbstractEntity.NODE_ID_CLEARED);
        break;
    }

    return builder
        .expiry_timestamp(
            EntityUtils.getEffectiveExpiration(
                null, builder.created_timestamp, builder.auto_renew_period))
        .build();
  }

  private static Account fromUpdateContract(
      RecordItem recordItem, ContractUpdateTransactionBody transactionBody) {
    var contractCreateResult = recordItem.getTransactionRecord().getContractCreateResult();

    var consensusTimestamp = recordItem.getConsensusTimestamp();

    var contractId = recordItem.getTransactionRecord().getReceipt().getContractID();
    var builder =
        builder()
            .account(EntityId.of(contractId).toString())
            .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
            .modified_timestamp(consensusTimestamp);

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (contractCreateResult.hasEvmAddress()) {
      builder.evm_address(
          DomainUtils.bytesToHexWithPrefix(
              contractCreateResult.getEvmAddress().getValue().toByteArray()));
    }

    if (transactionBody.hasAdminKey()) {
      builder.key(com.hedera.etl.entity.Key.from(transactionBody.getAdminKey()));
    }

    switch (transactionBody.getStakedIdCase()) {
      case STAKEDID_NOT_SET:
        break;
      case STAKED_NODE_ID:
        builder.staked_node_id(transactionBody.getStakedNodeId());
        builder.staked_account_id(EntityId.of(AbstractEntity.ACCOUNT_ID_CLEARED).toString());
        break;
      case STAKED_ACCOUNT_ID:
        var accountId = EntityId.of(transactionBody.getStakedAccountId());
        builder.staked_account_id(accountId.toString());
        builder.staked_node_id(AbstractEntity.NODE_ID_CLEARED);
        break;
    }

    if (transactionBody.hasExpirationTime()) {
      builder.expiry_timestamp(
          DomainUtils.timestampInNanosMax(transactionBody.getExpirationTime()));
    }

    if (transactionBody.hasMaxAutomaticTokenAssociations()) {
      builder.max_automatic_token_associations(
          (long) transactionBody.getMaxAutomaticTokenAssociations().getValue());
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

    return builder.build();
  }

  private static Account fromDeleteContract(
      RecordItem recordItem, ContractDeleteTransactionBody transactionBody) {
    var consensusTimestamp = recordItem.getConsensusTimestamp();

    return builder()
        .account(EntityId.of(transactionBody.getContractID()).toString())
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .deleted(true)
        .build();
  }

  // below methods are copied from hedera-mirror-node Utility.java and should be kept in sync

  /**
   * Converts an ECDSA secp256k1 alias to a 20 byte EVM address by taking the keccak hash of it.
   * Logic copied from services' AliasManager.
   *
   * @param alias the bytes representing a serialized Key protobuf
   * @return the 20 byte EVM address
   */
  @SuppressWarnings("java:S1168")
  public static byte[] aliasToEvmAddress(byte[] alias) {
    if (alias == null
        || alias.length != DomainUtils.EVM_ADDRESS_LENGTH
            && alias.length < ECDSA_SECP256K1_COMPRESSED_KEY_LENGTH) {
      return null;
    }

    if (alias.length == DomainUtils.EVM_ADDRESS_LENGTH) {
      return alias;
    }

    byte[] evmAddress = null;
    try {
      var key = Key.parseFrom(alias);
      if (key.getKeyCase() == Key.KeyCase.ECDSA_SECP256K1
          && key.getECDSASecp256K1().size() == ECDSA_SECP256K1_COMPRESSED_KEY_LENGTH) {
        byte[] rawCompressedKey = DomainUtils.toBytes(key.getECDSASecp256K1());
        evmAddress = recoverAddressFromPubKey(rawCompressedKey);
        if (evmAddress == null) {
          log.warn("Unable to recover EVM address from {}", Hex.encodeHexString(rawCompressedKey));
        }
      }
    } catch (Exception e) {
      var aliasHex = Hex.encodeHexString(alias);
      log.error("Unable to decode alias to EVM address: {}", aliasHex, e);
    }

    return evmAddress;
  }

  // This method is copied from hedera-services EthTxSigs::recoverAddressFromPubKey and should be
  // kept in sync
  @SuppressWarnings("java:S1191")
  private static byte[] recoverAddressFromPubKey(LibSecp256k1.secp256k1_pubkey pubKey) {
    final ByteBuffer recoveredFullKey = ByteBuffer.allocate(65);
    int value = recoveredFullKey.limit();
    final com.sun.jna.ptr.LongByReference fullKeySize = new com.sun.jna.ptr.LongByReference(value);
    LibSecp256k1.secp256k1_ec_pubkey_serialize(
        CONTEXT, recoveredFullKey, fullKeySize, pubKey, SECP256K1_EC_UNCOMPRESSED);

    recoveredFullKey.get(); // read and discard - recoveryId is not part of the account hash
    var preHash = new byte[64];
    recoveredFullKey.get(preHash, 0, 64);
    var keyHash = new Keccak.Digest256().digest(preHash);
    var address = new byte[20];
    System.arraycopy(keyHash, 12, address, 0, 20);
    return address;
  }

  // This method is copied from hedera-services EthTxSigs::recoverAddressFromPubKey and should be
  // kept in sync
  @SuppressWarnings("java:S1168")
  private static byte[] recoverAddressFromPubKey(byte[] pubKeyBytes) {
    LibSecp256k1.secp256k1_pubkey pubKey = new LibSecp256k1.secp256k1_pubkey();
    var parseResult =
        LibSecp256k1.secp256k1_ec_pubkey_parse(CONTEXT, pubKey, pubKeyBytes, pubKeyBytes.length);
    if (parseResult == 1) {
      return recoverAddressFromPubKey(pubKey);
    } else {
      return null;
    }
  }

  /**
   * Gets epoch day from the timestamp in nanos.
   *
   * @param timestamp The timestamp in nanos
   * @return The epoch day
   */
  public static long getEpochDay(long timestamp) {
    return LocalDate.ofInstant(Instant.ofEpochSecond(0, timestamp), ZoneOffset.UTC)
        .atStartOfDay()
        .toLocalDate()
        .toEpochDay();
  }
}
