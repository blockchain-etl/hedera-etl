package com.hedera.etl.entity.account;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.hederahashgraph.api.proto.java.Key;
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

import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.AbstractEntity;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.TimeUtils;

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

  @Nullable private byte[] alias;

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

  @Nullable private byte[] evm_address;

  @Nullable private Long expiry_timestamp;

  @Nullable private byte[] key;

  @Nullable private Integer max_automatic_token_associations;

  @Nullable private String memo;

  @Nullable private Long pending_reward;

  @Nullable private Boolean receiver_sig_required;

  @Nullable private Long staked_account_id;

  @Nullable private Long staked_node_id;

  @Nullable private Long stake_period_start;

  private static final int ECDSA_SECP256K1_COMPRESSED_KEY_LENGTH = 33;

  public static Account from(RecordItem recordItem) {

    if (recordItem.getTransactionBody().hasCryptoCreateAccount()) {

      var transactionRecord = recordItem.getTransactionRecord();
      var transactionBody = recordItem.getTransactionBody().getCryptoCreateAccount();
      var alias =
          DomainUtils.toBytes(
              transactionRecord.getAlias() != ByteString.EMPTY
                  ? transactionRecord.getAlias()
                  : transactionBody.getAlias());
      boolean emptyAlias = ArrayUtils.isEmpty(alias);
      var key = transactionBody.hasKey() ? transactionBody.getKey().toByteArray() : null;
      boolean emptyKey = ArrayUtils.isEmpty(key);

      AccountBuilder accountBuilder = Account.builder();

      var consensusTimestamp = recordItem.getConsensusTimestamp();

      accountBuilder.created(TimeUtils.fromNanos(consensusTimestamp));
      accountBuilder.created_timestamp(consensusTimestamp);
      accountBuilder.modified(TimeUtils.fromNanos(consensusTimestamp));
      accountBuilder.modified_timestamp(consensusTimestamp);

      accountBuilder.account(
          EntityId.of(recordItem.getTransactionRecord().getReceipt().getAccountID()).toString());

      accountBuilder.deleted(recordItem.getTransactionBody().hasCryptoDelete());

      if (!emptyAlias) {
        accountBuilder.alias(alias);
        if (emptyKey && alias.length > EVM_ADDRESS_LENGTH) {
          accountBuilder.key(alias);
        }
      }

      if (!emptyKey) {
        accountBuilder.key(key);
      }

      var evmAddress = transactionRecord.getEvmAddress();
      if (evmAddress != ByteString.EMPTY) {
        accountBuilder.evm_address(DomainUtils.toBytes(evmAddress));
      } else if (!emptyAlias) {
        accountBuilder.evm_address(aliasToEvmAddress(alias));
      }

      if (transactionBody.hasAutoRenewPeriod()) {
        accountBuilder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
      }

      accountBuilder.max_automatic_token_associations(
          transactionBody.getMaxAutomaticTokenAssociations());
      accountBuilder.memo(transactionBody.getMemo());
      accountBuilder.receiver_sig_required(transactionBody.getReceiverSigRequired());

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
          var accountId = EntityId.of(transactionBody.getStakedAccountId());
          accountBuilder.staked_account_id(accountId.getId());
        }
      }

      accountBuilder.stake_period_start(getEpochDay(recordItem.getConsensusTimestamp()));

      return accountBuilder.build();
    }
    if (recordItem.getTransactionBody().hasCryptoUpdateAccount()) {

      Account account = new Account();

      var transactionBody = recordItem.getTransactionBody().getCryptoUpdateAccount();

      var consensusTimestamp = recordItem.getConsensusTimestamp();
      account.setModified(TimeUtils.fromNanos(consensusTimestamp));
      account.setModified_timestamp(consensusTimestamp);

      if (transactionBody.hasAutoRenewPeriod()) {
        account.setAuto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
      }

      if (transactionBody.hasExpirationTime()) {
        account.setExpiry_timestamp(
            DomainUtils.timestampInNanosMax(transactionBody.getExpirationTime()));
      }

      if (transactionBody.hasKey()) {
        account.setKey(transactionBody.getKey().toByteArray());
      }

      if (transactionBody.hasMaxAutomaticTokenAssociations()) {
        account.setMax_automatic_token_associations(
            transactionBody.getMaxAutomaticTokenAssociations().getValue());
      }

      if (transactionBody.hasMemo()) {
        account.setMemo(transactionBody.getMemo().getValue());
      }

      if (transactionBody.hasReceiverSigRequiredWrapper()) {
        account.setReceiver_sig_required(
            transactionBody.getReceiverSigRequiredWrapper().getValue());
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
          account.setStaked_account_id(AbstractEntity.ACCOUNT_ID_CLEARED);
          break;
        case STAKED_ACCOUNT_ID:
          var accountId = EntityId.of(transactionBody.getStakedAccountId());
          account.setStaked_account_id(accountId.getId());
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

    return null;
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
