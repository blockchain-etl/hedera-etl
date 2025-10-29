package com.hedera.etl.entity.token;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.TokenBurnTransactionBody;
import com.hederahashgraph.api.proto.java.TokenCreateTransactionBody;
import com.hederahashgraph.api.proto.java.TokenDeleteTransactionBody;
import com.hederahashgraph.api.proto.java.TokenMintTransactionBody;
import com.hederahashgraph.api.proto.java.TokenPauseTransactionBody;
import com.hederahashgraph.api.proto.java.TokenUnpauseTransactionBody;
import com.hederahashgraph.api.proto.java.TokenUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.TokenWipeAccountTransactionBody;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.jetbrains.annotations.NotNull;

import com.hedera.etl.entity.Key;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

import static com.hedera.etl.reader.recordfile.domain.transaction.RecordFile.HAPI_VERSION_0_49_0;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Token {

  @Nullable private Key admin_key;
  @Nullable private String auto_renew_account;
  @Nullable private Long auto_renew_period;
  @Nullable private String created;
  @Nullable private Long created_timestamp;
  @Nullable private CustomFees custom_fees;
  @Nullable private Integer decimals;
  @Nullable private Boolean deleted;
  @Nullable private Long expiry_timestamp;
  @Nullable private Key fee_schedule_key;
  @Nullable private Boolean freeze_default;
  @Nullable private Key freeze_key;
  @Nullable private Long initial_supply;
  @Nullable private Key kyc_key;
  @Nullable private Long max_supply;
  @Nullable private String memo;
  @Nullable private byte[] metadata;
  @Nullable private Key metadata_key;
  @Nullable private String modified;
  @Nullable private Long modified_timestamp;
  @Nullable private String name;
  @Nullable private Key pause_key;

  @Nullable private String pause_status;

  @Nullable private Key supply_key;

  @Nullable private String supply_type;

  @Nullable private String symbol;

  @Nullable private String token_id;

  @Nullable private Long total_supply;

  @Nullable private String treasury_account_id;

  @Nullable private String type;

  @Nullable private Key wipe_key;

  public static Token from(RecordItem recordItem) {
    var transactionBody = recordItem.getTransactionBody();
    if (transactionBody.hasTokenCreation()) {
      return fromTokenCreation(recordItem, transactionBody.getTokenCreation());
    }
    if (transactionBody.hasTokenUpdate()) {
      return fromTokenUpdate(recordItem, transactionBody.getTokenUpdate());
    }
    if (transactionBody.hasTokenPause()) {
      return fromTokenPause(recordItem, transactionBody.getTokenPause());
    }
    if (transactionBody.hasTokenUnpause()) {
      return fromTokenUnpause(recordItem, transactionBody.getTokenUnpause());
    }
    // Unused hasTokenAirdrop()
    // Unused hasTokenCancelAirdrop()
    // Unused hasTokenClaimAirdrop()
    // Unused hasTokenAssociate()
    // Unused hasTokenDissociate())
    // Unused hasTokenFreeze()
    // Unused hasTokenUnfreeze()
    if (transactionBody.hasTokenMint()) {
      return fromTokenMint(recordItem, transactionBody.getTokenMint());
    }
    if (transactionBody.hasTokenBurn()) {
      return fromTokenBurn(recordItem, transactionBody.getTokenBurn());
    }
    if (transactionBody.hasTokenWipe()) {
      return fromTokenWipe(recordItem, transactionBody.getTokenWipe());
    }
    if (transactionBody.hasTokenDeletion()) {
      return fromTokenDeletion(recordItem, transactionBody.getTokenDeletion());
    }
    // unused hasTokenGrantKyc()
    // unused hasTokenRevokeKyc()
    // unused hasTokenUpdateNfts()
    return null;
  }

  private static Token fromTokenBurn(
      RecordItem recordItem, TokenBurnTransactionBody transactionBody) {
    var tokenId = EntityId.of(transactionBody.getToken());
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    long newTotalSupply = recordItem.getTransactionRecord().getReceipt().getNewTotalSupply();

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .total_supply(newTotalSupply)
        .build();
  }

  private static Token fromTokenCreation(
      RecordItem recordItem, TokenCreateTransactionBody transactionBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var tokenId = EntityId.of(recordItem.getTransactionRecord().getReceipt().getTokenID());
    var treasury = EntityId.of(transactionBody.getTreasury());

    var tokenBuilder =
        Token.builder()
            .created(FormatUtils.timestampFromNanos(consensusTimestamp))
            .created_timestamp(consensusTimestamp)
            .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
            .modified_timestamp(consensusTimestamp)
            .decimals(transactionBody.getDecimals())
            .freeze_default(transactionBody.getFreezeDefault())
            .initial_supply(transactionBody.getInitialSupply())
            .max_supply(transactionBody.getMaxSupply())
            .name(transactionBody.getName())
            .supply_type(SupplyType.fromId(transactionBody.getSupplyTypeValue()).name())
            .symbol(transactionBody.getSymbol())
            .token_id(tokenId.toString())
            .total_supply(transactionBody.getInitialSupply())
            .treasury_account_id(treasury.toString())
            .type(Type.fromId(transactionBody.getTokenTypeValue()).name())
            .custom_fees(
                CustomFees.from(consensusTimestamp, transactionBody.getCustomFeesList(), tokenId))
            .deleted(false);

    if (transactionBody.hasFeeScheduleKey()) {
      tokenBuilder.fee_schedule_key(Key.from(transactionBody.getFeeScheduleKey()));
    }

    if (transactionBody.hasAdminKey()) {
      tokenBuilder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasFreezeKey()) {
      tokenBuilder.freeze_key(Key.from(transactionBody.getFreezeKey()));
    }

    if (transactionBody.hasKycKey()) {
      tokenBuilder.kyc_key(Key.from(transactionBody.getKycKey()));
    }

    if (transactionBody.hasAutoRenewAccount()) {
      tokenBuilder.auto_renew_account(
          EntityId.of(transactionBody.getAutoRenewAccount()).toString());
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      tokenBuilder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (transactionBody.hasExpiry()) {
      tokenBuilder.expiry_timestamp(DomainUtils.timestampInNanosMax(transactionBody.getExpiry()));
    }

    // metadata and metadata key fields are supported from services 0.49.0. This is
    // a workaround of
    // the issue that
    // services 0.48.x processes such transactions as if the fields are not present.
    if (recordItem.getHapiVersion().isGreaterThanOrEqualTo(HAPI_VERSION_0_49_0)) {
      tokenBuilder.metadata(transactionBody.getMetadata().toByteArray());

      if (transactionBody.hasMetadataKey()) {
        tokenBuilder.metadata_key(Key.from(transactionBody.getMetadataKey()));
      }
    }

    if (transactionBody.hasPauseKey()) {
      tokenBuilder
          .pause_key(Key.from(transactionBody.getPauseKey()))
          .pause_status(PauseStatus.UNPAUSED.name());
    } else {
      tokenBuilder.pause_status(PauseStatus.NOT_APPLICABLE.name());
    }

    if (transactionBody.hasSupplyKey()) {
      tokenBuilder.supply_key(Key.from(transactionBody.getSupplyKey()));
    }

    if (transactionBody.hasWipeKey()) {
      tokenBuilder.wipe_key(Key.from(transactionBody.getWipeKey()));
    }
    return tokenBuilder.build();
  }

  private static Token fromTokenDeletion(
      RecordItem recordItem, TokenDeleteTransactionBody tokenDeletion) {
    var tokenId = EntityId.of(tokenDeletion.getToken());
    long consensusTimestamp = recordItem.getConsensusTimestamp();

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .deleted(true)
        .build();
  }

  private static Token fromTokenMint(
      RecordItem recordItem, @NotNull TokenMintTransactionBody transactionBody) {
    var tokenId = EntityId.of(transactionBody.getToken());
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    long newTotalSupply = recordItem.getTransactionRecord().getReceipt().getNewTotalSupply();

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .total_supply(newTotalSupply)
        .build();
  }

  private static Token fromTokenPause(
      RecordItem recordItem, TokenPauseTransactionBody transactionBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var tokenId = EntityId.of(transactionBody.getToken());

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .pause_status(PauseStatus.PAUSED.name())
        .build();
  }

  private static Token fromTokenUnpause(
      RecordItem recordItem, TokenUnpauseTransactionBody transactionBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var tokenId = EntityId.of(transactionBody.getToken());

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .pause_status(PauseStatus.UNPAUSED.name())
        .build();
  }

  private static Token fromTokenUpdate(
      RecordItem recordItem, TokenUpdateTransactionBody transactionBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var tokenId = EntityId.of(transactionBody.getToken());

    var builder = builder();

    if (transactionBody.hasAdminKey()) {
      builder.admin_key(Key.from(transactionBody.getAdminKey()));
    }

    if (transactionBody.hasAutoRenewAccount()) {
      builder.auto_renew_account(EntityId.of(transactionBody.getAutoRenewAccount()).toString());
    }

    if (transactionBody.hasAutoRenewPeriod()) {
      builder.auto_renew_period(transactionBody.getAutoRenewPeriod().getSeconds());
    }

    if (transactionBody.hasExpiry()) {
      builder.expiry_timestamp(DomainUtils.timestampInNanosMax(transactionBody.getExpiry()));
    }

    if (transactionBody.hasMemo()) {
      builder.memo(transactionBody.getMemo().getValue());
    }

    builder
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString());

    if (transactionBody.hasFeeScheduleKey()) {
      builder.fee_schedule_key(Key.from(transactionBody.getFeeScheduleKey()));
    }

    if (transactionBody.hasFreezeKey()) {
      builder.freeze_key(Key.from(transactionBody.getFreezeKey()));
    }

    if (transactionBody.hasKycKey()) {
      builder.kyc_key(Key.from(transactionBody.getKycKey()));
    }

    // metadata and metadata key fields are supported from services 0.49.0. This is
    // a workaround of
    // the issue that
    // services 0.48.x processes such transactions as if the fields are not present.
    if (recordItem.getHapiVersion().isGreaterThanOrEqualTo(HAPI_VERSION_0_49_0)) {
      if (transactionBody.hasMetadata()) {
        builder.metadata(DomainUtils.toBytes(transactionBody.getMetadata().getValue()));
      }

      if (transactionBody.hasMetadataKey()) {
        builder.metadata_key(Key.from(transactionBody.getMetadataKey()));
      }
    }

    if (!transactionBody.getName().isEmpty()) {
      builder.name(transactionBody.getName());
    }

    if (transactionBody.hasPauseKey()) {
      builder.pause_key(Key.from(transactionBody.getPauseKey()));
    }

    if (transactionBody.hasSupplyKey()) {
      builder.supply_key(Key.from(transactionBody.getSupplyKey()));
    }

    if (!transactionBody.getSymbol().isEmpty()) {
      builder.symbol(transactionBody.getSymbol());
    }

    if (transactionBody.hasTreasury()) {
      var treasury = EntityId.of(transactionBody.getTreasury());
      builder.treasury_account_id(treasury.toString());
    }

    if (transactionBody.hasWipeKey()) {
      builder.wipe_key(Key.from(transactionBody.getWipeKey()));
    }

    return builder.build();
  }

  private static Token fromTokenWipe(
      RecordItem recordItem, TokenWipeAccountTransactionBody transactionBody) {
    var tokenId = EntityId.of(transactionBody.getToken());
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    long newTotalSupply = recordItem.getTransactionRecord().getReceipt().getNewTotalSupply();

    return builder()
        .modified(FormatUtils.timestampFromNanos(consensusTimestamp))
        .modified_timestamp(consensusTimestamp)
        .token_id(tokenId.toString())
        .total_supply(newTotalSupply)
        .build();
  }

  public enum PauseStatus {
    NOT_APPLICABLE,
    PAUSED,
    UNPAUSED
  }

  @Getter
  @RequiredArgsConstructor
  public enum SupplyType {
    FINITE(1),
    INFINITE(0);

    private static final Map<Integer, SupplyType> ID_MAP =
        Arrays.stream(values())
            .collect(Collectors.toUnmodifiableMap(SupplyType::getId, Function.identity()));

    public static SupplyType fromId(int id) {
      return ID_MAP.getOrDefault(id, INFINITE);
    }

    private final int id;
  }

  @Getter
  @RequiredArgsConstructor
  public enum Type {
    FUNGIBLE_COMMON(0),
    NON_FUNGIBLE_UNIQUE(1);

    private static final Map<Integer, Type> ID_MAP =
        Arrays.stream(values())
            .collect(Collectors.toUnmodifiableMap(Type::getId, Function.identity()));

    public static Type fromId(int id) {
      return ID_MAP.getOrDefault(id, FUNGIBLE_COMMON);
    }

    private final int id;
  }
}
