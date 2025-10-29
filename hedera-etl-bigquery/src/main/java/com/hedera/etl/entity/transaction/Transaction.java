package com.hedera.etl.entity.transaction;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.ResponseCodeEnum;
import com.hederahashgraph.api.proto.java.TokenID;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.Key;
import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Log4j2
public class Transaction {

  @Nullable private String created;
  @Nullable private Long consensus_timestamp;
  @Nullable private Long charged_tx_fee;
  @Nullable private String entity_id;
  @Nullable private String transaction_id;
  @Nullable private Integer index;
  @Nullable private Long initial_balance;
  @Nullable private String memo_base64;
  @Nullable private Long max_fee;
  @Nullable private List<NftTransfer> nft_transfers;
  @Nullable private String node;
  @Nullable private Integer nonce;
  @Nullable private Long parent_consensus_timestamp;
  @Nullable private String payer_account_id;
  @Nullable private Integer resultCode;
  @Nullable private String result;
  @Nullable private Boolean scheduled;
  @Nullable private String bytes;
  @Nullable private String transaction_hash;
  @Nullable private Integer type;
  @Nullable private String name;
  @Nullable private Long valid_duration_seconds;
  @Nullable private Long valid_start_timestamp;
  @Nullable private Long block_timestamp_to;
  @Nullable private List<StakingRewardTransfer> staking_reward_transfers;
  @Nullable private List<TransactionTokenTransfersInner> token_transfers;
  @Nullable private List<TransactionTransfersInner> transfers;
  @Nullable private List<AssessedCustomFee> assessed_custom_fees;
  @Nullable private Key batch_key;
  @Nullable private List<MaxCustomFee> max_custom_fees;

  public static Transaction from(RecordItem recordItem) {

    TransactionBody body = recordItem.getTransactionBody();
    TransactionRecord txRecord = recordItem.getTransactionRecord();

    Long validDurationSeconds =
        body.hasTransactionValidDuration() ? body.getTransactionValidDuration().getSeconds() : null;
    var transactionId = body.getTransactionID();

    Transaction.TransactionBuilder transactionBuilder = Transaction.builder();

    transactionBuilder.charged_tx_fee(txRecord.getTransactionFee());
    transactionBuilder.created(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()));
    transactionBuilder.consensus_timestamp(recordItem.getConsensusTimestamp());
    transactionBuilder.transaction_id(FormatUtils.transactionId(transactionId));
    transactionBuilder.entity_id(
        EntityIdExtractor.getEntity(recordItem).map(EntityId::toString).orElse(null));
    transactionBuilder.index(recordItem.getTransactionIndex());
    transactionBuilder.initial_balance(body.getCryptoCreateAccount().getInitialBalance());
    transactionBuilder.max_fee(body.getTransactionFee());
    transactionBuilder.memo_base64(
        Base64.getEncoder().encodeToString(DomainUtils.toBytes(body.getMemoBytes())));
    transactionBuilder.node(EntityId.of(body.getNodeAccountID()).toString());
    transactionBuilder.nonce(transactionId.getNonce());
    transactionBuilder.payer_account_id(
        com.hedera.etl.entity.EntityId.from(recordItem.getPayerAccountId()).toString());
    transactionBuilder.resultCode(txRecord.getReceipt().getStatusValue());
    transactionBuilder.result(txRecord.getReceipt().getStatus().toString());
    transactionBuilder.scheduled(txRecord.hasScheduleRef());
    transactionBuilder.bytes(DomainUtils.toBase64(recordItem.getTransaction().toByteArray()));
    transactionBuilder.transaction_hash(DomainUtils.toBase64(txRecord.getTransactionHash()));
    transactionBuilder.type(recordItem.getTransactionType());
    transactionBuilder.name(TransactionType.of(recordItem.getTransactionType()).toString());
    transactionBuilder.valid_duration_seconds(validDurationSeconds);
    transactionBuilder.valid_start_timestamp(
        DomainUtils.timeStampInNanos(transactionId.getTransactionValidStart()));
    transactionBuilder.block_timestamp_to(recordItem.getFileConsensusEnd());
    transactionBuilder.staking_reward_transfers(insertStakingRewardTransfers(recordItem));
    transactionBuilder.max_custom_fees(getMaxCustomFees(body));
    //    transactionBuilder.schedule_ref(
    //            recordItem.getTransactionRecord().hasScheduleRef()
    //                    ?
    // com.hedera.etl.entity.EntityId.from(EntityId.of(txRecord.getScheduleRef()))
    //                    : null);

    // TODO handle this method?
    // transactionHandler.updateTransaction(transaction, recordItem);

    transactionBuilder.transfers(insertTransferList(recordItem));

    if (txRecord.hasParentConsensusTimestamp()) {
      transactionBuilder.parent_consensus_timestamp(
          DomainUtils.timestampInNanosMax(txRecord.getParentConsensusTimestamp()));
    }

    if (body.hasBatchKey()) {
      transactionBuilder.batch_key(Key.from(body.getBatchKey()));
    }

    Transaction transaction = transactionBuilder.build();

    // Errata records can fail with FAIL_INVALID but still have items in the record committed to
    // state.
    if (recordItem.isSuccessful()
        || recordItem.getTransactionStatus() == ResponseCodeEnum.FAIL_INVALID_VALUE) {
      // Record token transfers can be populated for multiple transaction types
      insertTokenTransfers(recordItem, transaction);
      transaction.setAssessed_custom_fees(insertAssessedCustomFees(recordItem));
    }

    return transaction;
  }

  private static List<StakingRewardTransfer> insertStakingRewardTransfers(RecordItem recordItem) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var payerAccountId = recordItem.getPayerAccountId();

    List<StakingRewardTransfer> stakingRewardTransfers = new ArrayList<>();

    for (var aa : recordItem.getTransactionRecord().getPaidStakingRewardsList()) {
      var accountId = EntityId.of(aa.getAccountID());
      var stakingRewardTransfer = new StakingRewardTransfer();
      stakingRewardTransfer.setAccount_id(accountId.toString());
      stakingRewardTransfer.setAmount(aa.getAmount());
      stakingRewardTransfer.setConsensus_timestamp(consensusTimestamp);
      stakingRewardTransfer.setPayer_account_id(payerAccountId.toString());
      stakingRewardTransfers.add(stakingRewardTransfer);
    }

    return stakingRewardTransfers;
  }

  private static List<TransactionTransfersInner> insertTransferList(RecordItem recordItem) {
    var transactionRecord = recordItem.getTransactionRecord();

    List<TransactionTransfersInner> transactionTransfersInners = new ArrayList<>();

    if (!transactionRecord.hasTransferList()) {
      return transactionTransfersInners;
    }

    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var transferList = transactionRecord.getTransferList();
    EntityId payerAccountId = recordItem.getPayerAccountId();
    var body = recordItem.getTransactionBody();
    boolean failedTransfer =
        !recordItem.isSuccessful()
            && body.hasCryptoTransfer()
            && consensusTimestamp < 1577836799000000000L;

    var spuriousErrataSet = false;

    for (int i = 0; i < transferList.getAccountAmountsCount(); ++i) {
      var aa = transferList.getAccountAmounts(i);
      var account = EntityId.of(aa.getAccountID());
      CryptoTransfer cryptoTransfer = new CryptoTransfer();
      cryptoTransfer.setAmount(aa.getAmount());
      cryptoTransfer.setConsensusTimestamp(consensusTimestamp);
      cryptoTransfer.setEntityId(account.getId());
      cryptoTransfer.setIsApproval(false);
      cryptoTransfer.setPayerAccountId(com.hedera.etl.entity.EntityId.from(payerAccountId));

      AccountAmount accountAmountInsideBody = null;
      if (cryptoTransfer.getAmount() < 0 || failedTransfer) {
        accountAmountInsideBody = findAccountAmount(aa, body);
      }

      if (accountAmountInsideBody != null) {
        cryptoTransfer.setIsApproval(accountAmountInsideBody.getIsApproval());
        if (failedTransfer) {
          cryptoTransfer.setErrata(ErrataType.DELETE);
        }
      }

      // spurious transfer removal
      if (failedTransfer
          && cryptoTransfer.getEntityId() != 98
          && cryptoTransfer.getAmount() > 0
          && (cryptoTransfer.getEntityId() < 3 || cryptoTransfer.getEntityId() > 27)
          && (cryptoTransfer.getEntityId() != payerAccountId.getId()
              || ((consensusTimestamp == 1570118944399195000L
                      || consensusTimestamp == 1570120372315307000L)
                  && cryptoTransfer.getEntityId() == payerAccountId.getId()))) {
        cryptoTransfer.setErrata(ErrataType.DELETE);
        spuriousErrataSet = true;
      }

      TransactionTransfersInner transactionTransfersInner = new TransactionTransfersInner();
      transactionTransfersInner.setAmount(cryptoTransfer.getAmount());
      transactionTransfersInner.setAccount(EntityId.of(cryptoTransfer.getEntityId()).toString());
      transactionTransfersInner.setIs_approval(cryptoTransfer.getIsApproval());
      transactionTransfersInner.setErrata(cryptoTransfer.getErrata());

      transactionTransfersInners.add(transactionTransfersInner);
    }

    if (spuriousErrataSet) {
      var erratas =
          transactionTransfersInners.stream()
              .filter(transfer -> ErrataType.DELETE.equals(transfer.getErrata()))
              .toList();
      for (var errata : erratas) {
        transactionTransfersInners.stream()
            .filter(transfer -> Objects.equals(transfer.getAmount(), errata.getAmount() * -1L))
            .forEach(transfer -> transfer.setErrata(ErrataType.DELETE));
      }
    }

    return transactionTransfersInners;
  }

  private static void insertNonFungibleTokenTransfers(
      RecordItem recordItem, Transaction transaction, TokenTransferList tokenTransferList) {
    if (tokenTransferList.getNftTransfersList().isEmpty()) {
      return;
    }

    var body = recordItem.getTransactionBody();
    var tokenId = tokenTransferList.getToken();
    var entityTokenId = EntityId.of(tokenId);

    for (var nftTransfer : tokenTransferList.getNftTransfersList()) {
      long serialNumber = nftTransfer.getSerialNumber();
      var receiverId = EntityId.of(nftTransfer.getReceiverAccountID());
      var senderId = EntityId.of(nftTransfer.getSenderAccountID());

      var nftTransferDomain = new NftTransfer();
      nftTransferDomain.setIs_approval(isApprovalNftTransfer(nftTransfer, tokenId, body));
      nftTransferDomain.setReceiver_account_id(receiverId.toString());
      nftTransferDomain.setSender_account_id(
          com.hedera.etl.entity.EntityId.from(senderId).toString());
      nftTransferDomain.setSerial_number(serialNumber);
      nftTransferDomain.setToken_id(entityTokenId.toString());
      transaction.addNftTransfer(nftTransferDomain);
    }
  }

  public void addNftTransfer(@NonNull NftTransfer nftTransfer) {
    if (this.nft_transfers == null) {
      this.nft_transfers = new ArrayList<>();
    }

    this.nft_transfers.add(nftTransfer);
  }

  private static void insertTokenTransfers(RecordItem recordItem, Transaction transaction) {

    var tokenTransferListsList = recordItem.getTransactionRecord().getTokenTransferListsList();

    for (int i = 0; i < tokenTransferListsList.size(); i++) {
      TokenTransferList tokenTransferList = tokenTransferListsList.get(i);

      transaction.setToken_transfers(insertFungibleTokenTransfers(recordItem, tokenTransferList));
      insertNonFungibleTokenTransfers(recordItem, transaction, tokenTransferList);
    }
  }

  private static List<AssessedCustomFee> insertAssessedCustomFees(RecordItem recordItem) {

    List<AssessedCustomFee> assessedCustomFees = new ArrayList<>();

    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var assessedCustomFeesList = recordItem.getTransactionRecord().getAssessedCustomFeesList();
    for (int i = 0; i < assessedCustomFeesList.size(); i++) {
      var protoAssessedCustomFee = assessedCustomFeesList.get(i);
      var collectorAccountId = EntityId.of(protoAssessedCustomFee.getFeeCollectorAccountId());
      // the effective payers must also appear in the *transfer lists of this transaction and the
      // corresponding EntityIds should have been added to EntityListener, so skip it here.
      var tokenId = EntityId.of(protoAssessedCustomFee.getTokenId());
      var assessedCustomFee = new AssessedCustomFee();
      assessedCustomFee.setAmount(protoAssessedCustomFee.getAmount());
      assessedCustomFee.setCollector_account_id(collectorAccountId.getId());
      assessedCustomFee.setConsensus_timestamp(consensusTimestamp);
      assessedCustomFee.setPayer_account_id(recordItem.getPayerAccountId().toString());
      assessedCustomFee.setToken_id(tokenId.toString());

      if (protoAssessedCustomFee.getEffectivePayerAccountIdCount() > 0) {
        var effectivePayerEntityIds = new ArrayList<Long>();
        for (var protoAccountId : protoAssessedCustomFee.getEffectivePayerAccountIdList()) {
          var effectivePayerAccountId = EntityId.of(protoAccountId);
          effectivePayerEntityIds.add(effectivePayerAccountId.getId());
          recordItem.addEntityId(effectivePayerAccountId);
        }
        assessedCustomFee.setEffective_payer_account_ids(effectivePayerEntityIds);
      }
      assessedCustomFees.add(assessedCustomFee);
    }
    return assessedCustomFees;
  }

  private static boolean isApprovalNftTransfer(
      com.hederahashgraph.api.proto.java.NftTransfer nftTransfer,
      TokenID tokenId,
      TransactionBody body) {
    if (!body.hasCryptoTransfer()) {
      return false;
    }

    var tokenTransfersList = body.getCryptoTransfer().getTokenTransfersList();
    for (var transferList : tokenTransfersList) {
      if (!transferList.getToken().equals(tokenId)) {
        continue;
      }

      for (var transfer : transferList.getNftTransfersList()) {
        if (transfer.getSerialNumber() == nftTransfer.getSerialNumber()
            && transfer.getReceiverAccountID().equals(nftTransfer.getReceiverAccountID())
            && transfer.getSenderAccountID().equals(nftTransfer.getSenderAccountID())) {
          return transfer.getIsApproval();
        }
      }
    }

    return false;
  }

  private static AccountAmount findAccountAmount(AccountAmount aa, TransactionBody body) {
    if (!body.hasCryptoTransfer()) {
      return null;
    }
    List<AccountAmount> accountAmountsList =
        body.getCryptoTransfer().getTransfers().getAccountAmountsList();
    for (AccountAmount a : accountAmountsList) {
      if (aa.getAmount() == a.getAmount() && aa.getAccountID().equals(a.getAccountID())) {
        return a;
      }
    }
    return null;
  }

  private static AccountAmount findAccountAmount(
      Predicate<AccountAmount> accountAmountPredicate, EntityId tokenId, TransactionBody body) {
    if (!body.hasCryptoTransfer()) {
      return null;
    }
    List<TokenTransferList> tokenTransfersLists = body.getCryptoTransfer().getTokenTransfersList();
    for (TokenTransferList transferList : tokenTransfersLists) {
      if (!EntityId.of(transferList.getToken()).equals(tokenId)) {
        continue;
      }
      for (AccountAmount aa : transferList.getTransfersList()) {
        if (accountAmountPredicate.test(aa)) {
          return aa;
        }
      }
    }
    return null;
  }

  private static List<TransactionTokenTransfersInner> insertFungibleTokenTransfers(
      RecordItem recordItem, TokenTransferList tokenTransferList) {

    List<TransactionTokenTransfersInner> transactionTokenTransfersInners = new ArrayList<>();

    if (tokenTransferList.getTransfersList().isEmpty()) {
      return transactionTokenTransfersInners;
    }

    var body = recordItem.getTransactionBody();
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    boolean isTokenDissociate = body.hasTokenDissociate();
    var payerAccountId = recordItem.getPayerAccountId();
    var tokenId = EntityId.of(tokenTransferList.getToken());
    var tokenTransfers = tokenTransferList.getTransfersList();
    int tokenTransferCount = tokenTransfers.size();

    boolean isDeletedTokenDissociate = isTokenDissociate && tokenTransferCount == 1;

    for (int i = 0; i < tokenTransferCount; i++) {
      AccountAmount accountAmount = tokenTransfers.get(i);
      EntityId accountId = EntityId.of(accountAmount.getAccountID());
      long amount = accountAmount.getAmount();
      var tokenTransfer =
          isDeletedTokenDissociate ? new DissociateTokenTransfer() : new TokenTransfer();
      tokenTransfer.setAmount(amount);
      tokenTransfer.setId(
          new TokenTransfer.Id(consensusTimestamp, tokenId.toString(), accountId.toString()));
      tokenTransfer.setIS_APPROVAL(false);
      tokenTransfer.setPAYER_ACCOUNT_ID(payerAccountId.toString());

      handleNegativeAccountAmounts(tokenId, body, accountAmount, amount, tokenTransfer);

      TransactionTokenTransfersInner transactionTokenTransfersInner =
          new TransactionTokenTransfersInner();

      transactionTokenTransfersInner.setToken_id(tokenId.toString());
      transactionTokenTransfersInner.setAccount(accountId.toString());
      transactionTokenTransfersInner.setIs_approval(tokenTransfer.getIS_APPROVAL());
      transactionTokenTransfersInner.setAmount(tokenTransfer.getAmount());

      transactionTokenTransfersInners.add(transactionTokenTransfersInner);
    }
    return transactionTokenTransfersInners;
  }

  private static void handleNegativeAccountAmounts(
      EntityId tokenId,
      TransactionBody body,
      AccountAmount accountAmount,
      long amount,
      TokenTransfer tokenTransfer) {
    // If a record AccountAmount with amount < 0 is not in the body;
    // but an AccountAmount with the same (TokenID, AccountID) combination is in the body with
    // is_approval=true,
    // then again set is_approval=true
    if (amount < 0) {

      // Is the accountAmount from the record also inside a body's transfer list for the given
      // tokenId?
      AccountAmount accountAmountInsideTransferList =
          findAccountAmount(accountAmount::equals, tokenId, body);
      if (accountAmountInsideTransferList == null) {

        // Is there any account amount inside the body's transfer list for the given tokenId
        // with the same accountId as the accountAmount from the record?
        AccountAmount accountAmountWithSameIdInsideBody =
            findAccountAmount(
                aa -> aa.getAccountID().equals(accountAmount.getAccountID()) && aa.getIsApproval(),
                tokenId,
                body);
        if (accountAmountWithSameIdInsideBody != null) {
          tokenTransfer.setIS_APPROVAL(true);
        }
      } else {
        tokenTransfer.setIS_APPROVAL(accountAmountInsideTransferList.getIsApproval());
      }
    }
  }

  private static List<MaxCustomFee> getMaxCustomFees(TransactionBody body) {
    int count = body.getMaxCustomFeesCount();
    if (count == 0) {
      return null;
    }
    return body.getMaxCustomFeesList().stream()
        .flatMap(limit -> MaxCustomFee.from(limit).stream())
        .toList();
  }
}
