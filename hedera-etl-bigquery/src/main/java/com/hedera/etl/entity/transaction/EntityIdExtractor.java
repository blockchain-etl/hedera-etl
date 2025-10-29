package com.hedera.etl.entity.transaction;

import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import com.hedera.etl.entity.Lookup;
import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;

@Log4j2
public class EntityIdExtractor {
  public static Optional<EntityId> getEntity(RecordItem recordItem) {
    return Optional.of(
            switch (TransactionType.of(recordItem.getTransactionType())) {
              case TransactionType.CONSENSUSCREATETOPIC ->
                  EntityId.of(recordItem.getTransactionRecord().getReceipt().getTopicID());
              case TransactionType.CONSENSUSDELETETOPIC ->
                  EntityId.of(
                      recordItem.getTransactionBody().getConsensusDeleteTopic().getTopicID());
              case TransactionType.CONSENSUSSUBMITMESSAGE ->
                  EntityId.of(
                      recordItem.getTransactionBody().getConsensusSubmitMessage().getTopicID());
              case TransactionType.CONSENSUSUPDATETOPIC ->
                  EntityId.of(
                      recordItem.getTransactionBody().getConsensusUpdateTopic().getTopicID());
              case TransactionType.CONTRACTCALL -> {
                var contractIdBody =
                    recordItem.getTransactionBody().getContractCall().getContractID();
                var contractIdReceipt =
                    recordItem.getTransactionRecord().getReceipt().getContractID();
                yield Lookup.lookup(contractIdReceipt, contractIdBody).orElse(EntityId.EMPTY);
              }
              case TransactionType.CONTRACTCREATEINSTANCE ->
                  Lookup.lookup(recordItem.getTransactionRecord().getReceipt().getContractID())
                      .orElse(EntityId.EMPTY);
              case TransactionType.CONTRACTUPDATEINSTANCE ->
                  Lookup.lookup(
                          recordItem
                              .getTransactionBody()
                              .getContractUpdateInstance()
                              .getContractID())
                      .filter(id -> !EntityId.isEmpty(id))
                      .or(
                          () ->
                              Lookup.lookup(
                                  recordItem.getTransactionRecord().getReceipt().getContractID()))
                      .orElse(EntityId.EMPTY);
              case TransactionType.CONTRACTDELETEINSTANCE -> {
                var contractIdBody =
                    recordItem.getTransactionBody().getContractDeleteInstance().getContractID();
                var contractIdReceipt =
                    recordItem.getTransactionRecord().getReceipt().getContractID();
                yield Lookup.lookup(contractIdReceipt, contractIdBody).orElse(EntityId.EMPTY);
              }
              case TransactionType.CRYPTOADDLIVEHASH ->
                  EntityId.of(
                      recordItem
                          .getTransactionBody()
                          .getCryptoAddLiveHash()
                          .getLiveHash()
                          .getAccountId());
              case TransactionType.CRYPTOCREATEACCOUNT ->
                  EntityId.of(recordItem.getTransactionRecord().getReceipt().getAccountID());
              case TransactionType.CRYPTODELETE ->
                  EntityId.of(
                      recordItem.getTransactionBody().getCryptoDelete().getDeleteAccountID());
              case TransactionType.CRYPTODELETELIVEHASH ->
                  EntityId.of(
                      recordItem
                          .getTransactionBody()
                          .getCryptoDeleteLiveHash()
                          .getAccountOfLiveHash());
              case TransactionType.CRYPTOUPDATEACCOUNT ->
                  EntityId.of(
                      recordItem
                          .getTransactionBody()
                          .getCryptoUpdateAccount()
                          .getAccountIDToUpdate());
              case TransactionType.ETHEREUMTRANSACTION -> {
                var transactionRecord = recordItem.getTransactionRecord();

                // pull entity from ContractResult
                var contractFunctionResult =
                    transactionRecord.hasContractCreateResult()
                        ? transactionRecord.getContractCreateResult()
                        : transactionRecord.getContractCallResult();

                yield EntityId.of(contractFunctionResult.getContractID());
              }
              case TransactionType.FILEAPPEND ->
                  EntityId.of(recordItem.getTransactionBody().getFileAppend().getFileID());
              case TransactionType.FILECREATE ->
                  EntityId.of(recordItem.getTransactionRecord().getReceipt().getFileID());
              case TransactionType.FILEDELETE ->
                  EntityId.of(recordItem.getTransactionBody().getFileDelete().getFileID());
              case TransactionType.FILEUPDATE ->
                  EntityId.of(recordItem.getTransactionBody().getFileUpdate().getFileID());
              case TransactionType.FREEZE ->
                  EntityId.of(recordItem.getTransactionBody().getFreeze().getUpdateFile());
              case TransactionType.NODECREATE ->
                  EntityId.of(recordItem.getTransactionBody().getNodeCreate().getAccountId());
              case TransactionType.NODEUPDATE ->
                  EntityId.of(recordItem.getTransactionBody().getNodeUpdate().getAccountId());
              case TransactionType.SCHEDULECREATE ->
                  EntityId.of(recordItem.getTransactionRecord().getReceipt().getScheduleID());
              case TransactionType.SCHEDULEDELETE ->
                  EntityId.of(recordItem.getTransactionBody().getScheduleDelete().getScheduleID());
              case TransactionType.SCHEDULESIGN ->
                  EntityId.of(recordItem.getTransactionBody().getScheduleSign().getScheduleID());
              case TransactionType.SYSTEMDELETE -> {
                var systemDelete = recordItem.getTransactionBody().getSystemDelete();
                if (systemDelete.hasContractID()) {
                  yield Lookup.lookup(systemDelete.getContractID()).orElse(EntityId.EMPTY);
                } else if (systemDelete.hasFileID()) {
                  yield EntityId.of(systemDelete.getFileID());
                }
                yield EntityId.EMPTY;
              }
              case TransactionType.SYSTEMUNDELETE -> {
                var systemUndelete = recordItem.getTransactionBody().getSystemUndelete();
                if (systemUndelete.hasContractID()) {
                  yield Lookup.lookup(systemUndelete.getContractID()).orElse(EntityId.EMPTY);
                } else if (systemUndelete.hasFileID()) {
                  yield EntityId.of(systemUndelete.getFileID());
                }

                yield EntityId.EMPTY;
              }
              case TransactionType.TOKENASSOCIATE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenAssociate().getAccount());
              case TransactionType.TOKENBURN ->
                  EntityId.of(recordItem.getTransactionBody().getTokenBurn().getToken());
              case TransactionType.TOKENCREATION ->
                  EntityId.of(recordItem.getTransactionRecord().getReceipt().getTokenID());
              case TransactionType.TOKENDELETION ->
                  EntityId.of(recordItem.getTransactionBody().getTokenDeletion().getToken());
              case TransactionType.TOKENDISSOCIATE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenDissociate().getAccount());
              case TransactionType.TOKENFEESCHEDULEUPDATE ->
                  EntityId.of(
                      recordItem.getTransactionBody().getTokenFeeScheduleUpdate().getTokenId());
              case TransactionType.TOKENFREEZE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenFreeze().getAccount());
              case TransactionType.TOKENGRANTKYC ->
                  EntityId.of(recordItem.getTransactionBody().getTokenGrantKyc().getAccount());
              case TransactionType.TOKENMINT ->
                  EntityId.of(recordItem.getTransactionBody().getTokenMint().getToken());
              case TransactionType.TOKENPAUSE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenPause().getToken());
              case TransactionType.TOKENREJECT -> {
                var tokenReject = recordItem.getTransactionBody().getTokenReject();
                yield tokenReject.hasOwner()
                    ? Lookup.lookup(tokenReject.getOwner()).orElse(EntityId.EMPTY)
                    : recordItem.getPayerAccountId();
              }
              case TransactionType.TOKENREVOKEKYC ->
                  EntityId.of(recordItem.getTransactionBody().getTokenRevokeKyc().getAccount());
              case TransactionType.TOKENUNFREEZE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenUnfreeze().getAccount());
              case TransactionType.TOKENUNPAUSE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenUnpause().getToken());
              case TransactionType.TOKENUPDATENFTS ->
                  EntityId.of(recordItem.getTransactionBody().getTokenUpdateNfts().getToken());
              case TransactionType.TOKENUPDATE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenUpdate().getToken());
              case TransactionType.TOKENWIPE ->
                  EntityId.of(recordItem.getTransactionBody().getTokenWipe().getToken());

              default -> EntityId.EMPTY;
            })
        .filter(id -> !id.equals(EntityId.EMPTY));
  }
}
