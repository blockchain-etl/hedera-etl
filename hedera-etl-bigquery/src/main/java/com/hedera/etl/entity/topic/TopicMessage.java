package com.hedera.etl.entity.topic;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.ConsensusMessageChunkInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class TopicMessage {
  @Nullable private ChunkInfo chunk_info;
  @Nullable private Long consensus_timestamp;
  @Nullable private String message;
  @Nullable private String payer_account_id;
  @Nullable private String running_hash;
  @Nullable private Integer running_hash_version;
  @Nullable private Long sequence_number;
  @Nullable private String topic_id;

  @Nullable private String created;

  public static TopicMessage from(RecordItem recordItem) {

    int transactionTypeValue = recordItem.getTransactionType();
    TransactionType transactionType = TransactionType.of(transactionTypeValue);

    if (transactionType != TransactionType.CONSENSUSSUBMITMESSAGE) {
      return null;
    }

    var transactionBody = recordItem.getTransactionBody().getConsensusSubmitMessage();
    var transactionRecord = recordItem.getTransactionRecord();
    var receipt = transactionRecord.getReceipt();
    int runningHashVersion =
        receipt.getTopicRunningHashVersion() == 0 ? 1 : (int) receipt.getTopicRunningHashVersion();

    TopicMessageBuilder topicBuilder = TopicMessage.builder();
    ChunkInfo.ChunkInfoBuilder chunkInfoBuilder = ChunkInfo.builder();

    if (transactionBody.hasChunkInfo()) {
      ConsensusMessageChunkInfo chunkInfo = transactionBody.getChunkInfo();
      chunkInfoBuilder.number(chunkInfo.getNumber());
      chunkInfoBuilder.total(chunkInfo.getTotal());
      if (chunkInfo.hasInitialTransactionID()) {
        var id = chunkInfo.getInitialTransactionID();

        chunkInfoBuilder.initial_transaction_id(
            ChunkInfo.InitialTransactionId.builder()
                .account_id(EntityId.of(id.getAccountID()).toString())
                .nonce(id.getNonce())
                .scheduled(id.getScheduled())
                .transaction_valid_start(
                    id.getTransactionValidStart().getSeconds() * 1_000_000_000L
                        + id.getTransactionValidStart().getNanos())
                .build());
      }
    }

    return topicBuilder
        .chunk_info(chunkInfoBuilder.build())
        .created(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .consensus_timestamp(recordItem.getConsensusTimestamp())
        .message(DomainUtils.toBase64(transactionBody.getMessage()))
        .payer_account_id(recordItem.getPayerAccountId().toString())
        .running_hash(DomainUtils.toBase64(receipt.getTopicRunningHash()))
        .running_hash_version(runningHashVersion)
        .sequence_number(receipt.getTopicSequenceNumber())
        .topic_id(
            EntityId.of(recordItem.getTransactionBody().getConsensusSubmitMessage().getTopicID())
                .toString())
        .build();
  }
}
