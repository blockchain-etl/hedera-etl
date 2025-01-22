package com.hedera.etl.entity.topic;

import com.hedera.etl.recordfile.domain.transaction.RecordItem;

import com.hedera.etl.recordfile.entity.EntityId;

import com.hederahashgraph.api.proto.java.ConsensusMessageChunkInfo;
import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;

import static com.hedera.etl.recordfile.utils.DomainUtils.toBytes;

@DefaultSchema(JavaBeanSchema.class)
@Data
@Builder
public class TopicMessage {

  @Nullable
  private ChunkInfo chunk_info;

  @Nullable
  private Long consensus_timestamp;

  @Nullable
  private byte[] message;

  @Nullable
  private String payer_account_id;

  @Nullable
  private byte[] running_hash;

  @Nullable
  private Integer running_hash_version;

  @Nullable
  private Long sequence_number;

  @Nullable
  private String topic_id;

  public static TopicMessage from(RecordItem recordItem) {


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
        chunkInfoBuilder.initial_transaction_id(chunkInfo.getInitialTransactionID().toByteArray());
      }
    }

    return topicBuilder.chunk_info(chunkInfoBuilder.build())
            .consensus_timestamp(recordItem.getConsensusTimestamp())
            .message(toBytes(transactionBody.getMessage()))
            .payer_account_id(recordItem.getPayerAccountId().toString())
            .running_hash(toBytes(receipt.getTopicRunningHash()))
            .running_hash_version(runningHashVersion)
            .sequence_number(receipt.getTopicSequenceNumber())
            .topic_id(EntityId.of(recordItem.getTransactionBody()
                    .getConsensusSubmitMessage()
                    .getTopicID()
                    ).toString())
            .build();
  }
}
