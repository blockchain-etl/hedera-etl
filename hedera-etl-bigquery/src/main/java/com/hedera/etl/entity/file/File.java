package com.hedera.etl.entity.file;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.FileAppendTransactionBody;
import com.hederahashgraph.api.proto.java.FileCreateTransactionBody;
import com.hederahashgraph.api.proto.java.FileDeleteTransactionBody;
import com.hederahashgraph.api.proto.java.FileUpdateTransactionBody;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;

import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@Data
@NoArgsConstructor
@AllArgsConstructor(onConstructor_ = @SchemaCreate)
@Builder
public class File {
  @Nullable private String fileId;
  @Nullable private byte[] content;
  @Nullable private Long expirationTimestamp;
  @Nullable private String created;
  @Nullable private Long createdTimestamp;
  @Nullable private String modified;
  @Nullable private Long modifiedTimestamp;
  @Nullable private Boolean deleted;
  @Nullable private String action;

  public static File from(RecordItem recordItem) {
    if (recordItem.getTransactionBody().hasFileCreate()) {
      return from(recordItem, recordItem.getTransactionBody().getFileCreate());
    }

    if (recordItem.getTransactionBody().hasFileAppend()) {
      return from(recordItem, recordItem.getTransactionBody().getFileAppend());
    }

    if (recordItem.getTransactionBody().hasFileUpdate()) {
      return from(recordItem, recordItem.getTransactionBody().getFileUpdate());
    }

    if (recordItem.getTransactionBody().hasFileDelete()) {
      return from(recordItem, recordItem.getTransactionBody().getFileDelete());
    }

    return null;
  }

  private static File from(RecordItem recordItem, FileUpdateTransactionBody fileUpdate) {
    return builder()
        .fileId(EntityId.of(fileUpdate.getFileID()).toString())
        .content(fileUpdate.getContents().toByteArray())
        .modified(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .modifiedTimestamp(recordItem.getConsensusTimestamp())
        .action("update")
        .build();
  }

  private static File from(RecordItem recordItem, FileCreateTransactionBody fileCreate) {
    return builder()
        .fileId(EntityId.of(recordItem.getTransactionRecord().getReceipt().getFileID()).toString())
        .content(fileCreate.getContents().toByteArray())
        .created(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .createdTimestamp(recordItem.getConsensusTimestamp())
        .modified(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .modifiedTimestamp(recordItem.getConsensusTimestamp())
        .expirationTimestamp(DomainUtils.timeStampInNanos(fileCreate.getExpirationTime()))
        .deleted(false)
        .action("create")
        .build();
  }

  private static File from(RecordItem recordItem, FileAppendTransactionBody fileAppend) {
    return builder()
        .fileId(EntityId.of(fileAppend.getFileID()).toString())
        .content(fileAppend.getContents().toByteArray())
        .modified(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .modifiedTimestamp(recordItem.getConsensusTimestamp())
        .action("append")
        .build();
  }

  private static File from(RecordItem recordItem, FileDeleteTransactionBody fileDelete) {
    return builder()
        .fileId(EntityId.of(fileDelete.getFileID()).toString())
        .modified(FormatUtils.timestampFromNanos(recordItem.getConsensusTimestamp()))
        .modifiedTimestamp(recordItem.getConsensusTimestamp())
        .action("delete")
        .deleted(true)
        .build();
  }
}
