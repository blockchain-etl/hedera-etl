package com.hedera.etl.entity.openaccess;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;

import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.util.FormatUtils;

@DefaultSchema(JavaBeanSchema.class)
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
@Data
@NoArgsConstructor
@AllArgsConstructor(onConstructor_ = @SchemaCreate)
@Builder
public class NativeTokenBalance {
  private String timestamp;
  private String address;
  private Long amount;
  private String transactionHash;
  @Nullable private Long blockHeight;
  @Nullable private String blockHash;
  @Nullable private String chainSpecific;

  public static List<NativeTokenBalance> from(RecordFile file) {
    return file.getItems().stream()
        .flatMap(item -> from(item, file).stream())
        .collect(Collectors.toList());
  }

  private static List<NativeTokenBalance> from(RecordItem item, RecordFile file) {
    var transaction = Transaction.from(item);
    if (transaction == null) {
      return List.of();
    } else {
      return transaction.getTransfers().stream()
          .map(
              transfer ->
                  builder()
                      .timestamp(FormatUtils.timestampFromNanos(item.getConsensusTimestamp()))
                      .address(transfer.getAccount())
                      .transactionHash(transaction.getTransaction_hash())
                      .amount(transfer.getAmount())
                      .blockHeight(file.getIndex())
                      .blockHash(file.getHash())
                      .chainSpecific(null)
                      .build())
          .toList();
    }
  }

  public static List<NativeTokenBalance> from(RecordItem item) {
    var transaction = Transaction.from(item);
    if (transaction == null) {
      return List.of();
    } else {
      return transaction.getTransfers().stream()
          .map(
              transfer ->
                  builder()
                      .timestamp(FormatUtils.timestampFromNanos(item.getConsensusTimestamp()))
                      .address(transfer.getAccount())
                      .transactionHash(transaction.getTransaction_hash())
                      .amount(transfer.getAmount())
                      .blockHeight(null)
                      .blockHash(null)
                      .chainSpecific(null)
                      .build())
          .toList();
    }
  }
}
