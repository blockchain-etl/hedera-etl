package com.hedera.etl.reader.recordfile.domain.transaction;

import java.io.Serializable;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.FileID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParentRecordItemProjection implements Serializable {
  private boolean successful;
  private ParentRecordItemProjection parent;
  private int transactionType;
  private Long consensusTimestamp;
  @Nullable private FileID bodyCallData;
  @Nullable private byte[] ethereumCallData;
  @Nullable private ContractCreateTransactionBody.InitcodeSourceCase sourceCase;
  @Nullable private FileID fileID;
  @Nullable private ByteString initcode;

  public static ParentRecordItemProjection fromRecordItem(RecordItem item) {
    var projectionBuilder =
        builder()
            .parent(item.getParent())
            .successful(item.isSuccessful())
            .transactionType(item.getTransactionType())
            .consensusTimestamp(item.getConsensusTimestamp());

    switch (item.getTransactionType()) {
      case 8 -> // CONTRACTCREATEINSTANCE ->
      {
        var transactionBody = item.getTransactionBody().getContractCreateInstance();
        projectionBuilder
            .fileID(transactionBody.getFileID())
            .initcode(transactionBody.getInitcode());
      }
      case 50 -> // ETHEREUMTRANSACTION ->
      {
        var body = item.getTransactionBody().getEthereumTransaction();

        // use callData FileID if present
        if (body.hasCallData()) {
          projectionBuilder.bodyCallData(body.getCallData());
        }

        if (item.getEthereumTransaction() != null) {
          projectionBuilder.ethereumCallData(item.getEthereumTransaction().getCallData());
        }
      }
      default -> {
        // no-op
      }
    }
    return projectionBuilder.build();
  }
}
