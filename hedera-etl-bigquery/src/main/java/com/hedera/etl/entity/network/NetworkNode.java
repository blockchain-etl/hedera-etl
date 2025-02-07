package com.hedera.etl.entity.network;

import java.util.List;

import javax.annotation.Nullable;

import com.hederahashgraph.api.proto.java.NodeStakeUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.NodeUpdateTransactionBody;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.entity.token.Key;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;
import com.hedera.etl.util.TimeUtils;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NetworkNode {

  @Nullable private Key admin_key;

  @Nullable private String description;
  @Nullable private String memo;
  @Nullable private String file_id;
  @Nullable private Long max_stake;
  @Nullable private Long min_stake;
  @Nullable private String node_account_id;
  @Nullable private String node_id;
  @Nullable private String node_cert_hash;
  @Nullable private Key public_key;
  @Nullable private Long reward_rate_start;
  @Nullable private List<Endpoint> service_endpoints;
  @Nullable private Long stake;
  @Nullable private Long stake_rewarded;
  @Nullable private Long stake_not_rewarded;
  @Nullable private Long staking_period;
  @Nullable private Long timestamp;

  @Nullable private String created;

  @DefaultSchema(JavaBeanSchema.class)
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  public static class Endpoint {
    @Nullable private String domain_name;
    @Nullable private String ip_address_v4;
    @Nullable private Integer port;
  }

  public static Iterable<NetworkNode> from(RecordItem recordItem) {
    if (recordItem.isSuccessful()) {
      if (TransactionType.NODESTAKEUPDATE.getProtoId() == recordItem.getTransactionType()) {
        return from(recordItem, recordItem.getTransactionBody().getNodeStakeUpdate());
      } else if (TransactionType.NODEUPDATE.getProtoId() == recordItem.getTransactionType()) {
        return from(recordItem, recordItem.getTransactionBody().getNodeUpdate());
      }
    }

    return List.of();
  }

  public static Iterable<NetworkNode> from(
      RecordItem recordItem, NodeStakeUpdateTransactionBody txBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    long stakingPeriod = DomainUtils.timestampInNanosMax(txBody.getEndOfStakingPeriod());

    return txBody.getNodeStakeList().stream()
        .map(
            nodeStake ->
                builder()
                    .created(TimeUtils.fromNanos(consensusTimestamp))
                    .max_stake(nodeStake.getMaxStake())
                    .min_stake(nodeStake.getMinStake())
                    .description(null)
                    .memo(recordItem.getTransactionBody().getMemo())
                    .file_id(
                        EntityId.of(recordItem.getTransactionRecord().getReceipt().getFileID())
                            .toString())
                    .node_account_id(null)
                    .node_id(EntityId.of(nodeStake.getNodeId()).toString())
                    .node_cert_hash(null)
                    .public_key(null)
                    .reward_rate_start(null)
                    .service_endpoints(List.of())
                    .stake(nodeStake.getStake())
                    .stake(nodeStake.getStakeRewarded())
                    .stake_not_rewarded(nodeStake.getStakeNotRewarded())
                    .staking_period(stakingPeriod)
                    .timestamp(consensusTimestamp)
                    .build())
        .toList();
  }

  public static Iterable<NetworkNode> from(
      RecordItem recordItem, NodeUpdateTransactionBody txBody) {
    long consensusTimestamp = recordItem.getConsensusTimestamp();

    return List.of(
        builder()
            .created(TimeUtils.fromNanos(consensusTimestamp))
            .max_stake(null)
            .min_stake(null)
            .description(txBody.getDescription().getValue())
            .memo(recordItem.getTransactionBody().getMemo())
            .file_id(
                EntityId.of(recordItem.getTransactionRecord().getReceipt().getFileID()).toString())
            .node_account_id(EntityId.of(txBody.getAccountId()).toString())
            .node_id(EntityId.of(txBody.getNodeId()).toString())
            .node_cert_hash(
                DomainUtils.bytesToHex(txBody.getGrpcCertificateHash().getValue().toByteArray()))
            .public_key(Key.from(txBody.getAdminKey()))
            .reward_rate_start(null)
            .service_endpoints(
                txBody.getServiceEndpointList().stream()
                    .map(
                        endpoint ->
                            Endpoint.builder()
                                .domain_name(endpoint.getDomainName())
                                .ip_address_v4(endpoint.getIpAddressV4().toString())
                                .port(endpoint.getPort())
                                .build())
                    .toList())
            .stake(null)
            .stake_not_rewarded(null)
            .staking_period(null)
            .timestamp(consensusTimestamp)
            .build());
  }
}
