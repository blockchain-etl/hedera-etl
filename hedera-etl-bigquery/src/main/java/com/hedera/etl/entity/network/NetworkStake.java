package com.hedera.etl.entity.network;

import com.hedera.etl.entity.FractionalAmount;
import com.hedera.etl.entity.TimestampRange;

import com.hedera.etl.entity.TransactionType;
import com.hedera.etl.recordfile.domain.transaction.RecordItem;

import com.hedera.etl.recordfile.utils.DomainUtils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;
import java.util.Objects;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NetworkStake {
  @Nullable
  private Long consensus_timestamp;
  @Nullable
  private Long max_stake_rewarded;
  @Nullable
  private Long max_staking_reward_rate_per_hbar;
  @Nullable
  private Long max_total_reward;
  @Nullable
  private FractionalAmount node_reward_fee_fraction;
  @Nullable
  private Long reserved_staking_rewards;
  @Nullable
  private Long reward_balance_threshold;
  @Nullable
  private Long stake_total;
  @Nullable
  private Long staking_period;
  @Nullable
  private Long staking_period_duration;
  @Nullable
  private Long staking_periods_stored;
  @Nullable
  private FractionalAmount staking_reward_fee_fraction;
  @Nullable
  private Long staking_reward_rate;
  @Nullable
  private Long staking_start_threshold;
  @Nullable
  private Long unreserved_staking_reward_balance;

  public static NetworkStake from(RecordItem recordItem) {
    if (TransactionType.NODESTAKEUPDATE.getProtoId() != recordItem.getTransactionType() || !recordItem.isSuccessful()) {
      return null;
    }
    long consensusTimestamp = recordItem.getConsensusTimestamp();

    var transactionBody = recordItem.getTransactionBody().getNodeStakeUpdate();
    long stakingPeriod = DomainUtils.timestampInNanosMax(transactionBody.getEndOfStakingPeriod());
    long stakeTotal = transactionBody.getNodeStakeList().stream()
            .map(nodeStake -> nodeStake.getStakeRewarded() + nodeStake.getStakeNotRewarded())
            .reduce(0L, Long::sum);

    return builder()
            .consensus_timestamp(consensusTimestamp)
            .max_stake_rewarded(transactionBody.getMaxStakeRewarded())
            .max_staking_reward_rate_per_hbar(transactionBody.getMaxStakingRewardRatePerHbar())
            .max_total_reward(transactionBody.getMaxTotalReward())
            .node_reward_fee_fraction(FractionalAmount.from(transactionBody.getNodeRewardFeeFraction()))
            .reserved_staking_rewards(transactionBody.getReservedStakingRewards())
            .reward_balance_threshold(transactionBody.getRewardBalanceThreshold())
            .stake_total(stakeTotal)
            .staking_period(stakingPeriod)
            .staking_period_duration(transactionBody.getStakingPeriod())
            .staking_periods_stored(transactionBody.getStakingPeriodsStored())
            .staking_start_threshold(transactionBody.getStakingStartThreshold())
            .staking_reward_fee_fraction(FractionalAmount.from(transactionBody.getStakingRewardFeeFraction()))
            .staking_reward_rate(transactionBody.getStakingRewardRate())
            .unreserved_staking_reward_balance(transactionBody.getUnreservedStakingRewardBalance())
            .build();
  }
}
