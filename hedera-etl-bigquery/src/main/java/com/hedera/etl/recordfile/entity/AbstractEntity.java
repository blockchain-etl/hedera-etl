/*
 * Copyright (C) 2019-2024 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hedera.etl.recordfile.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Range;
import java.sql.Date;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import com.hedera.etl.recordfile.utils.DomainUtils;

@Data
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public abstract class AbstractEntity {

    public static final long ACCOUNT_ID_CLEARED = 0L;
    public static final long DEFAULT_EXPIRY_TIMESTAMP =
            TimeUnit.MILLISECONDS.toNanos(Date.valueOf("2100-1-1").getTime());
    public static final long NODE_ID_CLEARED = -1L;

    @ToString.Exclude
    private byte[] alias;

    private Long autoRenewAccountId;

    private Long autoRenewPeriod;

    private Long balance;

    private Long balanceTimestamp;

    private Long createdTimestamp;

    private Boolean declineReward;

    private Boolean deleted;

    private Long ethereumNonce;

    @ToString.Exclude
    private byte[] evmAddress;

    private Long expirationTimestamp;

    private Long id;

    @ToString.Exclude
    private byte[] key;

    private Integer maxAutomaticTokenAssociations;

    private String memo;

    private Long num;

    private EntityId obtainerId;

    private Boolean permanentRemoval;

    private EntityId proxyAccountId;

    private String publicKey;

    private Long realm;

    private Boolean receiverSigRequired;

    private Long shard;

    private Long stakedAccountId;

    private Long stakedNodeId;

    private Long stakePeriodStart;

    @ToString.Exclude
    private byte[] submitKey;

    private Range<Long> timestampRange;

    private EntityType type;

    public void addBalance(Long balance) {
        if (balance == null) {
            return;
        }

        if (this.balance == null) {
            this.balance = balance;
        } else {
            this.balance += balance;
        }
    }

    public void setKey(byte[] key) {
        this.key = key;
        publicKey = DomainUtils.getPublicKey(key);
    }

    public void setMemo(String memo) {
        this.memo = DomainUtils.sanitize(memo);
    }

    public EntityId toEntityId() {
        return EntityId.of(shard, realm, num);
    }

    @JsonIgnore
    public long getEffectiveExpiration() {
        if (expirationTimestamp != null) {
            return expirationTimestamp;
        }

        if (createdTimestamp != null && autoRenewPeriod != null) {
            return createdTimestamp + TimeUnit.SECONDS.toNanos(autoRenewPeriod);
        }

        return DEFAULT_EXPIRY_TIMESTAMP;
    }

    @SuppressWarnings("java:S1610")
    // Necessary since Lombok doesn't use our setters for builders
    public abstract static class AbstractEntityBuilder<
            C extends AbstractEntity, B extends AbstractEntityBuilder<C, B>> {
        public B key(byte[] key) {
            this.key = key;
            this.publicKey = DomainUtils.getPublicKey(key);
            return self();
        }

        public B memo(String memo) {
            this.memo = DomainUtils.sanitize(memo);
            return self();
        }
    }
}
