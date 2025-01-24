/*
 * Copyright (C) 2019-2025 Hedera Hashgraph, LLC
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
package com.hedera.etl.entity.schedule;

import com.hedera.etl.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.recordfile.entity.EntityId;

import com.hedera.etl.recordfile.utils.DomainUtils;

import com.hedera.etl.util.TimeUtils;
import com.hedera.shaded.hapi.com.google.protobuf.ByteString;

import com.hedera.shaded.hapi.com.google.protobuf.UnknownFieldSet;

import com.hederahashgraph.api.proto.java.SignaturePair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DefaultSchema(JavaBeanSchema.class)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Log4j2
public class Schedule {
  @Nullable
  private String created;
  @Nullable
  private Long consensus_timestamp;
  @Nullable
  private EntityId creator_account_id;
  @Nullable
  private Long executed_timestamp;
  @Nullable
  private Long expiration_time;
  @Nullable
  private EntityId payer_account_id;
  @Nullable
  private String schedule_id;
  @Nullable
  private byte[] transaction_body;
  @Nullable
  private boolean wait_for_expiry;
  @Nullable
  private List<ScheduleSignature> signatures;
  @Nullable
  private String memo;
  @Nullable
  private byte[] admin_key;
  @Nullable
  private Boolean deleted;

  public static Schedule from(RecordItem recordItem) {
    if (!recordItem.getTransactionBody().hasScheduleCreate()) {
      return null;
    }

    var body = recordItem.getTransactionBody().getScheduleCreate();
    long consensusTimestamp = recordItem.getConsensusTimestamp();
    var creatorAccount = recordItem.getPayerAccountId();
    var expirationTime =
            body.hasExpirationTime() ? DomainUtils.timestampInNanosMax(body.getExpirationTime()) : null;
    var payerAccount = body.hasPayerAccountID() ? EntityId.of(body.getPayerAccountID()) : creatorAccount;
    var scheduleId =
            EntityId.of(recordItem.getTransactionRecord().getReceipt().getScheduleID());
    if(recordItem.getTransactionBody().hasScheduleCreate()) {
      return Schedule.builder()
              .created(TimeUtils.fromNanos(consensusTimestamp))
              .consensus_timestamp(consensusTimestamp)
              .creator_account_id(creatorAccount)
              .expiration_time(expirationTime)
              .payer_account_id(payerAccount)
              .schedule_id(scheduleId != null ? scheduleId.toString() : null)
              .transaction_body(body.getScheduledTransactionBody().toByteArray())
              .wait_for_expiry(body.getWaitForExpiry())
              .signatures(insertTransactionSignatures(consensusTimestamp, recordItem.getSignatureMap()
                      .getSigPairList()))
              .memo(recordItem.getTransactionBody().getScheduleCreate().getMemo())
              .admin_key(recordItem.getTransactionBody().getScheduleCreate().getAdminKey().toByteArray())
              .deleted(recordItem.getTransactionBody().hasScheduleDelete())
              .build();
    }
    return null;
  }

  private static List<ScheduleSignature> insertTransactionSignatures(long consensusTimestamp,
                                                                     List<SignaturePair> signaturePairList) {
    Set<ByteString> publicKeyPrefixes = new HashSet<>();

    List<ScheduleSignature> signatures = new ArrayList<>();

    for (SignaturePair signaturePair : signaturePairList) {
      ByteString prefix = signaturePair.getPubKeyPrefix();
      ByteString signature = null;
      var signatureCase = signaturePair.getSignatureCase();
      int type = signatureCase.getNumber();

      switch (signatureCase) {
        case CONTRACT:
          signature = signaturePair.getContract();
          break;
        case ECDSA_384:
          signature = signaturePair.getECDSA384();
          break;
        case ECDSA_SECP256K1:
          signature = signaturePair.getECDSASecp256K1();
          break;
        case ED25519:
          signature = signaturePair.getEd25519();
          break;
        case RSA_3072:
          signature = signaturePair.getRSA3072();
          break;
        case SIGNATURE_NOT_SET:
          Map<Integer, UnknownFieldSet.Field> unknownFields =
                  signaturePair.getUnknownFields().asMap();

          // If we encounter a signature that our version of the protobuf does not yet support, it will
          // return SIGNATURE_NOT_SET. Hence we should look in the unknown fields for the new signature.
          // ByteStrings are stored as length-delimited on the wire, so we search the unknown fields for a
          // field that has exactly one length-delimited value and assume it's our new signature bytes.
          for (Map.Entry<Integer, UnknownFieldSet.Field> entry : unknownFields.entrySet()) {
            UnknownFieldSet.Field field = entry.getValue();
            if (field.getLengthDelimitedList().size() == 1) {
              signature = field.getLengthDelimitedList().get(0);
              type = entry.getKey();
              break;
            }
          }

          if (signature == null) {
            log.error("Unsupported signature at {}: {}", consensusTimestamp, unknownFields);
            continue;
          }
          break;
        default:
          log.error("Unsupported signature case at {}: {}",
                  consensusTimestamp,
                  signaturePair.getSignatureCase());
          continue;
      }

      // Handle potential public key prefix collisions by taking first occurrence only ignoring duplicates
      if (publicKeyPrefixes.add(prefix)) {

        ScheduleSignature.builder().build();

        signatures.add(ScheduleSignature
                .builder()
                .consensus_timestamp(consensusTimestamp)
                .public_key_prefix(DomainUtils.toBytes(prefix))
                .signature(DomainUtils.toBytes(signature))
                .type(SignaturePair.SignatureCase.forNumber(type).toString())
                .build());
      }
    }

    return signatures;
  }
}
