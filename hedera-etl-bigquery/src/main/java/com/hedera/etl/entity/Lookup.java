package com.hedera.etl.entity;

import java.util.Optional;

import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ContractID;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;

import com.hedera.etl.entity.account.Account;
import com.hedera.etl.reader.recordfile.entity.EntityId;
import com.hedera.etl.reader.recordfile.utils.DomainUtils;

@Log4j2
public class Lookup {
  public static Optional<com.hedera.etl.reader.recordfile.entity.EntityId> lookup(
      ContractID... ids) {
    for (var id : ids) {
      var entityId = doLookup(id);
      if (!entityId.isEmpty() && !EntityId.isEmpty(entityId.get())) {
        return entityId;
      }
    }
    return Optional.empty();
  }

  public static Optional<EntityId> doLookup(ContractID contractId) {
    if (contractId == null || contractId.equals(ContractID.getDefaultInstance())) {
      return Optional.empty();
    }

    return switch (contractId.getContractCase()) {
      case CONTRACTNUM -> Optional.ofNullable(EntityId.of(contractId));
      case EVM_ADDRESS ->
          findByEvmAddress(
              DomainUtils.toBytes(contractId.getEvmAddress()),
              contractId.getShardNum(),
              contractId.getRealmNum());
      default -> {
        log.warn("Invalid ContractID: {}", contractId);
        yield Optional.empty();
      }
    };
  }

  private static Optional<EntityId> findByEvmAddress(
      byte[] evmAddress, long shardNum, long realmNum) {
    var id =
        Optional.ofNullable(DomainUtils.fromEvmAddress(evmAddress))
            // Verify shard and realm match when assuming evmAddress is in the 'shard.realm.num'
            // form
            .filter(e -> e.getShard() == shardNum && e.getRealm() == realmNum);
    // .or(() -> entityRepository.findByEvmAddress(evmAddress).map(EntityId::of));

    if (id.isEmpty()) {
      log.warn("Entity not found for EVM address {}", Hex.encodeHexString(evmAddress));
    }

    return id;
  }

  public static Optional<EntityId> lookup(AccountID accountId) {
    if (accountId == null || accountId.equals(AccountID.getDefaultInstance())) {
      return Optional.empty();
    }

    return switch (accountId.getAccountCase()) {
      case ACCOUNTNUM -> Optional.ofNullable(EntityId.of(accountId));
      case ALIAS -> {
        long shard = accountId.getShardNum();
        long realm = accountId.getRealmNum();
        byte[] alias = DomainUtils.toBytes(accountId.getAlias());
        yield alias.length == DomainUtils.EVM_ADDRESS_LENGTH
            ? findByEvmAddress(alias, shard, realm)
            : findByAliasEvmAddress(alias, shard, realm);
      }
      default -> {
        log.warn(
            "Invalid Account Case for AccountID {}: {}", accountId, accountId.getAccountCase());
        yield Optional.empty();
      }
    };
  }

  private static Optional<EntityId> findByAliasEvmAddress(
      byte[] alias, long shardNum, long realmNum) {
    var evmAddress = Account.aliasToEvmAddress(alias);
    if (evmAddress == null) {
      log.warn("Unable to find entity for alias {}", Hex.encodeHexString(alias));
      return Optional.empty();
    }

    return findByEvmAddress(evmAddress, shardNum, realmNum);
  }
}
