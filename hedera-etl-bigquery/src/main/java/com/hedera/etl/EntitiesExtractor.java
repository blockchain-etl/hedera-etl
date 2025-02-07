package com.hedera.etl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;

import com.hedera.etl.diff.MergeBatch;
import com.hedera.etl.entity.Block;
import com.hedera.etl.entity.account.Account;
import com.hedera.etl.entity.balance.Balance;
import com.hedera.etl.entity.network.ExchangeRateSet;
import com.hedera.etl.entity.network.NetworkFee;
import com.hedera.etl.entity.network.NetworkNode;
import com.hedera.etl.entity.network.NetworkStake;
import com.hedera.etl.entity.schedule.Schedule;
import com.hedera.etl.entity.smartcontracts.Contract;
import com.hedera.etl.entity.token.Token;
import com.hedera.etl.entity.topic.TopicMessage;
import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.recordfile.RecordFileTransform;

public class EntitiesExtractor {
  public static Map<String, PCollection<Row>> extract(PCollection<FileIO.ReadableFile> input) {
    var recordFiles = input.apply("Parse Record Files", new RecordFileTransform());
    var recordItems =
        recordFiles.apply(
            "Extract Record Items",
            FlatMapElements.into(TypeDescriptor.of(RecordItem.class)).via(RecordFile::getItems));

    var extractedFromRecordFiles =
        Extract.from(recordFiles).add(Block.class, Block::from).getOutput();

    var extractedFromRecordItems =
        Extract.from(recordItems)
            .add(Token.class, Token::from)
            .postprocessMultiOut(Token.class, MergeBatch.diffs("token", "token_id", "modified"))
            .add(TopicMessage.class, TopicMessage::from)
            .add(Contract.class, Contract::from)
            .postprocessMultiOut(
                Contract.class, MergeBatch.diffs("contract", "contract_id", "modified"))
            .add(Transaction.class, Transaction::from)
            .add(Schedule.class, Schedule::from)
            .add(NetworkStake.class, NetworkStake::from)
            .add(Account.class, Account::from)
            .postprocessMultiOut(Account.class, MergeBatch.diffs("account", "account", "modified"))
            .flatten(Balance.class, Balance::from)
            .postprocessMultiOut(
                Balance.class, MergeBatch.sum("balance", "account_id", "created", "amount"))
            .add(ExchangeRateSet.class, ExchangeRateSet::from)
            .add(NetworkFee.class, NetworkFee::from)
            .flatten(NetworkNode.class, NetworkNode::from)
            .postprocessMultiOut(
                NetworkNode.class, MergeBatch.diffs("network_node", "node_id", "timestamp"))
            .getOutput();

    var result = new HashMap<String, PCollection<Row>>();
    result.putAll(extractedFromRecordFiles);
    result.putAll(extractedFromRecordItems);

    return result;
  }

  private static class Extract<InputT> {
    private final PCollection<InputT> input;
    @Getter private final Map<String, PCollection<Row>> output;

    private Extract(PCollection<InputT> input) {
      this.input = input;
      this.output = new HashMap<>();
    }

    public <T> Extract<InputT> add(Class<T> type, SerializableFunction<InputT, T> mapper) {
      var toListMapper =
          (SerializableFunction<InputT, Iterable<T>>)
              input -> Optional.ofNullable(mapper.apply(input)).stream().toList();

      return flatten(type, toListMapper);
    }

    public <T> Extract<InputT> flatten(
        Class<T> type, SerializableFunction<InputT, Iterable<T>> mapper) {
      var className = type.getSimpleName();

      output.put(
          className,
          this.input
              .apply(
                  "Map Record Item into %s".formatted(className),
                  FlatMapElements.<T>into(TypeDescriptor.of(type)).via(mapper))
              .apply("Convert %s into Rows".formatted(className), Convert.toRows()));

      return this;
    }

    public <T> Extract<InputT> postprocess(
        Class<T> type, PTransform<PCollection<Row>, PCollection<Row>> transform) {
      var className = type.getSimpleName();

      output.put(
          className, output.get(className).apply("Postprocess %s".formatted(className), transform));

      return this;
    }

    public <T> Extract<InputT> postprocessMultiOut(
        Class<T> type, PTransform<PCollection<Row>, PCollectionTuple> transform) {
      var className = type.getSimpleName();

      var outputs = output.get(className).apply("Postprocess %s".formatted(className), transform);

      output.remove(className);

      outputs
          .getAll()
          .forEach(
              (tag, pcol) -> {
                var name =
                    StringUtils.isNotBlank(tag.getId())
                        ? "%s_%s".formatted(className, tag.getId())
                        : className;

                output.put(name, (PCollection<Row>) pcol);
              });

      return this;
    }

    public static <InputT> Extract<InputT> from(PCollection<InputT> input) {
      return new Extract<>(input);
    }
  }
}
