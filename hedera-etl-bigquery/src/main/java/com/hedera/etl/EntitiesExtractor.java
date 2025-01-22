package com.hedera.etl;

import com.hedera.etl.entity.Block;
import com.hedera.etl.entity.topic.TopicMessage;
import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.entity.smartcontracts.Contract;
import com.hedera.etl.entity.token.Token;
import com.hedera.etl.recordfile.RecordFileTransform;
import com.hedera.etl.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.recordfile.domain.transaction.RecordItem;

import lombok.Getter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EntitiesExtractor {
  public static Map<String, PCollection<Row>> extract(PCollection<FileIO.ReadableFile> input) {
    var recordFiles = input.apply("Parse Record Files", new RecordFileTransform());
    var recordItems = recordFiles.apply("Extract Record Items", FlatMapElements
            .into(TypeDescriptor.of(RecordItem.class))
            .via(RecordFile::getItems));

    var extractedFromRecordFiles = Extract.from(recordFiles)
            .add(Block.class, Block::from)
            .getOutput();

    var extractedFromRecordItems = Extract.from(recordItems)
            .add(Token.class, Token::from)
            .add(TopicMessage.class, TopicMessage::from)
            .add(Contract.class, Contract::from)
            .add(Transaction.class, Transaction::from)
            .getOutput();

    var result = new HashMap<String, PCollection<Row>>();
    result.putAll(extractedFromRecordFiles);
    result.putAll(extractedFromRecordItems);

    return result;
  }

  private static class Extract<InputT> {
    private final PCollection<InputT> input;
    @Getter
    private final Map<String, PCollection<Row>> output;

    private Extract(PCollection<InputT> input) {
      this.input = input;
      this.output = new HashMap<>();
    }

    public <T> Extract<InputT> add(Class<T> type, SerializableFunction<InputT, T> mapper) {
      var className = type.getSimpleName();
      var toListMapper = (SerializableFunction<InputT, Iterable<T>>) input ->
        Optional.ofNullable(mapper.apply(input))
                .stream()
                .toList();

      output.put(
              className,
              this.input
                      .apply("Map Record Item into %s".formatted(className), FlatMapElements
                              .<T>into(TypeDescriptor.of(type))
                              .via(toListMapper))
                      .apply("Convert %s into Rows".formatted(className), Convert.toRows())
      );

      return this;
    }

    public static <InputT> Extract<InputT> from(PCollection<InputT> input) {
      return new Extract<>(input);
    }
  }
}
