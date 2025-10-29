package com.hedera.etl;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;

import com.hedera.etl.diff.Merge;
import com.hedera.etl.entity.Block;
import com.hedera.etl.entity.account.Account;
import com.hedera.etl.entity.balance.Balance;
import com.hedera.etl.entity.balance.TokenTransfer;
import com.hedera.etl.entity.file.File;
import com.hedera.etl.entity.network.ExchangeRateSet;
import com.hedera.etl.entity.network.NetworkFee;
import com.hedera.etl.entity.network.NetworkNode;
import com.hedera.etl.entity.network.NetworkStake;
import com.hedera.etl.entity.schedule.Schedule;
import com.hedera.etl.entity.smartcontracts.Contract;
import com.hedera.etl.entity.token.Token;
import com.hedera.etl.entity.topic.TopicMessage;
import com.hedera.etl.entity.transaction.Transaction;
import com.hedera.etl.errata.historical.HistoricalAccountAndContractRepository;
import com.hedera.etl.errata.transaction.ErrataRepository;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordFile;
import com.hedera.etl.reader.recordfile.domain.transaction.RecordItem;
import com.hedera.etl.util.FormatUtils;

public class EntitiesExtractor {

  private static final LocalDate FIRST_ENTRIES_DATE = LocalDate.of(2019, 9, 13);

  private static final String RESTRICTED_PREFIX = "restricted.";
  private static final String OPEN_PREFIX = "open.";

  public static Result extract(PCollection<RecordFile> input) {
    return extract(input, List.of());
  }

  public static Result extract(PCollection<RecordFile> recordFiles, List<String> enabledOutputs) {
    var recordItems =
        recordFiles
            .apply(
                "Extract Record Items",
                FlatMapElements.into(TypeDescriptor.of(RecordItem.class)).via(RecordFile::getItems))
            .apply(
                "Add timestamps to records",
                WithTimestamps.<RecordItem>of(
                        row -> FormatUtils.jodaInstantFromNanos(row.getConsensusTimestamp()))
                    .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)));

    var recordItemsWithErratas = joinErratas(recordItems);

    var outputsForRestricted = getOutputsFor(enabledOutputs, RESTRICTED_PREFIX);
    var outputsForOpen = getOutputsFor(enabledOutputs, OPEN_PREFIX);

    Map<String, PCollection<Row>> restrictedAccess =
        outputsForRestricted.isEmpty() && !outputsForOpen.isEmpty()
            ? Map.of()
            : extractRestrictedAccessData(
                recordFiles, recordItemsWithErratas, outputsForRestricted);

    Map<String, PCollection<Row>> openAccess =
        outputsForOpen.isEmpty() && !outputsForRestricted.isEmpty()
            ? Map.of()
            : extractOpenAccessData(recordFiles, outputsForOpen);

    return Result.from(restrictedAccess, openAccess);
  }

  private static Set<String> getOutputsFor(List<String> enabledOutputs, String prefix) {
    return enabledOutputs.stream()
        .filter(out -> out.startsWith(prefix))
        .map(out -> out.replace(prefix, "").toLowerCase())
        .collect(Collectors.toSet());
  }

  private static PCollection<RecordItem> joinErratas(PCollection<RecordItem> input) {
    return input.apply(
        "Union of record items with erratas", Flatten.with(getErratas(input.getPipeline())));
  }

  private static PCollection<RecordItem> getErratas(Pipeline pipeline) {
    var options = pipeline.getOptions();

    if (!(options instanceof BatchStorageToBigQueryPipelineOptions)) {
      return pipeline.apply("No erratas", Create.empty(TypeDescriptor.of(RecordItem.class)));
    }
    var batchOptions = options.as(BatchStorageToBigQueryPipelineOptions.class);

    var startTimestamp =
        StringUtils.isBlank(batchOptions.getStartAboveFile())
            ? FormatUtils.nanosFromLocalDateTime(batchOptions.getIngestionDate().atStartOfDay())
            : FormatUtils.nanosFromFileName(batchOptions.getStartAboveFile());

    var endTimestamp =
        FormatUtils.nanosFromLocalDateTime(
            batchOptions.getIngestionEndDate().atTime(LocalTime.MAX));

    var erratas = ErrataRepository.readErratasForRange(startTimestamp, endTimestamp);

    if (erratas.isEmpty()) {
      return pipeline.apply("No erratas", Create.empty(TypeDescriptor.of(RecordItem.class)));
    }

    return pipeline.apply("Load erratas", Create.of(erratas));
  }

  private static Map<String, PCollection<Row>> extractRestrictedAccessData(
      PCollection<RecordFile> files, PCollection<RecordItem> items, Set<String> outputs) {
    var batchOptions =
        files.getPipeline().getOptions().as(BatchStorageToBigQueryPipelineOptions.class);

    var extractedFromRecordFiles =
        Extract.from("Extract restricted access data from Record Files", files, outputs)
            .add(Block.class, Block::from)
            .getOutput();

    var extractedFromRecordItems =
        Extract.from("Extract restricted access data from Record Items", items, outputs)
            .add(Token.class, Token::from)
            .postprocessMultiOut(
                Token.class, Merge.diffs("token", "token_id", "modified_timestamp"))
            .add(TopicMessage.class, TopicMessage::from)
            .add(File.class, File::from)
            .add(Transaction.class, Transaction::from)
            .flatten(Schedule.class, Schedule::from)
            .postprocessMultiOut(
                Schedule.class, Merge.diffs("schedule", "schedule_id", "modified_timestamp"))
            .add(NetworkStake.class, NetworkStake::from)
            .add(Account.class, Account::from, initAccounts(batchOptions))
            .postprocessMultiOut(
                Account.class, Merge.diffs("account", "account", "modified_timestamp"))
            .flatten(Balance.class, Balance::from)
            //            .postprocessMultiOut(
            //                Balance.class, Merge.sum("balance", "account_id",
            // "consensus_timestamp", "amount"))
            .flatten(TokenTransfer.class, TokenTransfer::from)
            .postprocessMultiOut(
                TokenTransfer.class,
                Merge.sum(
                    "token_transfer",
                    List.of("account_id", "token_id", "serial_number"),
                    "consensus_timestamp",
                    "amount"))
            .add(ExchangeRateSet.class, ExchangeRateSet::from)
            .add(NetworkFee.class, NetworkFee::from)
            .flatten(NetworkNode.class, NetworkNode::from)
            .postprocessMultiOut(
                NetworkNode.class, Merge.diffs("network_node", "node_id", "timestamp"))
            .add(Contract.class, Contract::from, initContracts(batchOptions))
            .postprocessMultiIn(
                Contract.class,
                Map.of(
                    "File",
                    Contract.JoinWithFileByteCode.FILES_TAG,
                    "Contract",
                    Contract.JoinWithFileByteCode.INPUT_TAG),
                new Contract.JoinWithFileByteCode())
            .postprocessMultiOut(
                Contract.class, Merge.diffs("contract", "contract_id", "modified_timestamp"))
            .getOutput();

    var result = new HashMap<String, PCollection<Row>>();
    result.putAll(extractedFromRecordFiles);
    result.putAll(extractedFromRecordItems);

    return result;
  }

  private static List<Account> initAccounts(BatchStorageToBigQueryPipelineOptions options) {
    if (FIRST_ENTRIES_DATE.equals(options.getIngestionDate())
        && StringUtils.isBlank(options.getStartAboveFile())) {
      return HistoricalAccountAndContractRepository.getAccounts();
    } else {
      return List.of();
    }
  }

  private static List<Contract> initContracts(BatchStorageToBigQueryPipelineOptions options) {
    if (FIRST_ENTRIES_DATE.equals(options.getIngestionDate())
        && StringUtils.isBlank(options.getStartAboveFile())) {
      return HistoricalAccountAndContractRepository.getContracts();
    } else {
      return List.of();
    }
  }

  private static Map<String, PCollection<Row>> extractOpenAccessData(
      PCollection<RecordFile> files, Set<String> outputs) {
    var fromFiles =
        Extract.from("Extract open access data", files, outputs)
            .add(
                com.hedera.etl.entity.openaccess.Block.class,
                com.hedera.etl.entity.openaccess.Block::from)
            .flatten(
                com.hedera.etl.entity.openaccess.NativeTokenBalance.class,
                com.hedera.etl.entity.openaccess.NativeTokenBalance::from)
            .flatten(
                com.hedera.etl.entity.openaccess.Transaction.class,
                com.hedera.etl.entity.openaccess.Transaction::from)
            .getOutput();

    var fromErratas =
        Extract.from(
                "Extract open access data from erratas", getErratas(files.getPipeline()), outputs)
            .flatten(
                com.hedera.etl.entity.openaccess.NativeTokenBalance.class,
                com.hedera.etl.entity.openaccess.NativeTokenBalance::from)
            .add(
                com.hedera.etl.entity.openaccess.Transaction.class,
                com.hedera.etl.entity.openaccess.Transaction::from)
            .getOutput();

    var result = new HashMap<String, PCollection<Row>>();

    if (fromFiles.get("Transaction") != null) {
      var transactionWithErratas =
          fromFiles
              .get("Transaction")
              .apply(
                  "Union with transaction erratas", Flatten.with(fromErratas.get("Transaction")));
      result.put("Transaction", transactionWithErratas);
    }
    if (fromFiles.get("NativeTokenBalance") != null) {
      var tokenBalanceWithErratas =
          fromFiles
              .get("NativeTokenBalance")
              .apply(
                  "Union with token balance erratas",
                  Flatten.with(fromErratas.get("NativeTokenBalance")));

      result.put("NativeTokenBalance", tokenBalanceWithErratas);
    }
    if (fromFiles.get("Block") != null) {
      result.put("Block", fromFiles.get("Block"));
    }
    return result;
  }

  public record Result(
      Map<String, PCollection<Row>> restrictedAccess,
      Map<String, PCollection<Row>> openAccess,
      Map<String, PCollection<Row>> technical) {
    static Result from(
        Map<String, PCollection<Row>> restrictedAccess, Map<String, PCollection<Row>> openAccess) {

      var filteredRestrictedAccess =
          restrictedAccess.entrySet().stream()
              .filter(
                  e ->
                      !(e.getKey().contains(Merge.DIFFS.getId())
                          || e.getKey().contains(Merge.LATEST.getId())
                          || e.getKey().equals("Balance")))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      //      replacePCol(filteredRestrictedAccess, "Balance", "native_token_balance");
      replacePCol(filteredRestrictedAccess, "TokenTransfer", "token_balance");

      var technical =
          restrictedAccess.entrySet().stream()
              .filter(
                  e ->
                      (e.getKey().contains(Merge.DIFFS.getId())
                              || e.getKey().contains(Merge.LATEST.getId()))
                          || e.getKey().equals("Balance"))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      replacePCol(technical, "Balance", "native_token_transfer");
      replacePCol(technical, "TokenTransfer_diffs", "token_transfer");

      return new Result(filteredRestrictedAccess, openAccess, technical);
    }

    private static void replacePCol(
        Map<String, PCollection<Row>> result, String oldName, String newName) {
      var col = result.get(oldName);
      if (col == null) {
        return;
      }
      result.remove(oldName);
      result.put(newName, col);
    }
  }

  private static class Extract<InputT> {
    private final String name;
    private final PCollection<InputT> input;
    private final Set<String> outputs;
    @Getter private final Map<String, PCollection<Row>> output;

    private Extract(String name, PCollection<InputT> input, Set<String> outputs) {
      this.name = name;
      this.input = input;
      this.outputs = outputs;
      this.output = new HashMap<>();
    }

    public <T> Extract<InputT> add(Class<T> type, SerializableFunction<InputT, T> mapper) {
      return add(type, mapper, null);
    }

    public <T> Extract<InputT> add(
        Class<T> type, SerializableFunction<InputT, T> mapper, Iterable<T> inject) {
      var toListMapper =
          (SerializableFunction<InputT, Iterable<T>>)
              input -> Optional.ofNullable(mapper.apply(input)).stream().toList();

      return flatten(type, toListMapper, inject);
    }

    public <T> Extract<InputT> flatten(
        Class<T> type, SerializableFunction<InputT, Iterable<T>> mapper) {
      return flatten(type, mapper, null);
    }

    public <T> Extract<InputT> flatten(
        Class<T> type, SerializableFunction<InputT, Iterable<T>> mapper, Iterable<T> inject) {
      var className = type.getSimpleName();

      SerializableFunction<InputT, Iterable<T>> filteredMapper =
          in -> Iterables.filter(mapper.apply(in), Objects::nonNull);

      if (outputs.isEmpty() || outputs.contains(className.toLowerCase())) {
        var mappedItems =
            this.input.apply(
                "%s/%s/Map Record Item".formatted(name, className),
                FlatMapElements.<T>into(TypeDescriptor.of(type)).via(filteredMapper));
        var itemsWithInjection = mappedItems;
        if (inject != null && Iterables.size(inject) > 0) {
          itemsWithInjection =
              mappedItems.apply(
                  "%s/%s/Union with injection".formatted(name, className),
                  Flatten.with(Create.of(inject)));
        }

        output.put(
            className,
            itemsWithInjection.apply(
                "%s/%s/Convert into Rows".formatted(name, className), Convert.toRows()));
      }

      return this;
    }

    public <T> Extract<InputT> postprocess(
        Class<T> type, PTransform<PCollection<Row>, PCollection<Row>> transform) {
      var className = type.getSimpleName();

      if (outputs.isEmpty() || outputs.contains(className.toLowerCase())) {
        output.put(
            className,
            output.get(className).apply("%s/%s/Postprocess".formatted(name, className), transform));
      }

      return this;
    }

    public <T> Extract<InputT> postprocessMultiOut(
        Class<T> type, PTransform<PCollection<Row>, PCollectionTuple> transform) {
      var className = type.getSimpleName();

      if (outputs.isEmpty() || outputs.contains(className.toLowerCase())) {
        var outputs =
            output.get(className).apply("%s/%s/Postprocess".formatted(name, className), transform);

        output.remove(className);

        outputs
            .getAll()
            .forEach(
                (tag, pcol) -> {
                  var name =
                      StringUtils.isNotBlank(tag.getId()) && !Merge.UPDATED.equals(tag)
                          ? "%s_%s".formatted(className, tag.getId())
                          : className;

                  output.put(name, (PCollection<Row>) pcol);
                });
      }

      return this;
    }

    public <T> Extract<InputT> postprocessMultiIn(
        Class<T> type,
        Map<String, TupleTag<Row>> tags,
        PTransform<PCollectionTuple, PCollection<Row>> transform) {
      var className = type.getSimpleName();

      if (outputs.isEmpty() || outputs.contains(className.toLowerCase())) {
        var inputs = PCollectionTuple.empty(input.getPipeline());
        for (var entry : tags.entrySet()) {
          inputs = inputs.and(entry.getValue(), output.get(entry.getKey()));
        }

        var result = inputs.apply("%s/%s/Postprocess".formatted(name, className), transform);

        output.remove(className);
        output.put(className, result);
      }

      return this;
    }

    public static <InputT> Extract<InputT> from(
        String name, PCollection<InputT> input, Set<String> outputs) {
      return new Extract<>(name, input, outputs);
    }
  }
}
