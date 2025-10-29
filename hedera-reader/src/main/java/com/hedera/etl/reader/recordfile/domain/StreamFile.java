package com.hedera.etl.reader.recordfile.domain;

import java.util.ArrayList;
import java.util.Collection;

import lombok.NonNull;

import com.hedera.etl.reader.recordfile.entity.StreamItem;

public interface StreamFile<T extends StreamItem> {

  default void clear() {
    setBytes(null);
    setItems(new ArrayList<>());
  }

  StreamFile<T> copy();

  byte[] getBytes();

  Long getConsensusEnd();

  Long getConsensusStart();

  Long getCount();

  String getFileHash();

  // Get the chained hash of the stream file
  default String getHash() {
    return null;
  }

  default Long getIndex() {
    return null;
  }

  Collection<T> getItems();

  Long getLoadEnd();

  Long getLoadStart();

  default String getMetadataHash() {
    return null;
  }

  String getName();

  Long getNodeId();

  // Get the chained hash of the previous stream file
  default String getPreviousHash() {
    return null;
  }

  void setBytes(byte[] bytes);

  default void setConsensusEnd(Long timestamp) {}

  void setConsensusStart(Long timestamp);

  default void setHash(String hash) {}

  default void setIndex(Long index) {}

  void setItems(Collection<T> items);

  void setName(String name);

  void setNodeId(@NonNull Long nodeId);

  default void setPreviousHash(String previousHash) {}
}
