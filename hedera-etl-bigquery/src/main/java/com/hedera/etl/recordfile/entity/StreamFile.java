package com.hedera.etl.recordfile.entity;

import lombok.NonNull;
import java.util.Collection;
import java.util.List;

public interface StreamFile<T extends StreamItem> {

    default void clear() {
        setBytes(null);
        setItems(List.of());
    }

    StreamFile<T> copy();

    byte[] getBytes();

    void setBytes(byte[] bytes);

    Long getConsensusStart();

    void setConsensusStart(Long timestamp);

    Long getConsensusEnd();

    default void setConsensusEnd(Long timestamp) {}

    Long getCount();

    String getFileHash();

    // Get the chained hash of the stream file
    default String getHash() {
        return null;
    }

    default void setHash(String hash) {}

    default Long getIndex() {
        return null;
    }

    default void setIndex(Long index) {}

    Collection<T> getItems();

    void setItems(Collection<T> items);

    Long getLoadEnd();

    Long getLoadStart();

    default String getMetadataHash() {
        return null;
    }

    String getName();

    void setName(String name);

    Long getNodeId();

    void setNodeId(@NonNull Long nodeId);

    // Get the chained hash of the previous stream file
    default String getPreviousHash() {
        return null;
    }

    default void setPreviousHash(String previousHash) {}
}
