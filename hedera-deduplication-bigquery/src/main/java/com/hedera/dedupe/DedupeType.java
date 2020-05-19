package com.hedera.dedupe;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum DedupeType {
    FULL("full"),
    INCREMENTAL("incremental");

    private final String name;

    @Override
    public String toString() {
        return name;
    }
}
