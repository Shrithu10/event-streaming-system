package com.eventstream.client;

import java.util.List;

/**
 * Returned by Consumer.poll().
 *
 * partitionId — which partition these messages came from.
 * nextOffset  — byte position to pass on the next poll() for this partition.
 */
public final class FetchResult {

    public final int         partitionId;
    public final List<byte[]> messages;
    public final long         nextOffset;

    public FetchResult(int partitionId, List<byte[]> messages, long nextOffset) {
        this.partitionId = partitionId;
        this.messages    = messages;
        this.nextOffset  = nextOffset;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
