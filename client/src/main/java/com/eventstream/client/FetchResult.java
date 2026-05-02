package com.eventstream.client;

import java.util.List;

/**
 * Returned by Consumer.poll().
 *
 * nextOffset is the byte position to pass on the next poll() call to continue
 * reading sequentially.  It equals the byte position immediately after the
 * last record received.
 */
public final class FetchResult {

    public final List<byte[]> messages;
    public final long         nextOffset;

    public FetchResult(List<byte[]> messages, long nextOffset) {
        this.messages   = messages;
        this.nextOffset = nextOffset;
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
