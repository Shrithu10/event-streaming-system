package com.eventstream.broker.storage;

/**
 * A record returned by LogSegment.read().
 * offset is the byte position of this record in the segment file.
 */
public final class LogEntry {

    public final long   offset;
    public final byte[] payload;

    public LogEntry(long offset, byte[] payload) {
        this.offset  = offset;
        this.payload = payload;
    }
}
