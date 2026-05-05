package com.eventstream.broker.topic;

import com.eventstream.broker.storage.LogEntry;
import com.eventstream.broker.storage.LogSegment;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Represents a single partition of a topic.
 *
 * In Phase 1 a partition owns exactly one LogSegment (one file).
 * Phase 2 will introduce multiple rolling segments and an index file.
 */
public final class Partition implements Closeable {

    public final String topicName;
    public final int    partitionId;

    private final LogSegment segment;

    public Partition(String topicName, int partitionId, LogSegment segment) {
        this.topicName   = topicName;
        this.partitionId = partitionId;
        this.segment     = segment;
    }

    public long append(byte[] message) throws IOException {
        return segment.append(message);
    }

    public List<LogEntry> fetch(long startOffset, int maxBytes) throws IOException {
        return segment.read(startOffset, maxBytes);
    }

    public long size() {
        return segment.size();
    }

    /** Byte offset of the next record to be written (used to initialise HW tracking). */
    public long writePosition() {
        return segment.size();
    }

    @Override
    public void close() throws IOException {
        segment.close();
    }
}
