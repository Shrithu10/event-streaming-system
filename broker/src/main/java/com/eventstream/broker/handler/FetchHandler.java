package com.eventstream.broker.handler;

import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.broker.storage.LogEntry;
import com.eventstream.broker.topic.Partition;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ErrorCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public final class FetchHandler {

    private static final Logger log = Logger.getLogger(FetchHandler.class.getName());

    private final TopicManager topicManager;

    public FetchHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * Phase 2 frame payload (after type byte):
     *   [2B topicLen][N topic]
     *   [4B partitionId]      ← NEW: consumer specifies partition explicitly
     *   [8B startOffset]
     *   [4B maxBytes]
     *
     * Consumers are responsible for tracking which partitions they consume and
     * their per-partition offsets. Phase 3 (consumer groups) will introduce
     * automatic offset management.
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topicName = readString(frame);
        if (topicName == null) {
            return ResponseEncoder.fetchResponse(ErrorCode.INTERNAL_ERROR, Collections.emptyList());
        }

        if (frame.remaining() < 16) { // partitionId(4) + startOffset(8) + maxBytes(4)
            return ResponseEncoder.fetchResponse(ErrorCode.INTERNAL_ERROR, Collections.emptyList());
        }
        int  partitionId  = frame.getInt();
        long startOffset  = frame.getLong();
        int  maxBytes     = frame.getInt();

        if (startOffset < 0) {
            return ResponseEncoder.fetchResponse(ErrorCode.INVALID_OFFSET, Collections.emptyList());
        }

        Partition partition = topicManager.getPartition(topicName, partitionId);
        if (partition == null) {
            return ResponseEncoder.fetchResponse(ErrorCode.TOPIC_NOT_FOUND, Collections.emptyList());
        }

        try {
            List<LogEntry> entries = partition.fetch(startOffset, maxBytes);
            return ResponseEncoder.fetchResponse(ErrorCode.NONE, entries);
        } catch (IOException e) {
            log.severe("Fetch failed for topic=" + topicName + " partition=" + partitionId
                    + ": " + e.getMessage());
            return ResponseEncoder.fetchResponse(ErrorCode.INTERNAL_ERROR, Collections.emptyList());
        }
    }

    private static String readString(ByteBuffer buf) {
        if (buf.remaining() < 2) return null;
        int len = buf.getShort() & 0xFFFF;
        if (buf.remaining() < len) return null;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
