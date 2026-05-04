package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroup;
import com.eventstream.broker.group.ConsumerGroup.FetchValidation;
import com.eventstream.broker.group.ConsumerGroupManager;
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

    private final TopicManager         topicManager;
    private final ConsumerGroupManager groupManager;

    public FetchHandler(TopicManager topicManager, ConsumerGroupManager groupManager) {
        this.topicManager = topicManager;
        this.groupManager = groupManager;
    }

    /**
     * Phase 3 frame payload (after type byte):
     *   [2B topicLen][N topic]
     *   [2B groupIdLen][G groupId]        empty string = plain (non-group) fetch
     *   [2B consumerIdLen][C consumerId]  empty for plain fetch
     *   [4B generationId]                 0 for plain fetch
     *   [4B partitionId]
     *   [8B startOffset]                  -1 = use last committed offset for this group
     *   [4B maxBytes]
     *
     * Group-aware path:
     *   1. Look up the group.
     *   2. Validate: state STABLE, generationId match, consumer is a member,
     *      partition is assigned to this consumer.
     *   3. Resolve startOffset: -1 → committedOffsets[partitionId] (0 if never committed).
     *   4. Read from LogSegment (identical to plain path).
     *
     * Plain path (empty groupId): skip all group validation.
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topic      = readString(frame);
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        if (topic == null || groupId == null || consumerId == null || frame.remaining() < 16) {
            return ResponseEncoder.fetchResponse(ErrorCode.INTERNAL_ERROR, Collections.emptyList());
        }

        int  generationId = frame.getInt();
        int  partitionId  = frame.getInt();
        long startOffset  = frame.getLong();
        int  maxBytes     = frame.getInt();

        if (startOffset < -1) {
            return ResponseEncoder.fetchResponse(ErrorCode.INVALID_OFFSET, Collections.emptyList());
        }

        // ---- Group-aware validation ----
        if (!groupId.isEmpty()) {
            ConsumerGroup group = groupManager.getGroup(groupId);
            if (group == null) {
                return ResponseEncoder.fetchResponse(ErrorCode.UNKNOWN_GROUP, Collections.emptyList());
            }

            FetchValidation v = group.validateFetch(consumerId, generationId, partitionId);
            if (v != FetchValidation.OK) {
                byte errCode = switch (v) {
                    case REBALANCING, WRONG_GENERATION -> ErrorCode.REBALANCE_IN_PROGRESS;
                    case UNKNOWN_CONSUMER              -> ErrorCode.UNKNOWN_CONSUMER;
                    case NOT_ASSIGNED                  -> ErrorCode.NOT_ASSIGNED;
                    default                            -> ErrorCode.INTERNAL_ERROR;
                };
                return ResponseEncoder.fetchResponse(errCode, Collections.emptyList());
            }

            // Resolve -1 → last committed offset (or 0 if never committed)
            if (startOffset == -1L) {
                long committed = group.committedOffsets.get(partitionId);
                startOffset = committed == -1L ? 0L : committed;
            }
        }

        // ---- Storage read (identical for both paths) ----
        Partition partition = topicManager.getPartition(topic, partitionId);
        if (partition == null) {
            return ResponseEncoder.fetchResponse(ErrorCode.TOPIC_NOT_FOUND, Collections.emptyList());
        }

        try {
            List<LogEntry> entries = partition.fetch(startOffset, maxBytes);
            return ResponseEncoder.fetchResponse(ErrorCode.NONE, entries);
        } catch (IOException e) {
            log.severe("Fetch failed topic=" + topic + " partition=" + partitionId + ": " + e.getMessage());
            return ResponseEncoder.fetchResponse(ErrorCode.INTERNAL_ERROR, Collections.emptyList());
        }
    }

    private static String readString(ByteBuffer buf) {
        if (buf.remaining() < 2) return null;
        int len = buf.getShort() & 0xFFFF;
        if (buf.remaining() < len) return null;
        byte[] b = new byte[len];
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
