package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.group.ConsumerGroupManager.OffsetCommitResult;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class OffsetCommitHandler {

    private final ConsumerGroupManager groupManager;

    public OffsetCommitHandler(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * Frame payload (after type byte):
     *   [2B groupIdLen][G groupId]
     *   [2B consumerIdLen][C consumerId]
     *   [4B generationId]
     *   [4B partitionId]
     *   [8B offset]
     *
     * The generationId check ensures a consumer that was evicted and replaced
     * cannot overwrite the offset committed by the new owner.
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        if (groupId == null || consumerId == null || frame.remaining() < 16) {
            return ResponseEncoder.groupAck(RequestType.OFFSET_COMMIT_ACK, ErrorCode.INTERNAL_ERROR);
        }
        int  generationId = frame.getInt();
        int  partitionId  = frame.getInt();
        long offset       = frame.getLong();

        OffsetCommitResult result = groupManager.commitOffset(
                groupId, consumerId, generationId, partitionId, offset);
        byte error = switch (result) {
            case OK                -> ErrorCode.NONE;
            case UNKNOWN_GROUP     -> ErrorCode.UNKNOWN_GROUP;
            case UNKNOWN_CONSUMER  -> ErrorCode.UNKNOWN_CONSUMER;
            case WRONG_GENERATION  -> ErrorCode.REBALANCE_IN_PROGRESS;
            case INVALID_PARTITION -> ErrorCode.INVALID_OFFSET;
        };
        return ResponseEncoder.groupAck(RequestType.OFFSET_COMMIT_ACK, error);
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
