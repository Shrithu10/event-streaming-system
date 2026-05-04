package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class OffsetFetchHandler {

    private final ConsumerGroupManager groupManager;

    public OffsetFetchHandler(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * Frame payload (after type byte):
     *   [2B groupIdLen][G groupId]
     *   [2B consumerIdLen][C consumerId]
     *   [4B partitionId]
     *
     * Returns the last committed offset for (groupId, partitionId).
     * Returns -1 if no offset has been committed yet (consumer should start from 0).
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        if (groupId == null || consumerId == null || frame.remaining() < 4) {
            return ResponseEncoder.offsetFetchAck(ErrorCode.INTERNAL_ERROR, -1);
        }
        int partitionId = frame.getInt();

        long offset = groupManager.fetchOffset(groupId, partitionId);
        if (offset == Long.MIN_VALUE) {
            return ResponseEncoder.offsetFetchAck(ErrorCode.UNKNOWN_GROUP, -1);
        }
        return ResponseEncoder.offsetFetchAck(ErrorCode.NONE, offset);
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
