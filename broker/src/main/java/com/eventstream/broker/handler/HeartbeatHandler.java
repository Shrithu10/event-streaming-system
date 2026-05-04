package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.group.ConsumerGroupManager.HeartbeatResult;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class HeartbeatHandler {

    private final ConsumerGroupManager groupManager;

    public HeartbeatHandler(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * Frame payload (after type byte):
     *   [2B groupIdLen][G groupId]
     *   [2B consumerIdLen][C consumerId]
     *   [4B generationId]
     *
     * Returns REBALANCE_IN_PROGRESS when the consumer's generationId is stale,
     * signalling that it must call JOIN_GROUP before fetching again.
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        if (groupId == null || consumerId == null || frame.remaining() < 4) {
            return ResponseEncoder.groupAck(RequestType.HEARTBEAT_ACK, ErrorCode.INTERNAL_ERROR);
        }
        int generationId = frame.getInt();

        HeartbeatResult result = groupManager.heartbeat(groupId, consumerId, generationId);
        byte error = switch (result) {
            case OK                    -> ErrorCode.NONE;
            case UNKNOWN_GROUP         -> ErrorCode.UNKNOWN_GROUP;
            case UNKNOWN_CONSUMER      -> ErrorCode.UNKNOWN_CONSUMER;
            case REBALANCE_IN_PROGRESS -> ErrorCode.REBALANCE_IN_PROGRESS;
        };
        return ResponseEncoder.groupAck(RequestType.HEARTBEAT_ACK, error);
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
