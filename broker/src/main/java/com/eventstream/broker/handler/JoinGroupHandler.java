package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.group.ConsumerGroupManager.JoinResult;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class JoinGroupHandler {

    private final ConsumerGroupManager groupManager;

    public JoinGroupHandler(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * Frame payload (after type byte):
     *   [2B groupIdLen][G groupId]
     *   [2B consumerIdLen][C consumerId]
     *   [2B topicLen][N topic]
     *   [4B sessionTimeoutMs]
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        String topic      = readString(frame);
        if (groupId == null || consumerId == null || topic == null || frame.remaining() < 4) {
            return ResponseEncoder.joinGroupAck(ErrorCode.INTERNAL_ERROR, 0, new int[0]);
        }
        int sessionTimeoutMs = frame.getInt();

        JoinResult result = groupManager.joinGroup(groupId, consumerId, topic, sessionTimeoutMs);
        if (result == null) {
            return ResponseEncoder.joinGroupAck(ErrorCode.TOPIC_NOT_FOUND, 0, new int[0]);
        }
        return ResponseEncoder.joinGroupAck(ErrorCode.NONE,
                result.generationId(), result.assignedPartitions());
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
