package com.eventstream.broker.handler;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.group.ConsumerGroupManager.LeaveResult;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class LeaveGroupHandler {

    private final ConsumerGroupManager groupManager;

    public LeaveGroupHandler(ConsumerGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    /**
     * Frame payload (after type byte):
     *   [2B groupIdLen][G groupId]
     *   [2B consumerIdLen][C consumerId]
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String groupId    = readString(frame);
        String consumerId = readString(frame);
        if (groupId == null || consumerId == null) {
            return ResponseEncoder.groupAck(RequestType.LEAVE_GROUP_ACK, ErrorCode.INTERNAL_ERROR);
        }

        LeaveResult result = groupManager.leaveGroup(groupId, consumerId);
        byte error = switch (result) {
            case OK              -> ErrorCode.NONE;
            case UNKNOWN_GROUP   -> ErrorCode.UNKNOWN_GROUP;
            case UNKNOWN_CONSUMER -> ErrorCode.UNKNOWN_CONSUMER;
        };
        return ResponseEncoder.groupAck(RequestType.LEAVE_GROUP_ACK, error);
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
