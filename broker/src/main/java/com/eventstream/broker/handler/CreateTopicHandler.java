package com.eventstream.broker.handler;

import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ErrorCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public final class CreateTopicHandler {

    private static final Logger log = Logger.getLogger(CreateTopicHandler.class.getName());

    private final TopicManager topicManager;

    public CreateTopicHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * Phase 2 frame payload (after type byte):
     *   [2B topicLen][N topic][4B numPartitions]
     *
     * numPartitions defaults to 1 if the field is absent (backward compat
     * with Phase 1 clients that do not send it).
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topicName = readString(frame);
        if (topicName == null || topicName.isEmpty()) {
            return ResponseEncoder.createTopicAck(ErrorCode.INTERNAL_ERROR);
        }

        int numPartitions = frame.remaining() >= 4 ? frame.getInt() : 1;
        if (numPartitions < 1 || numPartitions > 1024) {
            return ResponseEncoder.createTopicAck(ErrorCode.INTERNAL_ERROR);
        }

        try {
            boolean created = topicManager.createTopic(topicName, numPartitions);
            return ResponseEncoder.createTopicAck(created ? ErrorCode.NONE : ErrorCode.TOPIC_EXISTS);
        } catch (IOException e) {
            log.severe("Failed to create topic " + topicName + ": " + e.getMessage());
            return ResponseEncoder.createTopicAck(ErrorCode.INTERNAL_ERROR);
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
