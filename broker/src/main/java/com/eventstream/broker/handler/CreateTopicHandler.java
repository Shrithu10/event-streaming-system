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
     * Frame payload (after the request-type byte):
     *   [2 bytes: topic name length][N bytes: topic name (UTF-8)]
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topicName = readString(frame);
        if (topicName == null || topicName.isEmpty()) {
            return ResponseEncoder.createTopicAck(ErrorCode.INTERNAL_ERROR);
        }

        try {
            boolean created = topicManager.createTopic(topicName);
            byte errorCode = created ? ErrorCode.NONE : ErrorCode.TOPIC_EXISTS;
            return ResponseEncoder.createTopicAck(errorCode);
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
