package com.eventstream.broker.handler;

import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.broker.topic.Partition;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ErrorCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public final class ProduceHandler {

    private static final Logger log = Logger.getLogger(ProduceHandler.class.getName());

    private final TopicManager topicManager;

    public ProduceHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * Frame payload (after the request-type byte):
     *   [2 bytes: topic length][N bytes: topic][4 bytes: msg length][M bytes: msg]
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topicName = readString(frame);
        if (topicName == null) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1);
        }

        if (frame.remaining() < 4) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1);
        }
        int msgLen = frame.getInt();
        if (frame.remaining() < msgLen || msgLen < 0) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1);
        }
        byte[] message = new byte[msgLen];
        frame.get(message);

        // Phase 1: single partition per topic (partition 0).
        Partition partition = topicManager.getPartition(topicName, 0);
        if (partition == null) {
            return ResponseEncoder.produceAck(ErrorCode.TOPIC_NOT_FOUND, -1);
        }

        try {
            long offset = partition.append(message);
            return ResponseEncoder.produceAck(ErrorCode.NONE, offset);
        } catch (IOException e) {
            log.severe("Append failed for topic " + topicName + ": " + e.getMessage());
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1);
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
