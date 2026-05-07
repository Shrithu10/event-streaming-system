package com.eventstream.broker.handler;

import com.eventstream.broker.cluster.ReplicationManager;
import com.eventstream.broker.metrics.BrokerMetrics;
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

    private final TopicManager       topicManager;
    private final ReplicationManager replicationManager;

    public ProduceHandler(TopicManager topicManager, ReplicationManager replicationManager) {
        this.topicManager       = topicManager;
        this.replicationManager = replicationManager;
    }

    /**
     * Phase 2 frame payload (after type byte):
     *   [2B topicLen][N topic]
     *   [4B keyLen]  — -1 signals "no key" (round-robin), 0 also means no key
     *   [K key]      — present only when keyLen > 0
     *   [4B payloadLen][M payload]
     *
     * Partition selection:
     *   key present → murmur2(key) % numPartitions
     *   key absent  → round-robin counter % numPartitions
     *
     * The broker resolves the partition; the client never needs to know the
     * partition count upfront for produces.
     */
    public ByteBuffer handle(ByteBuffer frame) {
        String topicName = readString(frame);
        if (topicName == null) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
        }

        if (frame.remaining() < 4) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
        }
        int keyLen = frame.getInt();
        byte[] key = null;
        if (keyLen > 0) {
            if (frame.remaining() < keyLen) {
                return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
            }
            key = new byte[keyLen];
            frame.get(key);
        }

        if (frame.remaining() < 4) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
        }
        int payloadLen = frame.getInt();
        if (payloadLen < 0 || frame.remaining() < payloadLen) {
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
        }
        byte[] payload = new byte[payloadLen];
        frame.get(payload);

        Partition partition = topicManager.route(topicName, key);
        if (partition == null) {
            return ResponseEncoder.produceAck(ErrorCode.TOPIC_NOT_FOUND, -1, -1);
        }

        // Phase 4: reject produce if this broker is not the leader for the partition.
        if (!replicationManager.isLeader(topicName, partition.partitionId)) {
            log.fine("NOT_LEADER for " + topicName + "/" + partition.partitionId);
            return ResponseEncoder.produceAck(ErrorCode.NOT_LEADER, partition.partitionId, -1);
        }

        try {
            long t0     = System.nanoTime();
            long offset = partition.append(payload);
            BrokerMetrics.get().recordAppend(payload.length, System.nanoTime() - t0);
            // Advance the leader end-offset so followers can compute HW correctly.
            replicationManager.updateLeaderEndOffset(
                    topicName, partition.partitionId, partition.writePosition());
            return ResponseEncoder.produceAck(ErrorCode.NONE, partition.partitionId, offset);
        } catch (IOException e) {
            log.severe("Append failed for topic=" + topicName + ": " + e.getMessage());
            return ResponseEncoder.produceAck(ErrorCode.INTERNAL_ERROR, -1, -1);
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
