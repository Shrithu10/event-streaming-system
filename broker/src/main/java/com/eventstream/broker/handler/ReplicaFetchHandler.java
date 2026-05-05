package com.eventstream.broker.handler;

import com.eventstream.broker.cluster.ReplicationManager;
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

/**
 * Handles REPLICA_FETCH requests from follower brokers.
 *
 * Wire format (after the type byte):
 *   [2B topicLen][N topic][4B partitionId][4B followerBrokerId][8B fetchOffset][4B maxBytes]
 *
 * This handler:
 *   1. Verifies this broker is the leader (returns NOT_LEADER otherwise).
 *   2. Records the follower's current fetch position so HW can advance.
 *   3. Reads records from the local log starting at fetchOffset.
 *   4. Returns records + leaderEndOffset (so follower can detect when caught up).
 *
 * Called exclusively from worker threads — never from the selector thread.
 */
public final class ReplicaFetchHandler {

    private static final Logger log = Logger.getLogger(ReplicaFetchHandler.class.getName());

    private final TopicManager       topicManager;
    private final ReplicationManager replicationManager;

    public ReplicaFetchHandler(TopicManager topicManager, ReplicationManager replicationManager) {
        this.topicManager       = topicManager;
        this.replicationManager = replicationManager;
    }

    public ByteBuffer handle(ByteBuffer frame) {
        if (frame.remaining() < 2) {
            return ResponseEncoder.replicaFetchAck(ErrorCode.INTERNAL_ERROR, 0L, Collections.emptyList());
        }

        int    topicLen = frame.getShort() & 0xFFFF;
        // partitionId(4) + followerBrokerId(4) + fetchOffset(8) + maxBytes(4) = 20 bytes
        if (frame.remaining() < topicLen + 20) {
            return ResponseEncoder.replicaFetchAck(ErrorCode.INTERNAL_ERROR, 0L, Collections.emptyList());
        }

        byte[] tb = new byte[topicLen];
        frame.get(tb);
        String topic            = new String(tb, StandardCharsets.UTF_8);
        int    partitionId      = frame.getInt();
        int    followerBrokerId = frame.getInt();
        long   fetchOffset      = frame.getLong();
        int    maxBytes         = frame.getInt();

        if (!replicationManager.isLeader(topic, partitionId)) {
            log.fine("REPLICA_FETCH rejected — not leader for " + topic + "/" + partitionId);
            return ResponseEncoder.replicaFetchAck(ErrorCode.NOT_LEADER, 0L, Collections.emptyList());
        }

        Partition partition = topicManager.getPartition(topic, partitionId);
        if (partition == null) {
            return ResponseEncoder.replicaFetchAck(ErrorCode.TOPIC_NOT_FOUND, 0L, Collections.emptyList());
        }

        // Record the follower's current position BEFORE reading so HW advances
        // even when the response is empty (follower is caught up).
        replicationManager.onFollowerFetch(topic, partitionId, followerBrokerId, fetchOffset);

        try {
            List<LogEntry> entries       = partition.fetch(fetchOffset, maxBytes);
            long           leaderEndOffset = partition.writePosition();
            return ResponseEncoder.replicaFetchAck(ErrorCode.NONE, leaderEndOffset, entries);
        } catch (IOException e) {
            log.severe("Replica fetch read failed " + topic + "/" + partitionId + ": " + e.getMessage());
            return ResponseEncoder.replicaFetchAck(ErrorCode.INTERNAL_ERROR, 0L, Collections.emptyList());
        }
    }
}
