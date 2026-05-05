package com.eventstream.broker.network;

import com.eventstream.broker.storage.LogEntry;
import com.eventstream.common.protocol.ClusterConfig;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds fully-framed response ByteBuffers.
 *
 * Wire format for all responses:
 *   [4 bytes: body length][1 byte: response type][1 byte: error code][optional payload]
 */
public final class ResponseEncoder {

    private ResponseEncoder() {}

    // ---- Phase 1 / 2 ----

    public static ByteBuffer createTopicAck(byte errorCode) {
        ByteBuffer buf = ByteBuffer.allocate(4 + 2);
        buf.putInt(2);
        buf.put(RequestType.CREATE_TOPIC_ACK);
        buf.put(errorCode);
        buf.flip();
        return buf;
    }

    public static ByteBuffer produceAck(byte errorCode, int partitionId, long offset) {
        // body = type(1) + error(1) + partitionId(4) + offset(8) = 14
        ByteBuffer buf = ByteBuffer.allocate(4 + 14);
        buf.putInt(14);
        buf.put(RequestType.PRODUCE_ACK);
        buf.put(errorCode);
        buf.putInt(partitionId);
        buf.putLong(offset);
        buf.flip();
        return buf;
    }

    public static ByteBuffer fetchResponse(byte errorCode, List<LogEntry> entries) {
        int payloadBytes = 0;
        for (LogEntry e : entries) payloadBytes += 8 + 4 + e.payload.length;
        int bodyLen = 1 + 1 + 4 + payloadBytes;

        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.FETCH_RESPONSE);
        buf.put(errorCode);
        buf.putInt(entries.size());
        for (LogEntry e : entries) {
            buf.putLong(e.offset);
            buf.putInt(e.payload.length);
            buf.put(e.payload);
        }
        buf.flip();
        return buf;
    }

    // ---- Phase 3 ----

    public static ByteBuffer joinGroupAck(byte errorCode, int generationId, int[] partitions) {
        // body = type(1) + error(1) + generationId(4) + count(4) + partitionId[](4*N)
        int bodyLen = 1 + 1 + 4 + 4 + partitions.length * 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.JOIN_GROUP_ACK);
        buf.put(errorCode);
        buf.putInt(generationId);
        buf.putInt(partitions.length);
        for (int p : partitions) buf.putInt(p);
        buf.flip();
        return buf;
    }

    /** Generic 2-byte body response: type + error. Used by leave, heartbeat, offset-commit. */
    public static ByteBuffer groupAck(byte responseType, byte errorCode) {
        ByteBuffer buf = ByteBuffer.allocate(4 + 2);
        buf.putInt(2);
        buf.put(responseType);
        buf.put(errorCode);
        buf.flip();
        return buf;
    }

    public static ByteBuffer offsetFetchAck(byte errorCode, long offset) {
        // body = type(1) + error(1) + offset(8) = 10
        ByteBuffer buf = ByteBuffer.allocate(4 + 10);
        buf.putInt(10);
        buf.put(RequestType.OFFSET_FETCH_ACK);
        buf.put(errorCode);
        buf.putLong(offset);
        buf.flip();
        return buf;
    }

    public static ByteBuffer errorResponse(byte errorCode) {
        return createTopicAck(errorCode);
    }

    // ---- Phase 4 ----

    /**
     * REPLICA_FETCH_ACK:
     *   type(1) + error(1) + leaderEndOffset(8) + count(4) + [offset(8) + len(4) + payload]*
     */
    public static ByteBuffer replicaFetchAck(byte errorCode, long leaderEndOffset,
                                              List<LogEntry> entries) {
        int payloadBytes = 0;
        for (LogEntry e : entries) payloadBytes += 8 + 4 + e.payload.length;
        int bodyLen = 1 + 1 + 8 + 4 + payloadBytes;

        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.REPLICA_FETCH_ACK);
        buf.put(errorCode);
        buf.putLong(leaderEndOffset);
        buf.putInt(entries.size());
        for (LogEntry e : entries) {
            buf.putLong(e.offset);
            buf.putInt(e.payload.length);
            buf.put(e.payload);
        }
        buf.flip();
        return buf;
    }

    /**
     * METADATA_ACK:
     *   type(1) + error(1)
     *   + brokerCount(4) + [brokerId(4) + hostLen(2) + host + port(4)]*
     *   + assignCount(4) + [topicLen(2) + topic + partitionId(4) + leaderId(4)
     *                        + followerCount(4) + [followerId(4)]*]*
     */
    public static ByteBuffer metadataAck(byte errorCode,
                                          List<ClusterConfig.BrokerInfo> brokers,
                                          List<ClusterConfig.PartitionAssignment> assignments) {
        // Pre-encode strings so we can calculate sizes.
        byte[][] hostBytes  = new byte[brokers.size()][];
        byte[][] topicBytes = new byte[assignments.size()][];
        for (int i = 0; i < brokers.size(); i++)
            hostBytes[i] = brokers.get(i).host().getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < assignments.size(); i++)
            topicBytes[i] = assignments.get(i).topic().getBytes(StandardCharsets.UTF_8);

        int brokerBytes = 0;
        for (byte[] hb : hostBytes) brokerBytes += 4 + 2 + hb.length + 4;
        int assignBytes = 0;
        for (int i = 0; i < assignments.size(); i++) {
            assignBytes += 2 + topicBytes[i].length + 4 + 4 + 4
                    + assignments.get(i).followerIds().length * 4;
        }
        int bodyLen = 1 + 1 + 4 + brokerBytes + 4 + assignBytes;

        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.METADATA_ACK);
        buf.put(errorCode);

        buf.putInt(brokers.size());
        for (int i = 0; i < brokers.size(); i++) {
            ClusterConfig.BrokerInfo b = brokers.get(i);
            buf.putInt(b.brokerId());
            buf.putShort((short) hostBytes[i].length);
            buf.put(hostBytes[i]);
            buf.putInt(b.port());
        }

        buf.putInt(assignments.size());
        for (int i = 0; i < assignments.size(); i++) {
            ClusterConfig.PartitionAssignment a = assignments.get(i);
            buf.putShort((short) topicBytes[i].length);
            buf.put(topicBytes[i]);
            buf.putInt(a.partitionId());
            buf.putInt(a.leaderId());
            buf.putInt(a.followerIds().length);
            for (int fid : a.followerIds()) buf.putInt(fid);
        }

        buf.flip();
        return buf;
    }
}
