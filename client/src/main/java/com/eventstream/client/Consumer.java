package com.eventstream.client;

import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Synchronous consumer — Phase 3.
 *
 * Phase 3 additions:
 *   - joinGroup()     — registers with a consumer group; returns assigned partition ids
 *   - pollGroup()     — group-aware fetch; validates ownership, uses committed offset
 *   - commitOffset()  — commits the processed-up-to offset for a partition
 *   - heartbeat()     — must be called periodically to stay alive in the group
 *   - leaveGroup()    — graceful deregistration (triggers rebalance for remaining members)
 *   - fetchOffset()   — retrieves the last committed offset from the broker (used on restart)
 *
 * Plain fetch (no group) is still supported via poll(topic, partitionId, offset).
 *
 * Heartbeat note:
 *   Heartbeats share the same connection as fetches. The caller must ensure
 *   heartbeat() is invoked at least once every sessionTimeoutMs / 3 milliseconds.
 *   The ConsumerGroupDemo shows the recommended pattern: heartbeat before each
 *   poll loop iteration.
 *
 * Rebalance handling:
 *   When any group-aware method throws RebalanceException, the caller must
 *   call joinGroup() again to receive updated assignments before continuing.
 */
public final class Consumer implements Closeable {

    private static final int DEFAULT_MAX_BYTES = 1 << 20; // 1 MiB

    private final BrokerConnection conn;

    // Group state — null when not in a group
    private String groupId;
    private String consumerId;
    private int    generationId;
    private int[]  assignedPartitions = new int[0];

    // In-memory per-partition offset cursor (advanced after each successful poll)
    private final long[] partitionOffsets = new long[1024]; // max 1024 partitions per topic

    public Consumer(String host, int port) throws IOException {
        conn = new BrokerConnection(host, port);
    }

    // -------------------------------------------------------------------------
    // Group management
    // -------------------------------------------------------------------------

    /**
     * Joins a consumer group.  Triggers a rebalance on the broker side and
     * returns this consumer's newly assigned partition ids.
     *
     * Fetches the last committed offset for each assigned partition so that
     * pollGroup() resumes from where processing last left off.
     */
    public int[] joinGroup(String groupId, String consumerId,
                           String topic, int sessionTimeoutMs) throws IOException {
        this.groupId    = groupId;
        this.consumerId = consumerId;

        conn.send(buildJoinGroupRequest(groupId, consumerId, topic, sessionTimeoutMs));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE) {
            throw new IOException("joinGroup failed, error=0x" + hex(error));
        }
        this.generationId       = resp.getInt();
        int count               = resp.getInt();
        this.assignedPartitions = new int[count];
        for (int i = 0; i < count; i++) assignedPartitions[i] = resp.getInt();

        // Restore per-partition offsets from the last committed position.
        for (int p : assignedPartitions) {
            long committed = fetchOffset(groupId, p);
            partitionOffsets[p] = (committed == -1) ? 0L : committed;
        }
        return assignedPartitions;
    }

    public void leaveGroup() throws IOException {
        if (groupId == null) return;
        conn.send(buildLeaveGroupRequest(groupId, consumerId));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE && error != ErrorCode.UNKNOWN_CONSUMER) {
            throw new IOException("leaveGroup failed, error=0x" + hex(error));
        }
        groupId    = null;
        consumerId = null;
    }

    /**
     * Sends a heartbeat to keep this consumer alive in its group.
     *
     * @throws RebalanceException if the broker has rebalanced since the last joinGroup().
     *         The caller must then call joinGroup() again.
     */
    public void heartbeat() throws IOException {
        if (groupId == null) return;
        conn.send(buildHeartbeatRequest(groupId, consumerId, generationId));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error == ErrorCode.REBALANCE_IN_PROGRESS) {
            throw new RebalanceException("Rebalance in progress — call joinGroup() again");
        }
        if (error != ErrorCode.NONE) {
            throw new IOException("heartbeat failed, error=0x" + hex(error));
        }
    }

    // -------------------------------------------------------------------------
    // Group-aware fetch
    // -------------------------------------------------------------------------

    /**
     * Fetches from a partition the consumer owns within its group.
     * Uses the internal per-partition cursor (advanced by commitOffset).
     *
     * @throws RebalanceException if ownership has changed.
     */
    public FetchResult pollGroup(String topic, int partitionId) throws IOException {
        return pollGroup(topic, partitionId, DEFAULT_MAX_BYTES);
    }

    public FetchResult pollGroup(String topic, int partitionId, int maxBytes) throws IOException {
        long offset = partitionOffsets[partitionId];
        conn.send(buildFetchRequest(topic, groupId, consumerId, generationId,
                partitionId, offset, maxBytes));
        return parseFetchResponse(partitionId);
    }

    /**
     * Polls all assigned partitions in one pass.
     * Stops and re-throws RebalanceException on the first partition that triggers it.
     */
    public List<FetchResult> pollGroupAll(String topic) throws IOException {
        return pollGroupAll(topic, DEFAULT_MAX_BYTES);
    }

    public List<FetchResult> pollGroupAll(String topic, int maxBytes) throws IOException {
        List<FetchResult> results = new ArrayList<>(assignedPartitions.length);
        for (int p : assignedPartitions) {
            results.add(pollGroup(topic, p, maxBytes));
        }
        return results;
    }

    // -------------------------------------------------------------------------
    // Offset management
    // -------------------------------------------------------------------------

    /**
     * Commits the given offset for a partition and advances the internal cursor.
     * Call this after successfully processing the messages from pollGroup().
     *
     * @throws RebalanceException if the generation has changed.
     */
    public void commitOffset(int partitionId, long offset) throws IOException {
        conn.send(buildOffsetCommitRequest(groupId, consumerId, generationId, partitionId, offset));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error == ErrorCode.REBALANCE_IN_PROGRESS) {
            throw new RebalanceException("Rebalance in progress — call joinGroup() again");
        }
        if (error != ErrorCode.NONE) {
            throw new IOException("commitOffset failed, error=0x" + hex(error));
        }
        partitionOffsets[partitionId] = offset;
    }

    /** Fetches the last committed offset for a partition from the broker. */
    public long fetchOffset(String groupId, int partitionId) throws IOException {
        conn.send(buildOffsetFetchRequest(groupId, consumerId, partitionId));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE && error != ErrorCode.UNKNOWN_GROUP) {
            throw new IOException("fetchOffset failed, error=0x" + hex(error));
        }
        return resp.getLong(); // -1 if no committed offset yet
    }

    // -------------------------------------------------------------------------
    // Plain (non-group) fetch — backward-compatible with Phase 2
    // -------------------------------------------------------------------------

    public FetchResult poll(String topic, int partitionId, long startOffset, int maxBytes)
            throws IOException {
        conn.send(buildFetchRequest(topic, "", "", 0, partitionId, startOffset, maxBytes));
        return parseFetchResponse(partitionId);
    }

    public FetchResult poll(String topic, int partitionId, long startOffset) throws IOException {
        return poll(topic, partitionId, startOffset, DEFAULT_MAX_BYTES);
    }

    // -------------------------------------------------------------------------
    // Response parsing
    // -------------------------------------------------------------------------

    private FetchResult parseFetchResponse(int partitionId) throws IOException {
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error == ErrorCode.REBALANCE_IN_PROGRESS) {
            throw new RebalanceException("Rebalance in progress — call joinGroup() again");
        }
        if (error != ErrorCode.NONE) {
            throw new IOException("fetch failed, error=0x" + hex(error));
        }

        int count = resp.getInt();
        if (count == 0) {
            return new FetchResult(partitionId, Collections.emptyList(),
                    partitionOffsets[partitionId]);
        }

        List<byte[]> messages = new ArrayList<>(count);
        long nextOffset = partitionOffsets[partitionId];
        for (int i = 0; i < count; i++) {
            long recordOffset = resp.getLong();
            int  msgLen       = resp.getInt();
            byte[] msg        = new byte[msgLen];
            resp.get(msg);
            messages.add(msg);
            nextOffset = recordOffset + 4 + msgLen;
        }
        return new FetchResult(partitionId, messages, nextOffset);
    }

    @Override
    public void close() throws IOException {
        try { if (groupId != null) leaveGroup(); } catch (IOException ignored) {}
        conn.close();
    }

    // -------------------------------------------------------------------------
    // Request builders
    // -------------------------------------------------------------------------

    private static ByteBuffer buildJoinGroupRequest(String groupId, String consumerId,
                                                     String topic, int sessionTimeoutMs) {
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+gb.length + 2+cb.length + 2+tb.length + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.JOIN_GROUP);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.putShort((short) tb.length); buf.put(tb);
        buf.putInt(sessionTimeoutMs);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildLeaveGroupRequest(String groupId, String consumerId) {
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+gb.length + 2+cb.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.LEAVE_GROUP);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildHeartbeatRequest(String groupId, String consumerId,
                                                     int generationId) {
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+gb.length + 2+cb.length + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.HEARTBEAT);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.putInt(generationId);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildFetchRequest(String topic, String groupId, String consumerId,
                                                int generationId, int partitionId,
                                                long startOffset, int maxBytes) {
        byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+tb.length + 2+gb.length + 2+cb.length + 4 + 4 + 8 + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.FETCH);
        buf.putShort((short) tb.length); buf.put(tb);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.putInt(generationId);
        buf.putInt(partitionId);
        buf.putLong(startOffset);
        buf.putInt(maxBytes);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildOffsetCommitRequest(String groupId, String consumerId,
                                                        int generationId,
                                                        int partitionId, long offset) {
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+gb.length + 2+cb.length + 4 + 4 + 8;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.OFFSET_COMMIT);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.putInt(generationId);
        buf.putInt(partitionId);
        buf.putLong(offset);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildOffsetFetchRequest(String groupId, String consumerId,
                                                       int partitionId) {
        byte[] gb = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] cb = consumerId.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2+gb.length + 2+cb.length + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.OFFSET_FETCH);
        buf.putShort((short) gb.length); buf.put(gb);
        buf.putShort((short) cb.length); buf.put(cb);
        buf.putInt(partitionId);
        buf.flip();
        return buf;
    }

    private static String hex(byte b) {
        return Integer.toHexString(b & 0xFF);
    }
}
