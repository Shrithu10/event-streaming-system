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
 * Synchronous consumer — Phase 2.
 *
 * Changes from Phase 1:
 *   - poll() now requires an explicit partitionId. The consumer is responsible
 *     for tracking per-partition offsets (a Map<Integer, Long> is the typical
 *     pattern — see ConsumerDemo for an example).
 *   - FetchResult now includes the partitionId.
 *   - pollAll() convenience method fetches from every partition [0..numPartitions)
 *     in one pass.
 *
 * Phase 3 will introduce consumer groups with automatic partition assignment
 * and broker-managed offset tracking.
 */
public final class Consumer implements Closeable {

    private static final int DEFAULT_MAX_BYTES = 1 << 20; // 1 MiB

    private final BrokerConnection conn;

    public Consumer(String host, int port) throws IOException {
        conn = new BrokerConnection(host, port);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Fetches messages from a single partition.
     *
     * @param partitionId  the partition to read from
     * @param startOffset  byte offset to start reading from (0 = beginning)
     * @param maxBytes     max bytes to return in one call
     */
    public FetchResult poll(String topic, int partitionId, long startOffset, int maxBytes)
            throws IOException {
        conn.send(buildFetchRequest(topic, partitionId, startOffset, maxBytes));
        ByteBuffer resp = conn.receive();

        /* type  = */ resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE) {
            throw new IOException("poll failed, error=0x" + Integer.toHexString(error & 0xFF));
        }

        int count = resp.getInt();
        if (count == 0) {
            return new FetchResult(partitionId, Collections.emptyList(), startOffset);
        }

        List<byte[]> messages = new ArrayList<>(count);
        long nextOffset = startOffset;
        for (int i = 0; i < count; i++) {
            long recordOffset = resp.getLong();
            int  msgLen       = resp.getInt();
            byte[] msg        = new byte[msgLen];
            resp.get(msg);
            messages.add(msg);
            nextOffset = recordOffset + 4 + msgLen; // next record starts after this one
        }

        return new FetchResult(partitionId, messages, nextOffset);
    }

    public FetchResult poll(String topic, int partitionId, long startOffset) throws IOException {
        return poll(topic, partitionId, startOffset, DEFAULT_MAX_BYTES);
    }

    /**
     * Polls every partition [0..numPartitions) in sequence and returns all results.
     * Offsets per partition must be supplied by the caller via the offsets array
     * (index == partitionId).
     */
    public List<FetchResult> pollAll(String topic, int numPartitions, long[] offsets)
            throws IOException {
        List<FetchResult> results = new ArrayList<>(numPartitions);
        for (int p = 0; p < numPartitions; p++) {
            results.add(poll(topic, p, offsets[p]));
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        conn.close();
    }

    // -------------------------------------------------------------------------
    // Request builder
    // -------------------------------------------------------------------------

    private static ByteBuffer buildFetchRequest(String topic, int partitionId,
                                                long startOffset, int maxBytes) {
        byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
        // body = type(1) + topicLen(2) + topic + partitionId(4) + startOffset(8) + maxBytes(4)
        int bodyLen = 1 + 2 + tb.length + 4 + 8 + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.FETCH);
        buf.putShort((short) tb.length);
        buf.put(tb);
        buf.putInt(partitionId);
        buf.putLong(startOffset);
        buf.putInt(maxBytes);
        buf.flip();
        return buf;
    }
}
