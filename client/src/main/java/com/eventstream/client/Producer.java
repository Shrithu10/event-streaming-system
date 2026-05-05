package com.eventstream.client;

import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Synchronous producer — Phase 2.
 *
 * Changes from Phase 1:
 *   - createTopic() now accepts a numPartitions argument.
 *   - send() accepts an optional routing key; the broker hashes it to select
 *     the target partition (murmur2(key) % numPartitions).
 *   - PRODUCE_ACK now includes the partitionId the broker assigned.
 *
 * Usage:
 *   try (Producer p = new Producer("localhost", 9092)) {
 *       p.createTopic("orders", 4);
 *       SendResult r = p.send("orders", "user-42".getBytes(), payload);
 *       System.out.println("partition=" + r.partitionId + " offset=" + r.offset);
 *   }
 */
public final class Producer implements Closeable {

    private final BrokerConnection conn;

    public Producer(String host, int port) throws IOException {
        conn = new BrokerConnection(host, port);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public void createTopic(String topic, int numPartitions) throws IOException {
        conn.send(buildCreateTopicRequest(topic, numPartitions));
        ByteBuffer resp = conn.receive();
        /* type  = */ resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE && error != ErrorCode.TOPIC_EXISTS) {
            throw new IOException("createTopic failed, error=0x" + Integer.toHexString(error & 0xFF));
        }
    }

    /** Creates a single-partition topic (backward-compatible with Phase 1). */
    public void createTopic(String topic) throws IOException {
        createTopic(topic, 1);
    }

    /**
     * Sends a message with a routing key.
     * The broker selects the partition via murmur2(key) % numPartitions.
     */
    public SendResult send(String topic, byte[] key, byte[] message) throws IOException {
        conn.send(buildProduceRequest(topic, key, message));
        ByteBuffer resp = conn.receive();
        /* type      = */ resp.get();
        byte error     = resp.get();
        if (error == ErrorCode.NOT_LEADER) {
            // Consume the remaining partitionId + offset fields so the connection stays in sync.
            if (resp.remaining() >= 12) { resp.getInt(); resp.getLong(); }
            throw new NotLeaderException("Broker is not the leader — refresh metadata and retry");
        }
        if (error != ErrorCode.NONE) {
            throw new IOException("send failed, error=0x" + Integer.toHexString(error & 0xFF));
        }
        int  partitionId = resp.getInt();
        long offset      = resp.getLong();
        return new SendResult(partitionId, offset);
    }

    /** Sends a keyless message; the broker uses round-robin partition selection. */
    public SendResult send(String topic, byte[] message) throws IOException {
        return send(topic, null, message);
    }

    public SendResult send(String topic, String key, String message) throws IOException {
        return send(topic,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        conn.close();
    }

    // -------------------------------------------------------------------------
    // Request builders
    // -------------------------------------------------------------------------

    private static ByteBuffer buildCreateTopicRequest(String topic, int numPartitions) {
        byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 2 + tb.length + 4; // type + topicLen + topic + numPartitions
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.CREATE_TOPIC);
        buf.putShort((short) tb.length);
        buf.put(tb);
        buf.putInt(numPartitions);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildProduceRequest(String topic, byte[] key, byte[] message) {
        byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
        int keyLen = (key == null || key.length == 0) ? -1 : key.length;
        // body = type(1) + topicLen(2) + topic + keyLen(4) + key? + payloadLen(4) + payload
        int bodyLen = 1 + 2 + tb.length + 4 + (keyLen > 0 ? keyLen : 0) + 4 + message.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.PRODUCE);
        buf.putShort((short) tb.length);
        buf.put(tb);
        buf.putInt(keyLen);
        if (keyLen > 0) buf.put(key);
        buf.putInt(message.length);
        buf.put(message);
        buf.flip();
        return buf;
    }

    // -------------------------------------------------------------------------
    // Result type
    // -------------------------------------------------------------------------

    public record SendResult(int partitionId, long offset) {}
}
