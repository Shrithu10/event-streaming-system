package com.eventstream.client;

import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Synchronous producer.  Each send() call blocks until the broker has
 * persisted the message and returned an ACK with the assigned offset.
 *
 * Usage:
 *   try (Producer p = new Producer("localhost", 9092)) {
 *       p.createTopic("orders");
 *       long offset = p.send("orders", "hello".getBytes());
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

    /**
     * Creates a topic on the broker.  Safe to call if the topic already exists
     * (TOPIC_EXISTS is treated as success so callers can be idempotent).
     */
    public void createTopic(String topic) throws IOException {
        conn.send(buildCreateTopicRequest(topic));
        ByteBuffer resp = conn.receive();
        byte type  = resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE && error != ErrorCode.TOPIC_EXISTS) {
            throw new IOException("createTopic failed, errorCode=0x" + Integer.toHexString(error & 0xFF));
        }
    }

    /**
     * Sends a message to the given topic.
     *
     * @return the byte offset assigned to this message in the partition log
     */
    public long send(String topic, byte[] message) throws IOException {
        conn.send(buildProduceRequest(topic, message));
        ByteBuffer resp = conn.receive();
        byte type  = resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE) {
            throw new IOException("send failed, errorCode=0x" + Integer.toHexString(error & 0xFF));
        }
        return resp.getLong();
    }

    public long send(String topic, String message) throws IOException {
        return send(topic, message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        conn.close();
    }

    // -------------------------------------------------------------------------
    // Request builders
    // -------------------------------------------------------------------------

    private static ByteBuffer buildCreateTopicRequest(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        // body = type(1) + topicLen(2) + topic(N)
        int bodyLen = 1 + 2 + topicBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.CREATE_TOPIC);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.flip();
        return buf;
    }

    private static ByteBuffer buildProduceRequest(String topic, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        // body = type(1) + topicLen(2) + topic(N) + msgLen(4) + msg(M)
        int bodyLen = 1 + 2 + topicBytes.length + 4 + message.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.PRODUCE);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(message.length);
        buf.put(message);
        buf.flip();
        return buf;
    }
}
