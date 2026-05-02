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
 * Synchronous consumer.  Callers drive their own polling loop.
 *
 * Usage:
 *   try (Consumer c = new Consumer("localhost", 9092)) {
 *       long offset = 0;
 *       while (true) {
 *           FetchResult result = c.poll("orders", offset, 1 << 20);
 *           for (byte[] msg : result.messages) { process(msg); }
 *           offset = result.nextOffset;
 *           if (result.isEmpty()) Thread.sleep(50);
 *       }
 *   }
 *
 * Phase 3 will introduce consumer groups with automatic offset management.
 */
public final class Consumer implements Closeable {

    private static final int DEFAULT_MAX_BYTES = 1 << 20; // 1 MiB per fetch

    private final BrokerConnection conn;

    public Consumer(String host, int port) throws IOException {
        conn = new BrokerConnection(host, port);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Fetches up to maxBytes of messages starting at startOffset.
     *
     * Returns an empty FetchResult (with the same startOffset) if no new
     * messages are available — callers should back off before re-polling.
     */
    public FetchResult poll(String topic, long startOffset, int maxBytes) throws IOException {
        conn.send(buildFetchRequest(topic, startOffset, maxBytes));
        ByteBuffer resp = conn.receive();

        byte type  = resp.get();
        byte error = resp.get();
        if (error != ErrorCode.NONE) {
            throw new IOException("poll failed, errorCode=0x" + Integer.toHexString(error & 0xFF));
        }

        int count = resp.getInt();
        if (count == 0) {
            return new FetchResult(Collections.emptyList(), startOffset);
        }

        List<byte[]> messages = new ArrayList<>(count);
        long nextOffset = startOffset;

        for (int i = 0; i < count; i++) {
            long recordOffset = resp.getLong();
            int  msgLen       = resp.getInt();
            byte[] msg        = new byte[msgLen];
            resp.get(msg);
            messages.add(msg);
            // nextOffset = byte position immediately after this record
            nextOffset = recordOffset + 4 + msgLen;
        }

        return new FetchResult(messages, nextOffset);
    }

    public FetchResult poll(String topic, long startOffset) throws IOException {
        return poll(topic, startOffset, DEFAULT_MAX_BYTES);
    }

    @Override
    public void close() throws IOException {
        conn.close();
    }

    // -------------------------------------------------------------------------
    // Request builder
    // -------------------------------------------------------------------------

    private static ByteBuffer buildFetchRequest(String topic, long startOffset, int maxBytes) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        // body = type(1) + topicLen(2) + topic(N) + startOffset(8) + maxBytes(4)
        int bodyLen = 1 + 2 + topicBytes.length + 8 + 4;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);
        buf.put(RequestType.FETCH);
        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putLong(startOffset);
        buf.putInt(maxBytes);
        buf.flip();
        return buf;
    }
}
