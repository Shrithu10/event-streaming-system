package com.eventstream.broker.network;

import com.eventstream.broker.storage.LogEntry;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Builds fully-framed response ByteBuffers.
 *
 * Wire format for all responses:
 *   [4 bytes: body length][1 byte: response type][1 byte: error code][optional payload]
 *
 * Phase 2 change: produceAck now includes the partitionId so producers know
 * which partition accepted their message (important for key-based routing).
 *
 *   PRODUCE_ACK body: type(1) + error(1) + partitionId(4) + offset(8) = 14 bytes
 */
public final class ResponseEncoder {

    private ResponseEncoder() {}

    public static ByteBuffer createTopicAck(byte errorCode) {
        // body = type(1) + error(1) = 2
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
        // body = type(1) + error(1) + count(4) + per-entry: offset(8)+len(4)+payload
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

    public static ByteBuffer errorResponse(byte errorCode) {
        return createTopicAck(errorCode);
    }
}
