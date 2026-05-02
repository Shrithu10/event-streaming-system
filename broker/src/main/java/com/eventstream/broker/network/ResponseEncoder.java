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
 */
public final class ResponseEncoder {

    private ResponseEncoder() {}

    public static ByteBuffer createTopicAck(byte errorCode) {
        // body = type(1) + error(1) = 2 bytes
        ByteBuffer buf = ByteBuffer.allocate(4 + 2);
        buf.putInt(2);
        buf.put(RequestType.CREATE_TOPIC_ACK);
        buf.put(errorCode);
        buf.flip();
        return buf;
    }

    public static ByteBuffer produceAck(byte errorCode, long offset) {
        // body = type(1) + error(1) + offset(8) = 10 bytes
        ByteBuffer buf = ByteBuffer.allocate(4 + 10);
        buf.putInt(10);
        buf.put(RequestType.PRODUCE_ACK);
        buf.put(errorCode);
        buf.putLong(offset);
        buf.flip();
        return buf;
    }

    public static ByteBuffer fetchResponse(byte errorCode, List<LogEntry> entries) {
        // body = type(1) + error(1) + count(4) + sum(offset(8)+len(4)+payload)
        int payloadBytes = 0;
        for (LogEntry e : entries) {
            payloadBytes += 8 + 4 + e.payload.length;
        }
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

    /** Generic error response when we cannot determine a more specific type. */
    public static ByteBuffer errorResponse(byte errorCode) {
        // Reuse createTopicAck shape; the client will read error code regardless of type.
        return createTopicAck(errorCode);
    }
}
