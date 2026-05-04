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
}
