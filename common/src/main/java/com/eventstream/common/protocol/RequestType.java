package com.eventstream.common.protocol;

public final class RequestType {

    // ---- Phase 1 / 2 requests ----
    public static final byte CREATE_TOPIC   = 0x01;
    public static final byte PRODUCE        = 0x02;
    public static final byte FETCH          = 0x03;

    // ---- Phase 3 requests ----
    public static final byte JOIN_GROUP     = 0x04;
    public static final byte LEAVE_GROUP    = 0x05;
    public static final byte HEARTBEAT      = 0x06;
    public static final byte OFFSET_COMMIT  = 0x07;
    public static final byte OFFSET_FETCH   = 0x08;

    // ---- Phase 4 requests ----
    public static final byte REPLICA_FETCH  = 0x09;
    public static final byte METADATA       = 0x0A;

    // ---- Phase 1 / 2 responses ----
    public static final byte CREATE_TOPIC_ACK = (byte) 0x81;
    public static final byte PRODUCE_ACK      = (byte) 0x82;
    public static final byte FETCH_RESPONSE   = (byte) 0x83;

    // ---- Phase 3 responses ----
    public static final byte JOIN_GROUP_ACK     = (byte) 0x84;
    public static final byte LEAVE_GROUP_ACK    = (byte) 0x85;
    public static final byte HEARTBEAT_ACK      = (byte) 0x86;
    public static final byte OFFSET_COMMIT_ACK  = (byte) 0x87;
    public static final byte OFFSET_FETCH_ACK   = (byte) 0x88;

    // ---- Phase 4 responses ----
    public static final byte REPLICA_FETCH_ACK  = (byte) 0x89;
    public static final byte METADATA_ACK       = (byte) 0x8A;

    private RequestType() {}
}
