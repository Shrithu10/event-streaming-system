package com.eventstream.common.protocol;

public final class ErrorCode {

    // ---- Phase 1 / 2 ----
    public static final byte NONE            = 0x00;
    public static final byte TOPIC_NOT_FOUND = 0x01;
    public static final byte TOPIC_EXISTS    = 0x02;
    public static final byte INVALID_OFFSET  = 0x03;
    public static final byte INTERNAL_ERROR  = 0x04;

    // ---- Phase 3 ----
    public static final byte UNKNOWN_GROUP          = 0x05;
    public static final byte UNKNOWN_CONSUMER       = 0x06;
    public static final byte REBALANCE_IN_PROGRESS  = 0x07;
    public static final byte WRONG_GENERATION       = 0x08;
    public static final byte NOT_ASSIGNED           = 0x09;

    private ErrorCode() {}
}
