package com.eventstream.common.protocol;

public final class ErrorCode {

    public static final byte NONE            = 0x00;
    public static final byte TOPIC_NOT_FOUND = 0x01;
    public static final byte TOPIC_EXISTS    = 0x02;
    public static final byte INVALID_OFFSET  = 0x03;
    public static final byte INTERNAL_ERROR  = 0x04;

    private ErrorCode() {}
}
