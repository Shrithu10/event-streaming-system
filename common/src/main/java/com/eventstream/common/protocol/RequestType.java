package com.eventstream.common.protocol;

public final class RequestType {

    // ---- Requests ----
    public static final byte CREATE_TOPIC   = 0x01;
    public static final byte PRODUCE        = 0x02;
    public static final byte FETCH          = 0x03;

    // ---- Responses (high bit set mirrors the request) ----
    public static final byte CREATE_TOPIC_ACK = (byte) 0x81;
    public static final byte PRODUCE_ACK      = (byte) 0x82;
    public static final byte FETCH_RESPONSE   = (byte) 0x83;

    private RequestType() {}
}
