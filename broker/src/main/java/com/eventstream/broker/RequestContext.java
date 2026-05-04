package com.eventstream.broker;

import com.eventstream.broker.network.Connection;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

/**
 * Unit of work passed from the selector thread to a worker thread.
 *
 * The frame is a complete, decoded frame body (type byte + payload) owned
 * exclusively by this context after handoff — workers do not need to copy it.
 *
 * key and connection are needed by the worker so it can route the response
 * back through the selector's pending-change queue.
 */
public record RequestContext(
        SelectionKey key,
        Connection   connection,
        ByteBuffer   frame
) {}
