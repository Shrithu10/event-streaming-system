package com.eventstream.broker.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Per-connection state attached to a SelectionKey.
 *
 * Read side (selector thread only):
 *   readBuf accumulates raw bytes in write-mode. pollFrame() flips, extracts
 *   one complete length-prefixed frame, then compacts back to write-mode.
 *   This is unchanged from Phase 1.
 *
 * Write side (MPSC: multiple worker writers, one selector reader):
 *   Phase 2 changes writeQueue from ArrayDeque to ConcurrentLinkedQueue so
 *   that worker threads can safely enqueue responses while the selector thread
 *   drains them. ConcurrentLinkedQueue is lock-free for the MPSC pattern:
 *   offer() never blocks writers; poll() is called only by the selector.
 */
public final class Connection {

    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    public final SocketChannel channel;

    private ByteBuffer readBuf = ByteBuffer.allocate(64 * 1024);

    // Phase 2: ConcurrentLinkedQueue replaces ArrayDeque for MPSC safety.
    private final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

    public Connection(SocketChannel channel) {
        this.channel = channel;
    }

    public ByteBuffer readBuffer() {
        return readBuf;
    }

    public ByteBuffer pollFrame() throws IOException {
        readBuf.flip();

        if (readBuf.remaining() < 4) {
            readBuf.compact();
            return null;
        }

        int bodyLen = readBuf.getInt(readBuf.position());

        if (bodyLen < 0 || bodyLen > MAX_FRAME_BYTES) {
            readBuf.compact();
            throw new IOException("Protocol violation: bodyLen=" + bodyLen);
        }

        if (readBuf.remaining() < 4 + bodyLen) {
            if (readBuf.capacity() < 4 + bodyLen) {
                ByteBuffer bigger = ByteBuffer.allocate(4 + bodyLen + 1024);
                bigger.put(readBuf);
                readBuf = bigger;
            } else {
                readBuf.compact();
            }
            return null;
        }

        readBuf.getInt();
        byte[] body = new byte[bodyLen];
        readBuf.get(body);
        readBuf.compact();
        return ByteBuffer.wrap(body);
    }

    /** Called by worker threads — thread-safe via ConcurrentLinkedQueue. */
    public void enqueueResponse(ByteBuffer response) {
        writeQueue.offer(response);
    }

    /** Called by the selector thread only. */
    public ConcurrentLinkedQueue<ByteBuffer> writeQueue() {
        return writeQueue;
    }
}
