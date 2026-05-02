package com.eventstream.broker.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Per-connection state attached to a SelectionKey.
 *
 * Read side – frame reassembly:
 *   The NIO layer gives us raw bytes; TCP is a stream, not message-oriented.
 *   readBuf accumulates bytes in write-mode.  pollFrame() flips, extracts one
 *   complete [4-byte-length + body] frame if available, then compacts the
 *   buffer back to write-mode for the next channel.read().
 *
 * Write side – response queue:
 *   Handlers produce fully-framed response ByteBuffers.  They are queued here
 *   and flushed when the channel is writable.  Each buffer must be in read-mode
 *   (flipped) when enqueued.
 */
public final class Connection {

    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024; // 64 MB hard cap

    public final SocketChannel channel;

    // write-mode invariant: position = amount of data accumulated, limit = capacity
    private ByteBuffer readBuf = ByteBuffer.allocate(64 * 1024);

    // pending responses, each in read-mode
    private final Queue<ByteBuffer> writeQueue = new ArrayDeque<>();

    public Connection(SocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Returns the accumulation buffer so the caller can issue channel.read(buf).
     * The buffer is always in write-mode when returned.
     */
    public ByteBuffer readBuffer() {
        return readBuf;
    }

    /**
     * Attempts to extract one complete frame from the accumulation buffer.
     *
     * Invariant maintained: readBuf is always in write-mode after this call.
     *
     * @return the frame body (type byte + payload) in read-mode, or null if
     *         not enough bytes have arrived yet.
     */
    public ByteBuffer pollFrame() throws IOException {
        readBuf.flip(); // switch to read-mode

        if (readBuf.remaining() < 4) {
            readBuf.compact(); // back to write-mode
            return null;
        }

        // Peek at the 4-byte length WITHOUT advancing the position.
        int bodyLen = readBuf.getInt(readBuf.position());

        if (bodyLen < 0 || bodyLen > MAX_FRAME_BYTES) {
            readBuf.compact();
            throw new IOException("Protocol violation: invalid frame body length " + bodyLen);
        }

        if (readBuf.remaining() < 4 + bodyLen) {
            // Grow the buffer if this frame will not fit.
            if (readBuf.capacity() < 4 + bodyLen) {
                ByteBuffer bigger = ByteBuffer.allocate(4 + bodyLen + 1024);
                bigger.put(readBuf); // copies the unread bytes into the new buffer
                readBuf = bigger;    // readBuf is now in write-mode with data at [0..copied)
                // Note: after put(), bigger.position() = amount copied, limit = capacity ✓
            } else {
                readBuf.compact();
            }
            return null;
        }

        // Consume the 4-byte length field.
        readBuf.getInt();

        // Extract the frame body into a fresh buffer.
        byte[] body = new byte[bodyLen];
        readBuf.get(body);
        readBuf.compact(); // compact remaining bytes back to write-mode

        return ByteBuffer.wrap(body); // already in read-mode (wrap() starts at pos=0)
    }

    public void enqueueResponse(ByteBuffer response) {
        writeQueue.add(response);
    }

    public Queue<ByteBuffer> writeQueue() {
        return writeQueue;
    }
}
