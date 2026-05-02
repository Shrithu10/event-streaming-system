package com.eventstream.client;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Thin wrapper around a blocking TCP socket that handles the broker's
 * length-prefixed wire protocol.
 *
 * Clients are single-threaded by design in Phase 1; each Producer/Consumer
 * owns exactly one connection.  Phase 3 will add connection pooling for
 * consumer groups.
 */
public final class BrokerConnection implements Closeable {

    private final Socket           socket;
    private final DataOutputStream out;
    private final DataInputStream  in;

    public BrokerConnection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
        in  = new DataInputStream(new BufferedInputStream(socket.getInputStream(),  64 * 1024));
    }

    /**
     * Sends a fully-framed request.  The ByteBuffer must be in read-mode
     * (flipped) and already include the 4-byte length prefix.
     */
    public void send(ByteBuffer frame) throws IOException {
        if (frame.hasArray()) {
            out.write(frame.array(), frame.arrayOffset() + frame.position(), frame.remaining());
        } else {
            byte[] tmp = new byte[frame.remaining()];
            frame.get(tmp);
            out.write(tmp);
        }
        out.flush();
    }

    /**
     * Reads one response frame and returns the body (WITHOUT the 4-byte length
     * prefix) as a ByteBuffer in read-mode.
     */
    public ByteBuffer receive() throws IOException {
        int bodyLen = in.readInt();
        if (bodyLen < 0 || bodyLen > 64 * 1024 * 1024) {
            throw new IOException("Corrupt response: bodyLen=" + bodyLen);
        }
        byte[] body = new byte[bodyLen];
        in.readFully(body);
        return ByteBuffer.wrap(body);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
