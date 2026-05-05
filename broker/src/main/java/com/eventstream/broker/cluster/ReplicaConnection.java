package com.eventstream.broker.cluster;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Blocking TCP connection used exclusively by ReplicaFetchThread to pull
 * records from the leader broker.
 *
 * Wire format mirrors the client BrokerConnection: a 4-byte big-endian length
 * prefix followed by the frame body.  Runs on a dedicated background thread,
 * so blocking I/O is acceptable here.
 */
public final class ReplicaConnection implements Closeable {

    private final Socket           socket;
    private final DataInputStream  in;
    private final DataOutputStream out;

    public ReplicaConnection(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(5_000); // 5-second read timeout; triggers failure detection
        this.in  = new DataInputStream(new BufferedInputStream(socket.getInputStream(),  65536));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
    }

    public void send(ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        out.write(bytes);
        out.flush();
    }

    public ByteBuffer receive() throws IOException {
        int    len  = in.readInt();
        byte[] body = new byte[len];
        in.readFully(body);
        return ByteBuffer.wrap(body);
    }

    @Override
    public void close() {
        try { socket.close(); } catch (IOException ignored) {}
    }
}
