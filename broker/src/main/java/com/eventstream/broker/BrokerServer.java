package com.eventstream.broker;

import com.eventstream.broker.handler.RequestDispatcher;
import com.eventstream.broker.network.Connection;
import com.eventstream.broker.topic.TopicManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * Single-threaded NIO event loop.
 *
 * Architecture decisions:
 *
 *  1. One Selector thread handles accept / read / write.  No lock contention
 *     on the hot path; the only blocking operation is the disk write inside
 *     LogSegment, which completes in microseconds (kernel page cache).
 *
 *  2. Per-connection Connection objects are stored as SelectionKey attachments.
 *     This avoids any Map lookup on every I/O event.
 *
 *  3. OP_WRITE interest is enabled only when there is data in the write queue,
 *     and disabled immediately after the queue drains.  This avoids busy-spinning
 *     on writable channels that have nothing to send.
 *
 *  4. TCP_NODELAY is set on accepted sockets.  Our protocol is already
 *     length-prefixed, so Nagle's algorithm only adds latency.
 *
 * Phase 2 will offload request processing to a worker thread pool and use a
 * wake-up queue to feed responses back to the selector thread.
 */
public final class BrokerServer {

    private static final Logger log = Logger.getLogger(BrokerServer.class.getName());

    private final BrokerConfig      config;
    private final RequestDispatcher dispatcher;

    private Selector          selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean  running;

    public BrokerServer(BrokerConfig config, TopicManager topicManager) {
        this.config     = config;
        this.dispatcher = new RequestDispatcher(topicManager);
    }

    public void start() throws IOException {
        selector      = SelectorProvider.provider().openSelector();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(config.port), /* backlog */ 128);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        running = true;
        Thread loop = new Thread(this::eventLoop, "broker-event-loop");
        loop.setDaemon(false);
        loop.start();

        log.info("Broker listening on port " + config.port);
    }

    public void stop() {
        running = false;
        selector.wakeup(); // unblocks selector.select()
    }

    // -------------------------------------------------------------------------
    // Event loop
    // -------------------------------------------------------------------------

    private void eventLoop() {
        while (running) {
            try {
                selector.select(); // block until at least one key is ready
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if (!key.isValid()) continue;

                    try {
                        if (key.isAcceptable()) handleAccept();
                        if (key.isValid() && key.isReadable())  handleRead(key);
                        if (key.isValid() && key.isWritable()) handleWrite(key);
                    } catch (IOException e) {
                        log.warning("I/O error on channel: " + e.getMessage());
                        closeKey(key);
                    }
                }
            } catch (ClosedSelectorException e) {
                break; // normal shutdown
            } catch (IOException e) {
                log.severe("Selector error: " + e.getMessage());
            }
        }

        closeAll();
        log.info("Event loop terminated");
    }

    // -------------------------------------------------------------------------
    // Accept
    // -------------------------------------------------------------------------

    private void handleAccept() throws IOException {
        SocketChannel client = serverChannel.accept();
        if (client == null) return; // spurious wake-up

        client.configureBlocking(false);
        client.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);

        Connection conn = new Connection(client);
        client.register(selector, SelectionKey.OP_READ, conn);
        log.fine("Client connected: " + client.getRemoteAddress());
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    private void handleRead(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();

        int bytesRead = conn.channel.read(conn.readBuffer());
        if (bytesRead == -1) {
            // Client closed the connection gracefully.
            closeKey(key);
            return;
        }

        // There may be multiple complete frames in the buffer — drain them all.
        ByteBuffer frame;
        while ((frame = conn.pollFrame()) != null) {
            ByteBuffer response = dispatcher.dispatch(frame);
            conn.enqueueResponse(response);
        }

        // Register write interest if we have responses to send.
        if (!conn.writeQueue().isEmpty()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    private void handleWrite(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();
        Queue<ByteBuffer> queue = conn.writeQueue();

        while (!queue.isEmpty()) {
            ByteBuffer buf = queue.peek();
            conn.channel.write(buf);
            if (buf.hasRemaining()) {
                // Socket send buffer full; come back when the channel is writable again.
                break;
            }
            queue.poll(); // fully sent — remove from queue
        }

        if (queue.isEmpty()) {
            // No more data to write; stop polling for writability.
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    private void closeKey(SelectionKey key) {
        key.cancel();
        try { key.channel().close(); } catch (IOException ignored) {}
    }

    private void closeAll() {
        for (SelectionKey key : selector.keys()) {
            closeKey(key);
        }
        try { selector.close(); } catch (IOException ignored) {}
        try { serverChannel.close(); } catch (IOException ignored) {}
    }
}
