package com.eventstream.broker;

import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.handler.RequestDispatcher;
import com.eventstream.broker.network.Connection;
import com.eventstream.broker.topic.TopicManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * NIO event loop — Phase 3 (unchanged from Phase 2 structurally).
 *
 * Phase 1 recap: selector thread did everything (accept, read, dispatch, disk I/O, write).
 *
 * Phase 2 split:
 *   Selector thread — accept, read (frame assembly), write (drain write queue).
 *                     No blocking operations. No business logic.
 *
 *   Worker threads  — decode request, call handler (disk I/O), build response.
 *                     Enqueue response and post a PendingChange, then wakeup selector.
 *
 * Crossing the boundary safely:
 *
 *   Selector → Workers: ArrayBlockingQueue<RequestContext>
 *     offer() is non-blocking — if full, OP_READ is suspended for that connection
 *     (TCP flow control propagates back-pressure to the sender for free).
 *
 *   Workers → Selector: ConcurrentLinkedQueue<PendingChange> + selector.wakeup()
 *     Workers enqueue a PendingChange(key, OP_WRITE) and wakeup the selector.
 *     The selector drains this queue at the top of every loop iteration and
 *     applies interest-op changes safely in its own thread.
 *
 *   Workers → Connection.writeQueue: ConcurrentLinkedQueue<ByteBuffer>
 *     Workers call connection.enqueueResponse() (lock-free offer).
 *     The selector calls channel.write() when OP_WRITE fires.
 *
 * SelectionKey.interestOps() is NEVER called from worker threads.
 */
public final class BrokerServer {

    private static final Logger log = Logger.getLogger(BrokerServer.class.getName());

    private final BrokerConfig      config;
    private final RequestDispatcher dispatcher;

    // Selector thread owns these — no external access needed.
    private Selector           selector;
    private ServerSocketChannel serverChannel;
    private volatile boolean    running;

    // ---- Cross-thread structures ----

    // Selector → Workers
    private final ArrayBlockingQueue<RequestContext> requestQueue;

    // Workers → Selector (interest-op mutations)
    private final ConcurrentLinkedQueue<PendingChange> pendingChanges = new ConcurrentLinkedQueue<>();

    // Connections paused due to back-pressure (OP_READ suspended).
    // Accessed only by the selector thread.
    private final Set<SelectionKey> pausedConnections = new HashSet<>();

    private ExecutorService workerPool;

    public BrokerServer(BrokerConfig config, TopicManager topicManager,
                        ConsumerGroupManager groupManager) {
        this.config       = config;
        this.dispatcher   = new RequestDispatcher(topicManager, groupManager);
        this.requestQueue = new ArrayBlockingQueue<>(config.requestQueueCapacity);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    public void start() throws IOException {
        selector      = SelectorProvider.provider().openSelector();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(config.port), 128);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        startWorkers();

        running = true;
        Thread loop = new Thread(this::eventLoop, "broker-event-loop");
        loop.setDaemon(false);
        loop.start();

        log.info("Broker started — port=" + config.port
                + " workers=" + config.workerThreads
                + " queueCap=" + config.requestQueueCapacity);
    }

    public void stop() {
        running = false;
        selector.wakeup();
    }

    // -------------------------------------------------------------------------
    // Worker pool
    // -------------------------------------------------------------------------

    private void startWorkers() {
        AtomicInteger idx = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "broker-worker-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        };
        workerPool = Executors.newFixedThreadPool(config.workerThreads, tf);
        for (int i = 0; i < config.workerThreads; i++) {
            workerPool.submit(new RequestWorker(requestQueue, pendingChanges, selector, dispatcher));
        }
        log.info("Started " + config.workerThreads + " worker threads");
    }

    private void shutdownWorkers() {
        workerPool.shutdownNow(); // interrupt blocking take() calls
        try {
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warning("Worker pool did not terminate in 5 s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Event loop
    // -------------------------------------------------------------------------

    private void eventLoop() {
        while (running) {
            try {
                applyPendingChanges();   // must precede select() — applies OP_WRITE registrations
                resumePausedConnections();

                selector.select();       // blocks until a key is ready or wakeup() is called

                if (!running) break;

                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if (!key.isValid()) continue;
                    try {
                        if (key.isAcceptable())              handleAccept();
                        if (key.isValid() && key.isReadable())  handleRead(key);
                        if (key.isValid() && key.isWritable()) handleWrite(key);
                    } catch (IOException e) {
                        log.warning("I/O error on channel: " + e.getMessage());
                        closeKey(key);
                    }
                }
            } catch (ClosedSelectorException e) {
                break;
            } catch (IOException e) {
                log.severe("Selector error: " + e.getMessage());
            }
        }

        shutdownWorkers();
        closeAll();
        log.info("Event loop terminated");
    }

    // -------------------------------------------------------------------------
    // Pending interest-op changes (workers → selector)
    // -------------------------------------------------------------------------

    private void applyPendingChanges() {
        PendingChange change;
        while ((change = pendingChanges.poll()) != null) {
            SelectionKey k = change.key();
            if (k.isValid()) {
                k.interestOps(k.interestOps() | change.addOps());
            }
        }
    }

    // Resume connections that were paused for back-pressure once the queue
    // drains below 80% capacity.
    private void resumePausedConnections() {
        if (pausedConnections.isEmpty()) return;
        if (requestQueue.size() > config.requestQueueCapacity * 4 / 5) return;

        for (SelectionKey key : pausedConnections) {
            if (key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }
        }
        pausedConnections.clear();
    }

    // -------------------------------------------------------------------------
    // Accept
    // -------------------------------------------------------------------------

    private void handleAccept() throws IOException {
        SocketChannel client = serverChannel.accept();
        if (client == null) return;

        client.configureBlocking(false);
        client.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);

        Connection conn = new Connection(client);
        client.register(selector, SelectionKey.OP_READ, conn);
        log.fine("Client connected: " + client.getRemoteAddress());
    }

    // -------------------------------------------------------------------------
    // Read — frame assembly only; no dispatch
    // -------------------------------------------------------------------------

    private void handleRead(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();

        int bytesRead = conn.channel.read(conn.readBuffer());
        if (bytesRead == -1) { closeKey(key); return; }

        ByteBuffer frame;
        while ((frame = conn.pollFrame()) != null) {
            RequestContext ctx = new RequestContext(key, conn, frame);
            if (!requestQueue.offer(ctx)) {
                // Queue full: suspend reads — TCP flow control does the rest.
                key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                pausedConnections.add(key);
                log.fine("Back-pressure applied to " + conn.channel);
                break;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Write — drain the connection's write queue
    // -------------------------------------------------------------------------

    private void handleWrite(SelectionKey key) throws IOException {
        Connection conn = (Connection) key.attachment();
        var queue = conn.writeQueue();

        ByteBuffer buf;
        while ((buf = queue.peek()) != null) {
            conn.channel.write(buf);
            if (buf.hasRemaining()) break; // socket send buffer full; retry next cycle
            queue.poll();
        }

        if (queue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    private void closeKey(SelectionKey key) {
        key.cancel();
        try { key.channel().close(); } catch (IOException ignored) {}
        pausedConnections.remove(key);
    }

    private void closeAll() {
        for (SelectionKey key : selector.keys()) closeKey(key);
        try { selector.close(); }      catch (IOException ignored) {}
        try { serverChannel.close(); } catch (IOException ignored) {}
    }
}
