package com.eventstream.broker;

import com.eventstream.broker.handler.RequestDispatcher;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Worker thread: consumes RequestContext from the inbound queue, dispatches
 * to the appropriate handler (which performs disk I/O), then routes the
 * response back to the selector thread via two shared structures:
 *
 *   1. connection.enqueueResponse(buf)  — places the framed response in the
 *      connection's write queue (ConcurrentLinkedQueue, MPSC-safe).
 *
 *   2. pendingChanges.offer(PendingChange) + selector.wakeup() — tells the
 *      selector thread to register OP_WRITE for that connection on its next
 *      loop iteration.
 *
 * Workers never touch SelectionKey.interestOps() directly.
 */
public final class RequestWorker implements Runnable {

    private static final Logger log = Logger.getLogger(RequestWorker.class.getName());

    private final BlockingQueue<RequestContext>        inbound;
    private final ConcurrentLinkedQueue<PendingChange> pendingChanges;
    private final java.nio.channels.Selector           selector;
    private final RequestDispatcher                    dispatcher;

    public RequestWorker(
            BlockingQueue<RequestContext>        inbound,
            ConcurrentLinkedQueue<PendingChange> pendingChanges,
            java.nio.channels.Selector           selector,
            RequestDispatcher                    dispatcher) {
        this.inbound        = inbound;
        this.pendingChanges = pendingChanges;
        this.selector       = selector;
        this.dispatcher     = dispatcher;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            RequestContext ctx = null;
            try {
                ctx = inbound.take(); // blocks until work is available
                ByteBuffer response = dispatcher.dispatch(ctx.frame());
                sendResponse(ctx, response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore flag and exit
                break;
            } catch (Exception e) {
                log.severe("Worker error: " + e.getMessage());
                if (ctx != null) {
                    // Send an error response so the client isn't left waiting forever.
                    sendResponse(ctx, ResponseEncoder.errorResponse(ErrorCode.INTERNAL_ERROR));
                }
            }
        }
    }

    private void sendResponse(RequestContext ctx, ByteBuffer response) {
        ctx.connection().enqueueResponse(response);
        pendingChanges.offer(new PendingChange(ctx.key(), SelectionKey.OP_WRITE));
        selector.wakeup(); // O(1): writes one byte to an eventfd/pipe on Linux
    }
}
