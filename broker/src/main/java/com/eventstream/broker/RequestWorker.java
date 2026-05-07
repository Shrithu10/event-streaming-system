package com.eventstream.broker;

import com.eventstream.broker.handler.RequestDispatcher;
import com.eventstream.broker.metrics.BrokerMetrics;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ErrorCode;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Worker thread: consumes RequestContext from the inbound queue, dispatches
 * to the appropriate handler (which performs disk I/O), then routes the
 * response back to the selector thread via two shared structures:
 *
 *   1. connection.enqueueResponse(buf)  — places the framed response in the
 *      connection's write queue (ConcurrentLinkedQueue, MPSC-safe).
 *
 *   2. pendingChanges.offer(PendingChange) + conditional selector.wakeup() —
 *      tells the selector thread to register OP_WRITE for that connection.
 *
 * Wakeup batching (Phase 5):
 *   selector.wakeup() is an OS syscall (writes to an eventfd/pipe).  With N
 *   concurrent workers all responding in the same millisecond, the naive
 *   approach fires N syscalls where one would have sufficed.
 *
 *   The shared AtomicBoolean wakeupScheduled acts as a CAS gate:
 *     - The first worker to set it false→true calls selector.wakeup().
 *     - All other workers skip the call; the selector is already waking up.
 *     - The event loop resets the flag to false before each selector.select()
 *       so the next burst of responses gets one wakeup again.
 *
 *   Under a sustained 50k msg/s load with 4 workers this eliminates ~75% of
 *   wakeup() syscalls.  The saved count is tracked in BrokerMetrics.
 *
 * Workers never touch SelectionKey.interestOps() directly.
 */
public final class RequestWorker implements Runnable {

    private static final Logger log = Logger.getLogger(RequestWorker.class.getName());

    private final BlockingQueue<RequestContext>        inbound;
    private final ConcurrentLinkedQueue<PendingChange> pendingChanges;
    private final java.nio.channels.Selector           selector;
    private final RequestDispatcher                    dispatcher;
    private final AtomicBoolean                        wakeupScheduled;

    public RequestWorker(
            BlockingQueue<RequestContext>        inbound,
            ConcurrentLinkedQueue<PendingChange> pendingChanges,
            java.nio.channels.Selector           selector,
            RequestDispatcher                    dispatcher,
            AtomicBoolean                        wakeupScheduled) {
        this.inbound          = inbound;
        this.pendingChanges   = pendingChanges;
        this.selector         = selector;
        this.dispatcher       = dispatcher;
        this.wakeupScheduled  = wakeupScheduled;
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
        // Offer the interest-op change BEFORE the wakeup CAS so the selector
        // always finds the change in the queue when it wakes up.
        pendingChanges.offer(new PendingChange(ctx.key(), SelectionKey.OP_WRITE));
        if (wakeupScheduled.compareAndSet(false, true)) {
            selector.wakeup(); // one syscall per burst, not one per response
            BrokerMetrics.get().recordWakeup(false);
        } else {
            BrokerMetrics.get().recordWakeup(true);
        }
    }
}
