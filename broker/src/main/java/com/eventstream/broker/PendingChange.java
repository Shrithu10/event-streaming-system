package com.eventstream.broker;

import java.nio.channels.SelectionKey;

/**
 * A deferred SelectionKey interest-op mutation.
 *
 * Worker threads must never touch SelectionKey.interestOps() directly because
 * that method is not safe to call concurrently with Selector.select().
 * Instead, workers enqueue a PendingChange and call selector.wakeup().
 * The selector thread drains this queue at the top of every loop iteration
 * before it calls select() again, applying interest-op changes in the one
 * thread that owns the selector.
 */
public record PendingChange(SelectionKey key, int addOps) {}
