package com.eventstream.client;

/**
 * Thrown by Consumer methods when the broker signals REBALANCE_IN_PROGRESS.
 *
 * The caller must call joinGroup() again to receive a new assignment before
 * resuming fetch or offset-commit operations.
 */
public final class RebalanceException extends RuntimeException {

    public RebalanceException(String message) {
        super(message);
    }
}
