package com.eventstream.client;

import java.io.IOException;

/**
 * Thrown when the broker returns NOT_LEADER for a PRODUCE or FETCH request.
 * The caller should refresh cluster metadata and retry against the current leader.
 */
public final class NotLeaderException extends IOException {

    public NotLeaderException(String message) {
        super(message);
    }
}
