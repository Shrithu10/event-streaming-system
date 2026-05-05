package com.eventstream.broker.cluster;

/**
 * Leader-side view of a single follower replica.
 *
 * fetchOffset — the byte offset the follower requested in its last REPLICA_FETCH.
 *               Meaning: the follower has durably written every byte up to this offset.
 *               Written by the ReplicaFetchHandler worker thread (single writer per brokerId).
 *               Read by PartitionLeaderState.highWatermark() on the same or other workers.
 *               Volatile is sufficient: one writer, many readers, no compare-and-swap needed.
 *
 * lastContactMs — wall-clock time of the last REPLICA_FETCH received.
 *                 Used by monitoring / future ISR eviction logic.
 */
public final class ReplicaState {

    public final int brokerId;

    private volatile long fetchOffset   = 0L;
    private volatile long lastContactMs;

    public ReplicaState(int brokerId) {
        this.brokerId     = brokerId;
        this.lastContactMs = System.currentTimeMillis();
    }

    public long fetchOffset()   { return fetchOffset; }
    public long lastContactMs() { return lastContactMs; }

    public void updateFetchOffset(long offset) {
        this.fetchOffset    = offset;
        this.lastContactMs  = System.currentTimeMillis();
    }
}
