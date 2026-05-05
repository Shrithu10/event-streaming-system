package com.eventstream.broker.cluster;

import java.util.Map;

/**
 * Leader-side tracking for one partition.
 *
 * leaderEndOffset — byte position of the next record to be written.
 *                   Written only by the single append path (LogSegment.writeLock).
 *                   Read by highWatermark() on worker threads — volatile is sufficient.
 *
 * High Watermark — minimum of leaderEndOffset and every follower's fetchOffset.
 *   HW advances only as fast as the slowest replica.
 *   Consumers see records only up to HW, guaranteeing that any record visible
 *   to a consumer has been durably written on at least one replica.
 *
 * Phase 5+ improvement: maintain an ISR (in-sync replica) set and compute
 *   HW only over ISR members, so a slow/lagging follower does not drag HW down.
 */
public final class PartitionLeaderState {

    private volatile long leaderEndOffset = 0L;

    // brokerId → ReplicaState; immutable after initialisation.
    private final Map<Integer, ReplicaState> replicas;

    public PartitionLeaderState(Map<Integer, ReplicaState> replicas) {
        this.replicas = Map.copyOf(replicas);
    }

    public long leaderEndOffset() { return leaderEndOffset; }

    public void updateLeaderEndOffset(long offset) { leaderEndOffset = offset; }

    /**
     * HW = min(leaderEndOffset, all follower fetchOffsets).
     * Returns leaderEndOffset when there are no followers (single-node or post-failover).
     */
    public long highWatermark() {
        long hw = leaderEndOffset;
        for (ReplicaState r : replicas.values()) hw = Math.min(hw, r.fetchOffset());
        return hw;
    }

    /** Called by ReplicaFetchHandler when a REPLICA_FETCH arrives from a follower. */
    public void onFollowerFetch(int brokerId, long followerFetchOffset) {
        ReplicaState r = replicas.get(brokerId);
        if (r != null) r.updateFetchOffset(followerFetchOffset);
    }

    public Map<Integer, ReplicaState> replicas() { return replicas; }
}
