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
 * Known limitation — no ISR (In-Sync Replica) set:
 *   HW = min(all replicas), so a single slow or lagging follower drags HW down
 *   for all consumers on this partition.  In production, Kafka maintains an ISR:
 *   replicas that have fallen more than replica.lag.time.max.ms behind are
 *   removed from the set, and HW is computed only over ISR members.  This allows
 *   the cluster to maintain full consumer throughput even with a degraded replica.
 *   ISR management is the next step after Phase 4.
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
