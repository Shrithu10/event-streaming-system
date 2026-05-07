package com.eventstream.broker.cluster;

import com.eventstream.common.protocol.ClusterConfig;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Live view of cluster topology for this broker.
 *
 * Initialised from static ClusterConfig at startup.
 * On failover, LeaderElector calls promoteSelf() which atomically updates
 * the leaderMap via ConcurrentHashMap.put() — a single volatile write that
 * is immediately visible to all threads reading isLeader() / leaderId().
 *
 * Single-node mode (isSingleNodeMode() == true):
 *   - isLeader() always returns true.
 *   - isFollower() always returns false.
 *   - Replication is disabled.
 *
 * Known limitation — metadata propagation delay:
 *   promoteSelf() updates in-process memory only.  Other broker instances
 *   do not see the new leader until their clients refresh metadata (triggered
 *   by a NotLeaderException on the next produce/fetch attempt).  There is no
 *   broadcast or gossip protocol: convergence is eventual and driven entirely
 *   by client retries.  This is acceptable for Phase 4's static-cluster model
 *   but would require a distributed metadata store (etcd, ZooKeeper) in a
 *   production system where brokers must have a consistent view of leadership.
 */
public final class ClusterMetadata {

    private final int          localBrokerId;
    private final ClusterConfig config;
    private final boolean      singleNode;

    // topic+partition → current leaderId; updated on failover
    private final ConcurrentHashMap<PartitionKey, Integer> leaderMap   = new ConcurrentHashMap<>();
    // topic+partition → current follower brokerId array; updated on failover
    private final ConcurrentHashMap<PartitionKey, int[]>   followerMap = new ConcurrentHashMap<>();

    public ClusterMetadata(int localBrokerId, ClusterConfig config) {
        this.localBrokerId = localBrokerId;
        this.config        = config;
        this.singleNode    = config.isSingleNodeMode();

        for (ClusterConfig.PartitionAssignment a : config.assignments()) {
            PartitionKey key = new PartitionKey(a.topic(), a.partitionId());
            leaderMap.put(key, a.leaderId());
            followerMap.put(key, a.followerIds());
        }
    }

    public boolean isLeader(String topic, int partitionId) {
        if (singleNode) return true;
        Integer leader = leaderMap.get(new PartitionKey(topic, partitionId));
        return leader != null && leader == localBrokerId;
    }

    public boolean isFollower(String topic, int partitionId) {
        if (singleNode) return false;
        int[] followers = followerMap.getOrDefault(new PartitionKey(topic, partitionId), new int[0]);
        for (int f : followers) if (f == localBrokerId) return true;
        return false;
    }

    /** Returns the current leaderId, or localBrokerId if unknown (single-node fallback). */
    public int leaderId(String topic, int partitionId) {
        return leaderMap.getOrDefault(new PartitionKey(topic, partitionId), localBrokerId);
    }

    public int[] followerIds(String topic, int partitionId) {
        return followerMap.getOrDefault(new PartitionKey(topic, partitionId), new int[0]);
    }

    /**
     * Called by LeaderElector — makes this broker the new leader for the partition.
     * Removes this broker from the follower list and sets it as leader.
     * Visible to all threads after this call returns (ConcurrentHashMap volatile guarantee).
     */
    public void promoteSelf(String topic, int partitionId) {
        PartitionKey key     = new PartitionKey(topic, partitionId);
        int[]        oldFollow = followerMap.getOrDefault(key, new int[0]);
        leaderMap.put(key, localBrokerId);
        followerMap.put(key, removeValue(oldFollow, localBrokerId));
    }

    public int         localBrokerId() { return localBrokerId; }
    public ClusterConfig config()      { return config; }
    public boolean isSingleNodeMode()  { return singleNode; }

    private static int[] removeValue(int[] arr, int value) {
        int count = 0;
        for (int x : arr) if (x != value) count++;
        int[] result = new int[count];
        int   i      = 0;
        for (int x : arr) if (x != value) result[i++] = x;
        return result;
    }
}
