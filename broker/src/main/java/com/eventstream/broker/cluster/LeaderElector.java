package com.eventstream.broker.cluster;

import java.util.logging.Logger;

/**
 * Promotes this broker to leader for a partition after the current leader is
 * detected as unavailable.
 *
 * Split-brain prevention rule:
 *   Only promote if ALL of the following are true:
 *     1. The failed broker is still recorded as leader (no concurrent promotion).
 *     2. This broker is the FIRST entry in the follower list for the partition.
 *        Follower lists are ordered by brokerId (ascending) in the config file,
 *        so the lowest-numbered live follower always wins the election.
 *
 * In a 2-broker cluster this is sufficient: both brokers may detect the
 * failure, but only one satisfies condition 2.  In a 3+ broker cluster a
 * more sophisticated quorum mechanism (Raft / ZooKeeper) would be needed;
 * Phase 4 accepts this limitation.
 *
 * After promotion:
 *   ClusterMetadata is updated (volatile write visible to all threads).
 *   ReplicationManager stops the fetch thread and initialises leader state.
 *   Other brokers discover the new leader when clients refresh metadata.
 */
public final class LeaderElector {

    private static final Logger log = Logger.getLogger(LeaderElector.class.getName());

    private final int                localBrokerId;
    private final ClusterMetadata    metadata;
    private final ReplicationManager replicationManager;

    public LeaderElector(int localBrokerId, ClusterMetadata metadata,
                         ReplicationManager replicationManager) {
        this.localBrokerId      = localBrokerId;
        this.metadata           = metadata;
        this.replicationManager = replicationManager;
    }

    /**
     * Called by ReplicaFetchThread after FAILURE_THRESHOLD consecutive failures.
     *
     * @param topic          the topic whose leader appears to have failed
     * @param partitionId    the affected partition
     * @param failedLeaderId the brokerId that was the leader when the failure was detected
     */
    public void reportLeaderFailure(String topic, int partitionId, int failedLeaderId) {
        // Guard 1: another broker may have already taken over.
        if (metadata.leaderId(topic, partitionId) != failedLeaderId) {
            log.info("Leader already changed for " + topic + "/" + partitionId
                    + " — skipping election");
            return;
        }

        // Guard 2: only the first follower promotes itself.
        int[] followers = metadata.followerIds(topic, partitionId);
        if (followers.length == 0) {
            log.severe("No follower available for " + topic + "/" + partitionId
                    + " — partition is unavailable");
            return;
        }

        int firstFollower = followers[0];
        if (firstFollower != localBrokerId) {
            log.info("Not the election winner for " + topic + "/" + partitionId
                    + " (winner=" + firstFollower + ") — stopping fetch thread");
            return;
        }

        log.warning("Leader brokerId=" + failedLeaderId + " unreachable for "
                + topic + "/" + partitionId
                + " — promoting self (brokerId=" + localBrokerId + ")");

        metadata.promoteSelf(topic, partitionId);
        replicationManager.onLeaderPromotion(topic, partitionId);
    }
}
