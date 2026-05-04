package com.eventstream.broker.group;

/**
 * Per-member metadata inside a ConsumerGroup.
 *
 * lastHeartbeatMs is volatile so the heartbeat reaper (a separate scheduled
 * thread) can read it without holding any lock.
 *
 * assignedPartitions is written only during rebalance (under rebalanceLock)
 * and read by the member after JOIN_GROUP_ACK — no concurrent access.
 */
public final class ConsumerMember {

    public final String consumerId;
    public final int    sessionTimeoutMs;

    public volatile long  lastHeartbeatMs;
    public volatile int[] assignedPartitions = new int[0];

    public ConsumerMember(String consumerId, int sessionTimeoutMs) {
        this.consumerId       = consumerId;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.lastHeartbeatMs  = System.currentTimeMillis();
    }

    public boolean isAlive(long nowMs) {
        return (nowMs - lastHeartbeatMs) < sessionTimeoutMs;
    }
}
