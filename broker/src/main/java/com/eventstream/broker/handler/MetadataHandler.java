package com.eventstream.broker.handler;

import com.eventstream.broker.cluster.ClusterMetadata;
import com.eventstream.broker.cluster.ReplicationManager;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.common.protocol.ClusterConfig;
import com.eventstream.common.protocol.ErrorCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles METADATA requests from clients and follower brokers.
 *
 * Returns the current broker list and partition leader/follower assignments,
 * reflecting any leader promotions that occurred since startup.
 *
 * METADATA request has no payload beyond the type byte.
 *
 * Clients use this to discover which broker is the leader for each partition
 * and to reconnect after a failover.  The response is eventually consistent:
 * if a failover has just occurred, some clients may briefly see stale data
 * until they next refresh.
 */
public final class MetadataHandler {

    private final ReplicationManager replicationManager;

    public MetadataHandler(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    public ByteBuffer handle(ByteBuffer frame) {
        ClusterMetadata metadata = replicationManager.metadata();
        ClusterConfig   config   = metadata.config();

        List<ClusterConfig.BrokerInfo> brokers = config.brokers();

        // Build assignments with live leader data (post-failover).
        List<ClusterConfig.PartitionAssignment> live = new ArrayList<>(config.assignments().size());
        for (ClusterConfig.PartitionAssignment a : config.assignments()) {
            live.add(new ClusterConfig.PartitionAssignment(
                    a.topic(),
                    a.partitionId(),
                    metadata.leaderId(a.topic(), a.partitionId()),
                    metadata.followerIds(a.topic(), a.partitionId())));
        }

        return ResponseEncoder.metadataAck(ErrorCode.NONE, brokers, live);
    }
}
