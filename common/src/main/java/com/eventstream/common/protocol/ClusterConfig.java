package com.eventstream.common.protocol;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Static cluster topology — shared by broker and client.
 *
 * Config file format (Java properties):
 *   broker.1=localhost:9092
 *   broker.2=localhost:9093
 *   assign.orders.0=1:2          # topic "orders", partition 0: leader=1, follower=2
 *   assign.orders.1=2:1          # topic "orders", partition 1: leader=2, follower=1
 *
 * Rules:
 *   - Topic names may not contain dots (they are used as key separators).
 *   - Multiple followers: assign.topic.N=leaderId:followerId1,followerId2
 *   - No followers: assign.topic.N=leaderId   or   assign.topic.N=leaderId:
 *
 * Single-node mode: pass no assignments (isSingleNodeMode() == true).
 *   In this mode the broker is always the leader for every partition and
 *   replication is disabled.
 */
public final class ClusterConfig {

    public record BrokerInfo(int brokerId, String host, int port) {
        @Override public String toString() { return brokerId + "@" + host + ":" + port; }
    }

    public record PartitionAssignment(
            String topic, int partitionId, int leaderId, int[] followerIds) {}

    private final List<BrokerInfo>          brokers;
    private final List<PartitionAssignment> assignments;

    private ClusterConfig(List<BrokerInfo> brokers, List<PartitionAssignment> assignments) {
        this.brokers     = Collections.unmodifiableList(new ArrayList<>(brokers));
        this.assignments = Collections.unmodifiableList(new ArrayList<>(assignments));
    }

    public List<BrokerInfo>          brokers()     { return brokers; }
    public List<PartitionAssignment> assignments() { return assignments; }
    public boolean isSingleNodeMode()              { return assignments.isEmpty(); }

    public Optional<BrokerInfo> broker(int brokerId) {
        return brokers.stream().filter(b -> b.brokerId() == brokerId).findFirst();
    }

    public Optional<PartitionAssignment> assignment(String topic, int partitionId) {
        return assignments.stream()
                .filter(a -> a.topic().equals(topic) && a.partitionId() == partitionId)
                .findFirst();
    }

    // -------------------------------------------------------------------------
    // Factories
    // -------------------------------------------------------------------------

    public static ClusterConfig fromFile(Path path) throws IOException {
        Properties props = new Properties();
        try (var reader = Files.newBufferedReader(path)) {
            props.load(reader);
        }

        List<BrokerInfo>          brokers     = new ArrayList<>();
        List<PartitionAssignment> assignments = new ArrayList<>();

        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key).trim();

            if (key.startsWith("broker.")) {
                int      brokerId = Integer.parseInt(key.substring("broker.".length()).trim());
                String[] hp       = value.split(":");
                brokers.add(new BrokerInfo(brokerId, hp[0].trim(), Integer.parseInt(hp[1].trim())));

            } else if (key.startsWith("assign.")) {
                // assign.TOPIC.PARTITION_ID=LEADER_ID[:FOLLOWER_ID1,FOLLOWER_ID2,...]
                String rest    = key.substring("assign.".length());
                int    lastDot = rest.lastIndexOf('.');
                String topic   = rest.substring(0, lastDot);
                int    partId  = Integer.parseInt(rest.substring(lastDot + 1).trim());

                String[] val      = value.split(":", 2);
                int      leaderId = Integer.parseInt(val[0].trim());
                int[]    followers;
                if (val.length > 1 && !val[1].trim().isEmpty()) {
                    followers = Arrays.stream(val[1].trim().split(","))
                                      .mapToInt(s -> Integer.parseInt(s.trim()))
                                      .toArray();
                } else {
                    followers = new int[0];
                }
                assignments.add(new PartitionAssignment(topic, partId, leaderId, followers));
            }
        }
        return new ClusterConfig(brokers, assignments);
    }

    /** Single-node mode: no cluster config file required. */
    public static ClusterConfig singleNode(int brokerId, String host, int port) {
        return new ClusterConfig(List.of(new BrokerInfo(brokerId, host, port)), List.of());
    }

    /** Build from in-memory lists (used by MetadataClient after parsing METADATA_ACK). */
    public static ClusterConfig of(List<BrokerInfo> brokers, List<PartitionAssignment> assignments) {
        return new ClusterConfig(brokers, assignments);
    }
}
