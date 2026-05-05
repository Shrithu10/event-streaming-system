package com.eventstream.client.demo;

import com.eventstream.client.ClusterProducer;
import com.eventstream.client.Consumer;
import com.eventstream.client.FetchResult;
import com.eventstream.client.MetadataClient;
import com.eventstream.client.NotLeaderException;
import com.eventstream.client.Producer;
import com.eventstream.common.protocol.ClusterConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Phase 4 replication demo.
 *
 * Prerequisites:
 *   Two broker instances must be running with the cluster config below.
 *
 *   cluster.properties:
 *     broker.1=localhost:9092
 *     broker.2=localhost:9093
 *     assign.repl-topic.0=1:2      # partition 0: leader=broker-1, follower=broker-2
 *     assign.repl-topic.1=2:1      # partition 1: leader=broker-2, follower=broker-1
 *
 *   Start broker 1:
 *     java -jar broker.jar --broker-id 1 --port 9092 --cluster-config cluster.properties
 *
 *   Start broker 2:
 *     java -jar broker.jar --broker-id 2 --port 9093 --cluster-config cluster.properties
 *
 * What this demo shows:
 *   1. ClusterProducer routes messages to the correct leader and retries on NOT_LEADER.
 *   2. MetadataClient discovers the live topology from any broker.
 *   3. Direct produce to a follower returns NOT_LEADER.
 *   4. Consumers read only up to the High Watermark (fully replicated records).
 *   5. Simple failover: stopping broker 1 and re-running shows broker 2 is elected.
 *
 * Run:
 *   java -cp client.jar com.eventstream.client.demo.ReplicationDemo [host] [port]
 */
public final class ReplicationDemo {

    private static final String TOPIC       = "repl-topic";
    private static final int    NUM_PARTS   = 2;
    private static final String BROKER1_HOST = "localhost";
    private static final int    BROKER1_PORT = 9092;
    private static final String BROKER2_HOST = "localhost";
    private static final int    BROKER2_PORT = 9093;

    public static void main(String[] args) throws Exception {
        String bootstrapHost = args.length > 0 ? args[0] : BROKER1_HOST;
        int    bootstrapPort = args.length > 1 ? Integer.parseInt(args[1]) : BROKER1_PORT;

        System.out.println("=== Phase 4 Replication Demo ===\n");

        // ------------------------------------------------------------------
        // Step 1: Discover topology via METADATA
        // ------------------------------------------------------------------
        System.out.println("--- Cluster topology from METADATA ---");
        MetadataClient metaClient = new MetadataClient(bootstrapHost, bootstrapPort);
        ClusterConfig  config     = metaClient.metadata();

        System.out.println("Brokers:");
        for (ClusterConfig.BrokerInfo b : config.brokers()) {
            System.out.println("  brokerId=" + b.brokerId()
                    + "  " + b.host() + ":" + b.port());
        }
        System.out.println("Partition assignments:");
        for (ClusterConfig.PartitionAssignment a : config.assignments()) {
            System.out.println("  " + a.topic() + "/" + a.partitionId()
                    + "  leader=" + a.leaderId()
                    + "  followers=" + formatArray(a.followerIds()));
        }
        System.out.println();

        // ------------------------------------------------------------------
        // Step 2: Produce via ClusterProducer (routes to leader automatically)
        // ------------------------------------------------------------------
        System.out.println("--- Producing 10 messages via ClusterProducer ---");
        try (ClusterProducer cp = new ClusterProducer(bootstrapHost, bootstrapPort)) {
            cp.createTopic(TOPIC, NUM_PARTS);
            for (int i = 1; i <= 10; i++) {
                String key = "key-" + i;
                String msg = "message-" + i;
                Producer.SendResult r = cp.send(TOPIC, key, msg);
                System.out.println("  sent: key=" + key
                        + "  partition=" + r.partitionId()
                        + "  offset=" + r.offset());
            }
        }
        System.out.println();

        // ------------------------------------------------------------------
        // Step 3: Demonstrate NOT_LEADER when producing directly to a follower
        // ------------------------------------------------------------------
        System.out.println("--- Direct produce to follower (expects NOT_LEADER) ---");
        try (Producer followerProducer = new Producer(BROKER2_HOST, BROKER2_PORT)) {
            // partition 0 is led by broker 1; sending to broker 2 should be rejected
            followerProducer.send(TOPIC, "key-x".getBytes(StandardCharsets.UTF_8),
                    "should-fail".getBytes(StandardCharsets.UTF_8));
            System.out.println("  ERROR: expected NotLeaderException but none was thrown");
        } catch (NotLeaderException e) {
            System.out.println("  Got NotLeaderException as expected: " + e.getMessage());
        }
        System.out.println();

        // ------------------------------------------------------------------
        // Step 4: Read from leader and observe HW-clamped results
        // ------------------------------------------------------------------
        System.out.println("--- Reading from leader (records up to High Watermark) ---");
        // Give followers a moment to catch up
        Thread.sleep(500);

        try (Consumer consumer = new Consumer(bootstrapHost, bootstrapPort)) {
            for (int partId = 0; partId < NUM_PARTS; partId++) {
                FetchResult result = consumer.poll(TOPIC, partId, 0L);
                System.out.println("  partition=" + partId
                        + "  records=" + result.messages().size()
                        + "  nextOffset=" + result.nextOffset());
                for (byte[] msg : result.messages()) {
                    System.out.println("    " + new String(msg, StandardCharsets.UTF_8));
                }
            }
        }
        System.out.println();

        System.out.println("Demo complete.");
        System.out.println();
        System.out.println("To test failover:");
        System.out.println("  1. Stop broker 1 (Ctrl+C on its terminal).");
        System.out.println("  2. Re-run this demo pointing at broker 2 (port 9093).");
        System.out.println("  3. The ClusterProducer will refresh metadata, find broker 2");
        System.out.println("     as the promoted leader, and continue producing.");
    }

    private static String formatArray(int[] arr) {
        if (arr == null || arr.length == 0) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(arr[i]);
        }
        return sb.append("]").toString();
    }
}
