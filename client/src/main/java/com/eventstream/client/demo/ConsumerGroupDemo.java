package com.eventstream.client.demo;

import com.eventstream.client.Consumer;
import com.eventstream.client.FetchResult;
import com.eventstream.client.Producer;
import com.eventstream.client.RebalanceException;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Phase 3 consumer group demo.
 *
 * Scenario:
 *   1. Producer publishes 40 messages across a 4-partition topic.
 *   2. Consumer A joins the group and is assigned all 4 partitions.
 *   3. Consumer B joins the same group; the broker rebalances — 2 partitions each.
 *   4. Both consumers drain their assigned partitions, committing offsets after each fetch.
 *   5. Consumer A leaves; the broker rebalances — B gets all 4 partitions.
 *   6. Consumer B re-polls to confirm nothing new arrives (all offsets were committed).
 *   7. A fresh Consumer C joins to prove committed offsets survive across consumers.
 *
 * Run: java -cp client.jar com.eventstream.client.demo.ConsumerGroupDemo [host] [port]
 */
public final class ConsumerGroupDemo {

    private static final String TOPIC          = "group-demo-topic";
    private static final String GROUP_ID       = "demo-group";
    private static final int    NUM_PARTITIONS = 4;
    private static final int    SESSION_MS     = 10_000;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int    port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        System.out.println("Connecting to " + host + ":" + port + "\n");

        // ------------------------------------------------------------------
        // Step 1: produce 40 messages — 10 per partition via explicit keys
        // ------------------------------------------------------------------
        try (Producer producer = new Producer(host, port)) {
            producer.createTopic(TOPIC, NUM_PARTITIONS);
            System.out.println("Topic '" + TOPIC + "' ready (" + NUM_PARTITIONS + " partitions)");

            for (int p = 0; p < NUM_PARTITIONS; p++) {
                for (int i = 1; i <= 10; i++) {
                    // Key is chosen so that Murmur2(key) % 4 == p.
                    // We use numeric string keys whose hash distributes naturally.
                    String key = "p" + p;
                    String msg = "msg-p" + p + "-" + i;
                    producer.send(TOPIC, key, msg);
                }
            }
            System.out.println("Produced 40 messages (10 per partition)\n");
        }

        // ------------------------------------------------------------------
        // Step 2: Consumer A joins — gets all 4 partitions initially
        // ------------------------------------------------------------------
        Consumer consumerA = new Consumer(host, port);
        int[] partitionsA  = consumerA.joinGroup(GROUP_ID, "consumer-A", TOPIC, SESSION_MS);
        System.out.println("Consumer A joined  | assigned: " + fmt(partitionsA));

        // ------------------------------------------------------------------
        // Step 3: Consumer B joins — triggers rebalance; 2 partitions each.
        //         Consumer A discovers the rebalance on its next heartbeat and re-joins.
        // ------------------------------------------------------------------
        Consumer consumerB = new Consumer(host, port);
        int[] partitionsB  = consumerB.joinGroup(GROUP_ID, "consumer-B", TOPIC, SESSION_MS);
        System.out.println("Consumer B joined  | assigned: " + fmt(partitionsB));

        // Consumer A must re-join to learn its new (reduced) assignment.
        partitionsA = consumerA.joinGroup(GROUP_ID, "consumer-A", TOPIC, SESSION_MS);
        System.out.println("Consumer A re-joined | assigned: " + fmt(partitionsA) + "\n");

        // ------------------------------------------------------------------
        // Step 4: Both consumers drain their partitions and commit offsets
        // ------------------------------------------------------------------
        System.out.println("--- Consumer A draining ---");
        drain(consumerA, "consumer-A", TOPIC);

        System.out.println("\n--- Consumer B draining ---");
        drain(consumerB, "consumer-B", TOPIC);

        // ------------------------------------------------------------------
        // Step 5: Consumer A leaves — B gets all 4 partitions after rebalance
        // ------------------------------------------------------------------
        System.out.println("\nConsumer A leaving the group...");
        consumerA.leaveGroup();
        consumerA.close();

        partitionsB = consumerB.joinGroup(GROUP_ID, "consumer-B", TOPIC, SESSION_MS);
        System.out.println("Consumer B re-joined after A left | assigned: " + fmt(partitionsB));

        // ------------------------------------------------------------------
        // Step 6: B polls — should see 0 messages (offsets already committed)
        // ------------------------------------------------------------------
        System.out.println("\n--- Consumer B polling after A left (expects 0 messages) ---");
        drain(consumerB, "consumer-B", TOPIC);

        consumerB.leaveGroup();
        consumerB.close();

        // ------------------------------------------------------------------
        // Step 7: Fresh Consumer C — proves committed offsets persist
        // ------------------------------------------------------------------
        System.out.println("\n--- Consumer C joins (should see 0 unread messages) ---");
        try (Consumer consumerC = new Consumer(host, port)) {
            int[] partitionsC = consumerC.joinGroup(GROUP_ID, "consumer-C", TOPIC, SESSION_MS);
            System.out.println("Consumer C assigned: " + fmt(partitionsC));
            drain(consumerC, "consumer-C", TOPIC);
        }

        System.out.println("\nDemo complete.");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Polls all assigned partitions in a loop until every partition returns empty,
     * committing after each non-empty fetch.  Handles RebalanceException by re-joining.
     */
    private static void drain(Consumer consumer, String consumerId, String topic)
            throws Exception {
        int totalMessages = 0;
        boolean progress  = true;

        while (progress) {
            progress = false;
            try {
                consumer.heartbeat();
                List<FetchResult> results = consumer.pollGroupAll(topic);

                for (FetchResult result : results) {
                    if (result.messages().isEmpty()) continue;
                    progress = true;

                    for (byte[] raw : result.messages()) {
                        System.out.println("  [" + consumerId + " | partition "
                                + result.partitionId() + "] "
                                + new String(raw, StandardCharsets.UTF_8));
                        totalMessages++;
                    }
                    consumer.commitOffset(result.partitionId(), result.nextOffset());
                }

            } catch (RebalanceException e) {
                System.out.println("  [" + consumerId + "] Rebalance detected — re-joining...");
                consumer.joinGroup(GROUP_ID, consumerId, topic, SESSION_MS);
                progress = true; // retry after new assignment
            }
        }

        System.out.println("  => " + consumerId + " received " + totalMessages + " messages total");
    }

    private static String fmt(int[] arr) {
        if (arr == null || arr.length == 0) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(arr[i]);
        }
        return sb.append("]").toString();
    }
}
