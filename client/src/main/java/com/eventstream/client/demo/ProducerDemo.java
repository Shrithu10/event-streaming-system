package com.eventstream.client.demo;

import com.eventstream.client.Producer;
import com.eventstream.client.Producer.SendResult;

/**
 * Phase 2 producer demo.
 *
 * Creates a topic with 4 partitions and sends 20 messages — 10 with explicit
 * routing keys (hash-routed) and 10 without keys (round-robin).  The assigned
 * partition and byte offset are printed for each message so you can verify
 * that key-based routing is consistent (same key always hits the same partition)
 * and that keyless messages spread across all 4 partitions.
 *
 * Run: java -cp client.jar com.eventstream.client.demo.ProducerDemo [host] [port]
 */
public final class ProducerDemo {

    private static final String TOPIC         = "demo-topic";
    private static final int    NUM_PARTITIONS = 4;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int    port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        System.out.println("Connecting to " + host + ":" + port);

        try (Producer producer = new Producer(host, port)) {

            producer.createTopic(TOPIC, NUM_PARTITIONS);
            System.out.println("Topic '" + TOPIC + "' ready (" + NUM_PARTITIONS + " partitions)\n");

            // --- Key-routed messages (same key → same partition) ---
            System.out.println("Key-routed messages:");
            String[] keys = {"user-1", "user-2", "user-3", "user-1", "user-2",
                             "user-3", "user-1", "user-2", "user-3", "user-4"};
            for (int i = 0; i < keys.length; i++) {
                String msg = "event-" + (i + 1) + " for " + keys[i];
                SendResult r = producer.send(TOPIC, keys[i], msg);
                System.out.printf("  key=%-8s  partition=%d  offset=%-6d  %s%n",
                        keys[i], r.partitionId(), r.offset(), msg);
            }

            System.out.println();

            // --- Keyless messages (round-robin) ---
            System.out.println("Round-robin messages (no key):");
            for (int i = 1; i <= 10; i++) {
                String msg = "broadcast-" + i;
                SendResult r = producer.send(TOPIC, (byte[]) null,
                        msg.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                System.out.printf("  partition=%d  offset=%-6d  %s%n",
                        r.partitionId(), r.offset(), msg);
            }

            System.out.println("\nDone. 20 messages sent across " + NUM_PARTITIONS + " partitions.");
        }
    }
}
