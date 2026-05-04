package com.eventstream.client.demo;

import com.eventstream.client.Consumer;
import com.eventstream.client.FetchResult;

import java.util.List;

/**
 * Phase 2 consumer demo.
 *
 * Reads from all 4 partitions of "demo-topic" in a round-robin polling loop,
 * maintaining a per-partition offset array.  Exits after 3 consecutive empty
 * polls across all partitions.
 *
 * This demonstrates the fundamental multi-partition consumption pattern that
 * Phase 3 (consumer groups) will automate.
 *
 * Run: java -cp client.jar com.eventstream.client.demo.ConsumerDemo [host] [port]
 */
public final class ConsumerDemo {

    private static final String TOPIC          = "demo-topic";
    private static final int    NUM_PARTITIONS  = 4;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int    port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;

        System.out.println("Connecting to " + host + ":" + port);
        System.out.println("Reading from topic '" + TOPIC + "' (" + NUM_PARTITIONS + " partitions)\n");

        try (Consumer consumer = new Consumer(host, port)) {

            long[] offsets   = new long[NUM_PARTITIONS]; // all start at 0
            int    total     = 0;
            int    emptyRound = 0;

            while (emptyRound < 3) {
                List<FetchResult> results = consumer.pollAll(TOPIC, NUM_PARTITIONS, offsets);

                boolean anyMessages = false;
                for (FetchResult result : results) {
                    if (result.isEmpty()) continue;
                    anyMessages = true;

                    for (byte[] msg : result.messages) {
                        total++;
                        System.out.printf("  [msg %3d]  partition=%d  offset=%-6d  %s%n",
                                total, result.partitionId, offsets[result.partitionId],
                                new String(msg));
                    }
                    offsets[result.partitionId] = result.nextOffset;
                }

                if (!anyMessages) {
                    emptyRound++;
                    Thread.sleep(200);
                } else {
                    emptyRound = 0;
                }
            }

            System.out.println("\nNo more messages. Total read: " + total);
            System.out.println("Final offsets per partition:");
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                System.out.printf("  partition-%d: %d bytes consumed%n", p, offsets[p]);
            }
        }
    }
}
