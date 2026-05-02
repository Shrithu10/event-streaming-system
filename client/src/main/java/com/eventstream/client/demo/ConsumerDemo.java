package com.eventstream.client.demo;

import com.eventstream.client.Consumer;
import com.eventstream.client.FetchResult;

/**
 * Standalone demo: connects to the broker and reads all messages from offset 0,
 * polling until no new messages arrive for 3 consecutive rounds.
 *
 * Run: java -cp client.jar com.eventstream.client.demo.ConsumerDemo [host] [port]
 */
public final class ConsumerDemo {

    public static void main(String[] args) throws Exception {
        String host  = args.length > 0 ? args[0] : "localhost";
        int    port  = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        String topic = "demo-topic";

        System.out.println("Connecting to broker at " + host + ":" + port);

        try (Consumer consumer = new Consumer(host, port)) {
            long offset     = 0;
            int  emptyRound = 0;
            int  total      = 0;

            System.out.println("Reading from topic '" + topic + "' (offset 0) ...");

            while (emptyRound < 3) {
                FetchResult result = consumer.poll(topic, offset);

                if (result.isEmpty()) {
                    emptyRound++;
                    Thread.sleep(200);
                    continue;
                }

                emptyRound = 0;
                for (byte[] msg : result.messages) {
                    total++;
                    System.out.printf("  [msg %2d @ offset %-6d]  %s%n",
                            total, offset, new String(msg));
                    offset = result.nextOffset; // advance after each message is processed
                }
                offset = result.nextOffset;
            }

            System.out.println("No more messages. Total read: " + total);
        }
    }
}
