package com.eventstream.client.demo;

import com.eventstream.client.Producer;

/**
 * Standalone demo: connects to the broker, creates a topic, sends 10 messages,
 * and prints each message's assigned offset.
 *
 * Run: java -cp client.jar com.eventstream.client.demo.ProducerDemo [host] [port]
 */
public final class ProducerDemo {

    public static void main(String[] args) throws Exception {
        String host  = args.length > 0 ? args[0] : "localhost";
        int    port  = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        String topic = "demo-topic";

        System.out.println("Connecting to broker at " + host + ":" + port);

        try (Producer producer = new Producer(host, port)) {
            producer.createTopic(topic);
            System.out.println("Topic '" + topic + "' ready");

            for (int i = 1; i <= 10; i++) {
                String message = "message-" + i + " | timestamp=" + System.currentTimeMillis();
                long offset = producer.send(topic, message);
                System.out.printf("  sent [%2d] offset=%-6d  %s%n", i, offset, message);
            }

            System.out.println("Done. 10 messages sent.");
        }
    }
}
