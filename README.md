# Event Streaming System

A production-oriented distributed event streaming system built from scratch using low-level Java networking and storage primitives. The architecture mirrors the core design of Apache Kafka without relying on any heavy frameworks.

---

## Overview

The system implements the fundamental components of a log-based event streaming platform:

- Append-only, sequential disk writes via `FileChannel`
- Custom binary wire protocol over raw TCP
- Non-blocking I/O using Java NIO Selector
- O(1) offset-based message retrieval (offset = byte position in log)
- Concurrent read / serialized write access to storage layer

---

## Architecture

```
Producer / Consumer
       |
       | TCP (binary protocol)
       |
  BrokerServer  (single NIO event-loop thread)
       |
  RequestDispatcher
  /         |         \
Create    Produce    Fetch
Topic
       |
  TopicManager  (ConcurrentHashMap, lock-free reads)
       |
  Partition  (one per topic in Phase 1)
       |
  LogSegment  (FileChannel, append-only)
       |
  ~/eventstream-logs/<topic>/partition-0/00000000.log
```

### Wire Protocol

```
Frame:  [ 4B body-length ][ 1B type ][ payload ... ]

Request types          Response types
  0x01  CREATE_TOPIC     0x81  CREATE_TOPIC_ACK   error(1)
  0x02  PRODUCE          0x82  PRODUCE_ACK        error(1) + offset(8)
  0x03  FETCH            0x83  FETCH_RESPONSE     error(1) + count(4)
                                                  + [offset(8) + len(4) + payload]*
```

### On-Disk Log Format

```
<log-root>/<topic>/partition-0/00000000.log
  [ 4B length ][ payload ][ 4B length ][ payload ] ...

offset of a record  =  its starting byte position in the file
```

---

## Module Structure

```
event-streaming-system/
├── common/                         # Shared protocol constants
│   └── protocol/
│       ├── RequestType.java        # Request / response type bytes
│       └── ErrorCode.java          # Error code constants
│
├── broker/                         # Broker server
│   └── broker/
│       ├── BrokerMain.java         # Entry point, shutdown hook
│       ├── BrokerConfig.java       # Port and log-directory config
│       ├── BrokerServer.java       # NIO Selector event loop
│       ├── network/
│       │   ├── Connection.java     # Per-connection buffer + write queue
│       │   └── ResponseEncoder.java# Builds framed response ByteBuffers
│       ├── handler/
│       │   ├── RequestDispatcher.java
│       │   ├── CreateTopicHandler.java
│       │   ├── ProduceHandler.java
│       │   └── FetchHandler.java
│       ├── topic/
│       │   ├── TopicManager.java   # Topic lifecycle, ConcurrentHashMap
│       │   └── Partition.java      # Thin wrapper over LogSegment
│       └── storage/
│           ├── LogSegment.java     # Append-only FileChannel log
│           └── LogEntry.java       # Read result (offset + payload)
│
└── client/                         # Producer and consumer library
    └── client/
        ├── BrokerConnection.java   # Blocking socket, length-prefix framing
        ├── Producer.java           # Synchronous producer API
        ├── Consumer.java           # Polling consumer API
        ├── FetchResult.java        # Returned by Consumer.poll()
        └── demo/
            ├── ProducerDemo.java
            └── ConsumerDemo.java
```

---

## Design Decisions

**Single event-loop thread**
One thread drives the NIO Selector for all accept / read / write events. There is no synchronization on the hot path. The only blocking call is the disk write inside `LogSegment`, which completes in microseconds against the kernel page cache. A worker thread pool (Phase 2) will offload request processing to allow parallel partition I/O.

**Offset = byte position**
Seeking to any offset is O(1) via `FileChannel.read(buffer, offset)`. No index file is required in Phase 1. Phase 2 will introduce a `.index` file mapping logical message numbers to byte positions, matching Kafka's approach.

**Conditional OP_WRITE interest**
Write interest on a channel is registered only when the write queue is non-empty and cleared the moment the queue drains. This avoids busy-spinning on writable-but-idle channels, a common mistake in NIO code.

**Concurrent reads, serialized writes**
`FileChannel.read(buf, position)` is thread-safe per the Java NIO specification — multiple consumers read concurrently without locks. All writes hold `writeLock` and advance `AtomicLong writePosition` only after the write loop completes, so readers never observe a partially-written record.

**Race-free topic creation**
`TopicManager` builds the `Partition` fully before calling `ConcurrentHashMap.putIfAbsent`. If two threads race, the losing thread closes its extra segment. Callers never observe a topic that lacks partition 0.

---

## Requirements

- Java 17+
- Maven 3.8+

---

## Build

```bash
mvn clean package -q
```

---

## Run

**Start the broker**

```bash
java -jar broker/target/broker-1.0.0-SNAPSHOT.jar
```

Optional flags:

```bash
java -jar broker/target/broker-1.0.0-SNAPSHOT.jar --port 9093 --logdir /tmp/logs
```

**Run the producer demo** (in a separate terminal)

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ProducerDemo
```

**Run the consumer demo** (in a separate terminal)

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ConsumerDemo
```

The consumer can be started before the producer. It will poll with 200ms back-off and pick up messages as they arrive.

---

## Expected Output

Producer:
```
Connecting to broker at localhost:9092
Topic 'demo-topic' ready
  sent [ 1] offset=0       message-1 | timestamp=...
  sent [ 2] offset=50      message-2 | timestamp=...
  ...
```

Consumer:
```
Connecting to broker at localhost:9092
Reading from topic 'demo-topic' (offset 0) ...
  [msg  1 @ offset 0     ]  message-1 | timestamp=...
  [msg  2 @ offset 50    ]  message-2 | timestamp=...
  ...
```

---

## Roadmap

**Phase 2 — Partitioning**
- Multiple partitions per topic
- Hash-based producer routing (`murmur2(key) % partitions`)
- Segment index file for logical offset mapping
- Worker thread pool for parallel partition I/O

**Phase 3 — Consumer Groups**
- Shared partition assignment across consumer instances
- Offset tracking per consumer group

**Phase 4 — Replication**
- Leader / follower model
- Basic failover

**Phase 5 — Performance**
- Batch writes
- Zero-copy path (`FileChannel.transferTo`)
- Syscall reduction
- JMH benchmarks

---

## Status

Phase 1 — Complete  
Phase 2 — In progress
