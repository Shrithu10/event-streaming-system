# Event Streaming System

A production-oriented distributed event streaming system built from scratch using low-level Java networking and storage primitives. The architecture mirrors the core design of Apache Kafka without relying on any heavy frameworks.

---

## Overview

The system implements the fundamental components of a log-based event streaming platform:

- Append-only, sequential disk writes via `FileChannel`
- Custom binary wire protocol over raw TCP
- Non-blocking I/O using Java NIO Selector
- Worker thread pool for parallel request processing and disk I/O
- Multi-partition topics with Murmur2 hash-based routing
- O(1) offset-based message retrieval (offset = byte position in log)
- Concurrent reads and serialized writes at the storage layer

---

## Architecture

```
Producers / Consumers
        |
        | TCP  (binary protocol)
        |
  BrokerServer  (single NIO event-loop thread)
  accept | read | write only — zero blocking operations
        |
        | ArrayBlockingQueue<RequestContext>
        |
  Worker Thread Pool  (N = availableProcessors)
  request decode | disk I/O | response build
        |
        | ConcurrentLinkedQueue<PendingChange>  +  selector.wakeup()
        |
  BrokerServer writes response to client
        |
  RequestDispatcher
  /          |          \
Create     Produce     Fetch
Topic
        |
  TopicManager  (ConcurrentHashMap<name, Topic>)
        |
  Topic  { Partition[]  +  Murmur2 routing  +  round-robin counter }
        |
  Partition  (index == partitionId, O(1) array access)
        |
  LogSegment  (FileChannel, append-only, positional reads)
        |
  <log-root>/<topic>/partition-N/00000000.log
```

### Threading Model

The selector thread and worker threads communicate through two lock-free structures:

```
Selector thread                          Worker threads
────────────────                         ─────────────────────────────────
read → pollFrame()
  → requestQueue.offer(ctx)  ─────────►  ctx = requestQueue.take()
                                          dispatch(ctx.frame)  [disk I/O]
                                          connection.enqueueResponse(buf)
applyPendingChanges()        ◄─────────  pendingChanges.offer(PendingChange)
  key.interestOps |= OP_WRITE            selector.wakeup()
write → drain writeQueue
```

`SelectionKey.interestOps()` is only ever called from the selector thread. Workers signal interest-op changes via `PendingChange` records drained at the top of every event loop iteration.

### Wire Protocol

```
Frame:  [ 4B body-length ][ 1B type ][ payload ... ]

Request types          Response types
  0x01  CREATE_TOPIC     0x81  CREATE_TOPIC_ACK   error(1)
  0x02  PRODUCE          0x82  PRODUCE_ACK        error(1) + partitionId(4) + offset(8)
  0x03  FETCH            0x83  FETCH_RESPONSE     error(1) + count(4)
                                                  + [offset(8) + len(4) + payload]*

CREATE_TOPIC payload:  topicLen(2) + topic + numPartitions(4)
PRODUCE payload:       topicLen(2) + topic + keyLen(4) + key? + payloadLen(4) + payload
FETCH payload:         topicLen(2) + topic + partitionId(4) + startOffset(8) + maxBytes(4)
```

### On-Disk Log Format

```
<log-root>/<topic>/partition-<N>/00000000.log
  [ 4B length ][ payload ][ 4B length ][ payload ] ...

offset of a record  =  its starting byte position in the file  (O(1) seek)
```

---

## Module Structure

```
event-streaming-system/
├── common/
│   └── protocol/
│       ├── RequestType.java          # Request / response type bytes
│       └── ErrorCode.java            # Error code constants
│
├── broker/
│   └── broker/
│       ├── BrokerMain.java           # Entry point, shutdown hook
│       ├── BrokerConfig.java         # Port, log dir, worker count, queue capacity
│       ├── BrokerServer.java         # NIO Selector event loop (Phase 2: read/write only)
│       ├── RequestContext.java        # Unit of work: selector → worker
│       ├── PendingChange.java         # Deferred interest-op mutation: worker → selector
│       ├── RequestWorker.java         # Worker thread: dispatch + disk I/O + response
│       ├── network/
│       │   ├── Connection.java        # Per-connection buffer + ConcurrentLinkedQueue write queue
│       │   └── ResponseEncoder.java   # Builds framed response ByteBuffers
│       ├── handler/
│       │   ├── RequestDispatcher.java
│       │   ├── CreateTopicHandler.java
│       │   ├── ProduceHandler.java
│       │   └── FetchHandler.java
│       ├── topic/
│       │   ├── TopicManager.java      # Topic lifecycle
│       │   ├── Topic.java             # Partition[] array + Murmur2 routing
│       │   └── Partition.java         # Thin wrapper over LogSegment
│       ├── storage/
│       │   ├── LogSegment.java        # Append-only FileChannel log
│       │   └── LogEntry.java          # Read result (offset + payload)
│       └── util/
│           └── Murmur2.java           # 32-bit hash for partition routing
│
└── client/
    └── client/
        ├── BrokerConnection.java      # Blocking socket, length-prefix framing
        ├── Producer.java              # Synchronous producer (key-routed or round-robin)
        ├── Consumer.java              # Polling consumer (per-partition + pollAll)
        ├── FetchResult.java           # partitionId + messages + nextOffset
        └── demo/
            ├── ProducerDemo.java
            └── ConsumerDemo.java
```

---

## Design Decisions

**Selector thread handles only I/O — never business logic**
The NIO Selector thread accepts connections, assembles frames, and drains write queues. All request processing and disk I/O happens on worker threads. This eliminates head-of-line blocking: a slow disk write on one partition cannot stall reads for other connections.

**`ArrayBlockingQueue` for selector-to-worker handoff**
Bounded capacity gives built-in back-pressure. When the queue is full, `OP_READ` is suspended for the producing connection — TCP flow control propagates the signal back to the sender at no extra cost. The queue resumes reads once it drains below 80% capacity.

**`ConcurrentLinkedQueue<PendingChange>` for worker-to-selector signalling**
Workers must never call `SelectionKey.interestOps()` directly; that method is not safe to call concurrently with `Selector.select()`. Instead, workers post a `PendingChange` record and call `selector.wakeup()`. The selector drains the queue at the top of every loop iteration and applies changes in its own thread.

**`Partition[]` array instead of `ConcurrentHashMap<Integer, Partition>`**
Partitions are fixed at topic creation time. An array indexed by partition id is O(1) with no hashing overhead on every produce and fetch call.

**Murmur2 for partition routing**
Java's `String.hashCode()` has poor low-bit distribution, which causes hot partitions with certain key patterns. Murmur2 (the same algorithm Kafka uses) produces a uniform 32-bit output, yielding balanced partition load regardless of key structure.

**Offset = byte position**
`FileChannel.read(buffer, offset)` seeks to any position in O(1) — no index file is needed. Phase 3 will introduce a `.index` file mapping logical message numbers to byte positions for human-friendly sequential offsets.

**Concurrent reads, serialized writes**
`FileChannel.read(buf, position)` is thread-safe per the Java NIO specification — multiple consumers read concurrently with no locks. All appends hold `ReentrantLock writeLock` and advance `AtomicLong writePosition` only after the write loop completes, so readers never observe a partial record.

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

All flags and their defaults:

```bash
java -jar broker/target/broker-1.0.0-SNAPSHOT.jar \
  --port 9092          \
  --logdir ~/eventstream-logs \
  --workers 8          \
  --queue-cap 10000    \
  --partitions 1
```

**Run the producer demo**

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ProducerDemo
```

**Run the consumer demo**

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ConsumerDemo
```

---

## Expected Output

Producer (Phase 2 — 4 partitions, key-routed):
```
Topic 'demo-topic' ready (4 partitions)

Key-routed messages:
  key=user-1    partition=2  offset=0       event-1 for user-1
  key=user-2    partition=0  offset=0       event-2 for user-2
  key=user-3    partition=3  offset=0       event-3 for user-3
  key=user-1    partition=2  offset=46      event-4 for user-1   <- same key, same partition
  ...

Round-robin messages (no key):
  partition=0  offset=46      broadcast-1
  partition=1  offset=0       broadcast-2
  partition=2  offset=92      broadcast-3
  partition=3  offset=46      broadcast-4
  ...
```

Consumer (Phase 2 — reads all 4 partitions):
```
Reading from topic 'demo-topic' (4 partitions)

  [msg   1]  partition=0  offset=0       event-2 for user-2
  [msg   2]  partition=1  offset=0       broadcast-2
  [msg   3]  partition=2  offset=0       event-1 for user-1
  ...

Final offsets per partition:
  partition-0: 184 bytes consumed
  partition-1: 92 bytes consumed
  partition-2: 276 bytes consumed
  partition-3: 138 bytes consumed
```

---

## Roadmap

**Phase 3 — Consumer Groups**
- Broker-managed partition assignment across consumer instances
- Offset tracking per consumer group
- Rebalance protocol on consumer join / leave

**Phase 4 — Replication**
- Leader / follower model per partition
- In-sync replica set (ISR)
- Basic failover on leader failure

**Phase 5 — Performance**
- Batch writes and read-ahead
- Zero-copy path (`FileChannel.transferTo`)
- Syscall reduction and write coalescing
- JMH benchmarks (throughput and latency)

---

## Status

Phase 1 — Complete
Phase 2 — Complete
Phase 3 — In progress
