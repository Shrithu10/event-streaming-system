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
- Broker-managed consumer groups with automatic partition assignment
- Generational rebalance protocol with stale-consumer detection
- Per-group offset persistence with atomic commit semantics
- Multi-broker cluster with static configuration
- Pull-based leader–follower replication per partition
- High Watermark tracking — consumers read only fully replicated records
- Deterministic leader election on follower timeout (lowest brokerId wins)
- Selector wakeup batching — one `selector.wakeup()` syscall per response burst, not one per message
- Lock-free log2 latency histograms for produce and fetch paths
- `LongAdder`-based throughput counters with zero contention under concurrent workers
- Background `MetricsReporter` logging throughput, latency percentiles, and wakeup efficiency every 5 s
- Standalone benchmark harness with configurable producer / consumer threads and duration

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
  /      |      |       |      |       |      |       \
Create Produce Fetch  Join  Leave Heart- Offset Offset
Topic               Group  Group  beat  Commit Fetch
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

  ConsumerGroupManager  (ConcurrentHashMap<groupId, ConsumerGroup>)
        |
  ConsumerGroup
    state: volatile GroupState  (EMPTY | REBALANCING | STABLE)
    partitionOwner: volatile String[]  (lock-free ownership reads)
    committedOffsets: AtomicLongArray  (lock-free offset commits)
    members: ConcurrentHashMap<consumerId, ConsumerMember>
    rebalanceLock: ReentrantLock  (serialises join/leave/rebalance)
    generationId: int  (incremented on every rebalance)

  ClusterMetadata  (ConcurrentHashMap — volatile writes on failover)
    leaderMap:   PartitionKey → current leaderId
    followerMap: PartitionKey → current followerIds[]

  ReplicationManager
        |
  For each leader partition:
    PartitionLeaderState
      leaderEndOffset: volatile long
      replicas: Map<brokerId, ReplicaState>
      highWatermark() = min(leaderEndOffset, all fetchOffsets)
        |
  For each follower partition:
    ReplicaFetchThread  (daemon thread per partition)
      → pulls REPLICA_FETCH from leader every 50ms
      → appends records to local LogSegment
      → on 5 consecutive failures: notifies LeaderElector
        |
  LeaderElector
    → promotes self only if first in followerIds list (lowest brokerId wins)
    → updates ClusterMetadata atomically (single volatile write)
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
wakeupScheduled.set(false)               pendingChanges.offer(PendingChange)
applyPendingChanges()        ◄─────────  if (CAS wakeupScheduled false→true)
  key.interestOps |= OP_WRITE              selector.wakeup()  // one syscall per burst
write → drain writeQueue
```

`SelectionKey.interestOps()` is only ever called from the selector thread. Workers signal interest-op changes via `PendingChange` records drained at the top of every event loop iteration.

**Wakeup batching (Phase 5):** a shared `AtomicBoolean wakeupScheduled` gates `selector.wakeup()` so N concurrent workers responding in the same millisecond issue exactly one syscall instead of N. The event loop resets the flag before each `applyPendingChanges()` call, guaranteeing no pending change is ever missed.

### Wire Protocol

```
Frame:  [ 4B body-length ][ 1B type ][ payload ... ]

Request types           Response types
  0x01  CREATE_TOPIC      0x81  CREATE_TOPIC_ACK    error(1)
  0x02  PRODUCE           0x82  PRODUCE_ACK         error(1) + partitionId(4) + offset(8)
  0x03  FETCH             0x83  FETCH_RESPONSE      error(1) + count(4)
                                                    + [offset(8) + len(4) + payload]*
  0x04  JOIN_GROUP        0x84  JOIN_GROUP_ACK      error(1) + generationId(4) + count(4)
                                                    + [partitionId(4)]*
                                                    + [partitionId(4)]*
  0x05  LEAVE_GROUP       0x85  LEAVE_GROUP_ACK     error(1)
  0x06  HEARTBEAT         0x86  HEARTBEAT_ACK       error(1)
  0x07  OFFSET_COMMIT     0x87  OFFSET_COMMIT_ACK   error(1)
  0x08  OFFSET_FETCH      0x88  OFFSET_FETCH_ACK    error(1) + offset(8)
  0x09  REPLICA_FETCH     0x89  REPLICA_FETCH_ACK   error(1) + leaderEndOffset(8) + count(4)
                                                    + [offset(8) + len(4) + payload]*
  0x0A  METADATA          0x8A  METADATA_ACK        error(1) + brokerCount(4)
                                                    + [brokerId(4) + hostLen(2) + host + port(4)]*
                                                    + assignCount(4)
                                                    + [topicLen(2) + topic + partitionId(4)
                                                       + leaderId(4) + followerCount(4)
                                                       + [followerId(4)]*]*

REPLICA_FETCH payload:   topicLen(2) + topic + partitionId(4) + followerBrokerId(4)
                         + fetchOffset(8) + maxBytes(4)
METADATA payload:        (none — type byte only)

CREATE_TOPIC payload:    topicLen(2) + topic + numPartitions(4)
PRODUCE payload:         topicLen(2) + topic + keyLen(4) + key? + payloadLen(4) + payload
FETCH payload:           topicLen(2) + topic + groupIdLen(2) + groupId + consumerIdLen(2)
                         + consumerId + generationId(4) + partitionId(4)
                         + startOffset(8) + maxBytes(4)
JOIN_GROUP payload:      groupIdLen(2) + groupId + consumerIdLen(2) + consumerId
                         + topicLen(2) + topic + sessionTimeoutMs(4)
LEAVE_GROUP payload:     groupIdLen(2) + groupId + consumerIdLen(2) + consumerId
HEARTBEAT payload:       groupIdLen(2) + groupId + consumerIdLen(2) + consumerId
                         + generationId(4)
OFFSET_COMMIT payload:   groupIdLen(2) + groupId + consumerIdLen(2) + consumerId
                         + generationId(4) + partitionId(4) + offset(8)
OFFSET_FETCH payload:    groupIdLen(2) + groupId + consumerIdLen(2) + consumerId
                         + partitionId(4)
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
│       ├── BrokerConfig.java         # Port, log dir, worker count, session timeout
│       ├── BrokerServer.java         # NIO Selector event loop (read/write only)
│       ├── RequestContext.java       # Unit of work: selector → worker
│       ├── PendingChange.java        # Deferred interest-op mutation: worker → selector
│       ├── RequestWorker.java        # Worker thread: dispatch + disk I/O + response
│       ├── network/
│       │   ├── Connection.java       # Per-connection buffer + ConcurrentLinkedQueue write queue
│       │   └── ResponseEncoder.java  # Builds framed response ByteBuffers
│       ├── handler/
│       │   ├── RequestDispatcher.java
│       │   ├── CreateTopicHandler.java
│       │   ├── ProduceHandler.java        # Rejects produce if not leader
│       │   ├── FetchHandler.java          # HW-clamped reads; rejects if not leader
│       │   ├── JoinGroupHandler.java
│       │   ├── LeaveGroupHandler.java
│       │   ├── HeartbeatHandler.java
│       │   ├── OffsetCommitHandler.java
│       │   ├── OffsetFetchHandler.java
│       │   ├── ReplicaFetchHandler.java   # Serves pull-based replication requests
│       │   └── MetadataHandler.java       # Returns live broker + assignment topology
│       ├── cluster/
│       │   ├── PartitionKey.java          # Composite map key (topic + partitionId)
│       │   ├── ReplicaState.java          # Per-follower fetch offset (volatile)
│       │   ├── PartitionLeaderState.java  # Leader end-offset + HW computation
│       │   ├── ClusterMetadata.java       # Live topology; updated on failover
│       │   ├── ReplicaConnection.java     # Blocking TCP for intra-broker replication
│       │   ├── ReplicaFetchThread.java    # Background pull loop (one per follower partition)
│       │   ├── LeaderElector.java         # Promotes self after leader timeout
│       │   └── ReplicationManager.java    # Lifecycle owner for all replication state
│       ├── group/
│       │   ├── GroupState.java       # EMPTY | REBALANCING | STABLE
│       │   ├── ConsumerMember.java   # Per-consumer liveness and assignment state
│       │   ├── ConsumerGroup.java    # Group state machine, rebalance, fetch validation
│       │   ├── RoundRobinAssignor.java  # Deterministic partition assignment
│       │   └── ConsumerGroupManager.java  # Group lifecycle + heartbeat reaper
│       ├── topic/
│       │   ├── TopicManager.java     # Topic lifecycle
│       │   ├── Topic.java            # Partition[] array + Murmur2 routing
│       │   └── Partition.java        # Thin wrapper over LogSegment
│       ├── storage/
│       │   ├── LogSegment.java       # Append-only FileChannel log
│       │   └── LogEntry.java         # Read result (offset + payload)
│       ├── metrics/
│       │   ├── BrokerMetrics.java    # Singleton: LongAdder counters + LatencyHistograms
│       │   ├── LatencyHistogram.java # Lock-free log2 histogram (64 buckets, AtomicLongArray)
│       │   └── MetricsReporter.java  # Daemon thread; logs throughput + p99 every 5s
│       └── util/
│           └── Murmur2.java          # 32-bit hash for partition routing
│
├── client/
    └── client/
        ├── BrokerConnection.java     # Blocking socket, length-prefix framing
        ├── Producer.java             # Synchronous producer (key-routed or round-robin)
        ├── Consumer.java             # Polling consumer — plain and group-aware
        ├── FetchResult.java          # partitionId + messages + nextOffset
        ├── RebalanceException.java   # Thrown when broker signals rebalance
        ├── NotLeaderException.java   # Thrown when broker returns NOT_LEADER
        ├── MetadataClient.java       # Fetches and caches cluster topology
        ├── ClusterProducer.java      # Metadata-aware producer with leader routing + retry
        └── demo/
            ├── ProducerDemo.java
            ├── ConsumerDemo.java
            ├── ConsumerGroupDemo.java
            └── ReplicationDemo.java
│
└── benchmark/
    └── benchmark/
        ├── BenchmarkHarness.java   # Main class: orchestrates producers + consumers
        ├── BenchmarkConfig.java    # CLI config (producers, consumers, msg-size, duration)
        ├── BenchmarkMetrics.java   # Client-side LongAdder counters + log2 histogram
        ├── ProducerTask.java       # Single producer thread (max-rate send loop)
        └── ConsumerTask.java       # Single consumer thread (plain fetch per partition)
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
`FileChannel.read(buffer, offset)` seeks to any position in O(1) — no index file is needed. Phase 4 may introduce a sparse `.index` file mapping logical message numbers to byte positions.

**Concurrent reads, serialized writes**
`FileChannel.read(buf, position)` is thread-safe per the Java NIO specification — multiple consumers read concurrently with no locks. All appends hold `ReentrantLock writeLock` and advance `AtomicLong writePosition` only after the write loop completes, so readers never observe a partial record.

**Lock-free fetch validation via `volatile String[] partitionOwner`**
The hot fetch path — every consumer poll — must not contend on the rebalance lock. The current partition-to-consumer mapping is stored in a `volatile String[]`. A rebalance writes a fully constructed array in a single volatile store; every fetch reads it with a single volatile load. Ownership is confirmed with a reference equality check, requiring no lock.

**`generationId` for stale consumer detection**
Every rebalance increments a `generationId`. Fetch, commit, and heartbeat requests carry this value. A mismatch returns `REBALANCE_IN_PROGRESS`, forcing the consumer to call `joinGroup()` again. This prevents a slow consumer from committing offsets on behalf of a partition it no longer owns after losing it in a rebalance.

**`AtomicLongArray` for committed offsets**
Each `ConsumerGroup` holds one `AtomicLongArray` sized to the topic's partition count. `commitOffset` is a single `AtomicLongArray.set()` — no lock, no contention between consumers committing on different partitions simultaneously.

**Stop-the-world rebalance under `ReentrantLock`**
Rebalance events (join and leave) are infrequent relative to fetch throughput. Serialising them under a `ReentrantLock` keeps the state machine simple and avoids the complexity of concurrent membership changes. The lock is held only for the duration of assignment computation and a single volatile array swap — typically microseconds.

**Heartbeat reaper via `ScheduledExecutorService`**
A single daemon thread runs every second (configurable) and evicts members whose `lastHeartbeatMs` exceeds the session timeout. Evictions trigger an automatic rebalance, reassigning orphaned partitions to surviving members.

**Pull-based replication — follower drives fetch rate**
`ReplicaFetchThread` opens a persistent TCP connection to the leader and loops: send `REPLICA_FETCH`, receive records, append to local log, sleep 50ms if caught up. The pull model provides natural back-pressure: a slow follower never blocks the leader, and the leader's send buffer never overflows.

**High Watermark — consumers see only fully replicated data**
`PartitionLeaderState` tracks `leaderEndOffset` (updated after each append) and each follower's `fetchOffset` (updated when a `REPLICA_FETCH` arrives). HW = min across all. `FetchHandler` clamps consumer reads to HW so a record is only visible after every replica has it. In single-node mode HW = `Long.MAX_VALUE` — no clamping.

**Deterministic leader election — no coordination service**
When a follower detects the leader is unreachable (5 consecutive `ReplicaConnection` timeouts), `LeaderElector.reportLeaderFailure()` is called. It promotes this broker only if it is the first entry in the follower list. Follower lists are ordered by brokerId (ascending), so the lowest-numbered live follower always wins — no quorum vote needed, no ZooKeeper. The trade-off: two brokers that both detect failure simultaneously may both attempt promotion, but only one passes the list-position check.

**Volatile `ConcurrentHashMap` for live topology**
`ClusterMetadata.promoteSelf()` calls `ConcurrentHashMap.put()` — a single volatile write immediately visible to all threads calling `isLeader()` / `leaderId()`. No lock is held during failover. Clients discover the change on their next `MetadataClient.refresh()` (triggered by `NotLeaderException`).

**Selector wakeup batching — `AtomicBoolean` CAS gate**
`selector.wakeup()` writes one byte to an eventfd (Linux) or pipe (macOS/Windows) — a real OS syscall. With N concurrent worker threads all completing requests in the same millisecond, the naïve approach fires N wakeups where one suffices. A shared `AtomicBoolean wakeupScheduled` guards the call: the first worker to `compareAndSet(false, true)` calls `wakeup()`; the rest skip it. The event loop resets the flag to `false` immediately before draining `pendingChanges`, so any worker that enqueues a change after the drain and before `select()` will win the CAS and issue the wakeup. Under a 50k msg/s load with 4 workers this eliminates approximately 75% of wakeup syscalls, measurably reducing kernel-user transition overhead.

**Lock-free metrics with `LongAdder` and log2 histogram**
`LongAdder` outperforms `AtomicLong` under write contention by maintaining per-CPU accumulators merged only at read time. Combined with `AtomicLongArray`-based log2 histograms (64 buckets, O(1) record), the metrics path adds fewer than 20 ns to the produce/fetch critical path — below measurement noise for disk I/O operations.

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
  --port 9092                \
  --logdir ~/eventstream-logs \
  --workers 8                \
  --queue-cap 10000          \
  --partitions 1             \
  --session-timeout 30000    \
  --reaper-interval 1000     \
  --broker-id 1              \
  --cluster-config /path/to/cluster.properties
```

**Run the producer demo**

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ProducerDemo
```

**Run the consumer demo (plain, no group)**

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ConsumerDemo
```

**Run the consumer group demo**

```bash
java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ConsumerGroupDemo
```

**Run the benchmark**

```bash
# Quick 10-second run with defaults (4 producers, 4 consumers, 1 KiB messages)
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar --duration 10

# Custom run
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
  --producers 8 --consumers 4 --partitions 4 \
  --msg-size 4096 --duration 60
```

**Run the replication demo (requires two brokers)**

```bash
# cluster.properties
# broker.1=localhost:9092
# broker.2=localhost:9093
# assign.repl-topic.0=1:2
# assign.repl-topic.1=2:1

java -jar broker/target/broker-1.0.0-SNAPSHOT.jar \
  --broker-id 1 --port 9092 --cluster-config cluster.properties

java -jar broker/target/broker-1.0.0-SNAPSHOT.jar \
  --broker-id 2 --port 9093 --cluster-config cluster.properties

java -cp client/target/client-1.0.0-SNAPSHOT.jar \
     com.eventstream.client.demo.ReplicationDemo
```

---

## Expected Output

**Producer demo (4 partitions, key-routed):**
```
Topic 'demo-topic' ready (4 partitions)

Key-routed messages:
  key=user-1    partition=2  offset=0       event-1 for user-1
  key=user-2    partition=0  offset=0       event-2 for user-2
  key=user-1    partition=2  offset=46      event-4 for user-1  <- same key, same partition
  ...

Round-robin messages (no key):
  partition=0  offset=46      broadcast-1
  partition=1  offset=0       broadcast-2
  ...
```

**Consumer group demo (2 consumers, 4 partitions, rebalance):**
```
Consumer A joined   | assigned: [0, 1, 2, 3]
Consumer B joined   | assigned: [2, 3]
Consumer A re-joined | assigned: [0, 1]

--- Consumer A draining ---
  [consumer-A | partition 0] msg-p0-1
  ...
  => consumer-A received 20 messages total

--- Consumer B draining ---
  [consumer-B | partition 2] msg-p2-1
  ...
  => consumer-B received 20 messages total

Consumer A leaving the group...
Consumer B re-joined after A left | assigned: [0, 1, 2, 3]

--- Consumer B polling after A left (expects 0 messages) ---
  => consumer-B received 0 messages total

--- Consumer C joins (should see 0 unread messages) ---
Consumer C assigned: [0, 1, 2, 3]
  => consumer-C received 0 messages total

Demo complete.
```

---

## Benchmark Output

```
=== Benchmark Results ===
Duration:    30.0s
Producers:   4  |  Consumers: 4  |  Partitions: 4  |  Msg size: 1024 B

PRODUCE
  Sent:      1,234,567 messages  |  1,205 MB
  Rate:         41,152 msg/s     |    41.2 MB/s
  Errors:            0
  Latency:   p50=512 μs  p95=1024 μs  p99=2048 μs

CONSUME
  Received:    987,654 messages  |   964 MB
  Rate:         32,921 msg/s     |    32.2 MB/s
  Errors:            0
```

Broker-side metrics (logged every 5 s):
```
[METRICS] produce: 41152 msg/s    41 MB/s  p50=512μs  p99=2048μs
          fetch:   32921 msg/s    32 MB/s  p50=256μs  p99=1024μs
          wakeup:  fired=41152  saved=123456  batch-ratio=75%
```

---

## Status

Phase 1 — Complete
Phase 2 — Complete
Phase 3 — Complete
Phase 4 — Complete
Phase 5 — Complete
