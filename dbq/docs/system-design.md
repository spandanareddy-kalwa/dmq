# Distributed Message Queue: System Design

## 1. Overview

This project implements a distributed message queue designed around a Kafka-like mental model, but built from first principles using Java 17, gRPC, ZooKeeper, local disk append-only logs, and Docker Compose.

The system is intentionally designed for a student team: realistic enough to explain distributed concepts clearly, but scoped to be understandable and implementable in phases.

## 2. Core Concepts

- Producer: client that publishes messages to a topic.
- Broker: node that owns partitions and handles produce/fetch operations.
- Coordinator / Metadata Service: central registry for cluster metadata and health.
- Topic: logical stream of messages.
- Partition: ordered sequence of messages inside a topic.
- Consumer Group: group of consumers sharing work for a topic.
- Offset: position of a consumer within a partition.
- Replication: copying partition data across brokers.
- Leader/Follower: election and failover model for partitions.
- ISR: in-sync replicas that are caught up enough to participate in commits.
- Ack: producer write durability guarantee.

## 3. Requirements

### Functional requirements

- Producers publish messages.
- Consumers consume messages.
- Topics and partitions are supported.
- Ordering is preserved within a partition.
- Consumer groups are supported.
- Consumer offsets are maintained.
- Configurable delivery semantics are available.
- Batching is supported.
- Messages are persisted to disk.
- Partitions are replicated.
- Broker failures are detected.
- New leaders are elected.
- Partition metadata is tracked.
- Message retention and truncation are supported.
- Acknowledgement behavior is configurable.

### Non-functional requirements

- High throughput
- Low latency
- Horizontal scalability
- Durability
- Fault tolerance
- Recoverability
- Observability
- Backpressure
- Configurability

### Version 1 scope

IMPLEMENTED:

- Maven multi-module skeleton and shared Protobuf/gRPC contracts
- ZooKeeper-backed persistent topics, partitions, and committed offsets, plus ephemeral broker registrations
- coordinator gRPC for broker registration, heartbeats, topic/partition metadata, consumer group membership, replica progress, and offsets
- broker gRPC processes with local CRC32C-framed append-only logs, recovery, truncation, retention, and pull fetch
- producer partition routing, explicit batches, cached channels, bounded retries, and message-ID deduplication in broker logs
- consumer pull flow, simple round-robin assignment, group heartbeats, and explicit commits
- synchronous ISR replication for ACK_ALL, replica progress/high-watermarks, follower catch-up, and basic leader failover
- local one- and three-broker gRPC integration tests

PARTIALLY IMPLEMENTED:

- the coordinator is a single process; ZooKeeper persistence does not provide coordinator HA or leader fencing
- group membership and assignments are in memory and must re-form after coordinator restart
- `ACK_0` cannot be fire-and-forget over the current unary API and still waits for local append/fsync
- retention runs on broker requests and removes only expired sealed segments
- a rejoining replica must match the current leader's reported end offset before entering ISR; divergent logs requiring operator repair are not reconciled
- tests cover local scenarios but not sustained load, network partitions, or production-scale performance

SIMULATED:

- production-scale load, operational metrics dashboards, and advanced security behavior

NOT IMPLEMENTED:

- exactly-once transactions across producer writes, consumer processing, and offset commits
- TLS, authentication, topic-level authorization, and protected administrative APIs
- multi-coordinator HA, leader epochs/fencing, multi-datacenter replication, and automated partition reassignment
- a background retention service, persistent group membership, and a dedicated metrics/exporter stack
- global ordering across partitions

### Current Implementation Notes

The coordinator executable uses ZooKeeper by default (`DBQ_ZOOKEEPER_CONNECT_STRING`, default `localhost:2181`). Broker registration uses ephemeral nodes; topics, partitions, and committed offsets use persistent nodes. Messages remain exclusively on broker disks. Broker liveness is also checked using coordinator-received heartbeat timestamps. The implementation uses a single coordinator and does not claim split-brain safety if that coordinator is duplicated or loses connectivity to ZooKeeper.

`DBQ_COORDINATOR_PORT` defaults to `9090`; `DBQ_HEARTBEAT_TIMEOUT_MS` defaults to `15000`. Broker logs recover from disk on restart and truncate records beyond the persisted high-watermark when the coordinator reports one. The end-to-end suite verifies one- and three-broker flows, including a leader failure, but Docker Compose could not be exercised in the current environment.

## 4. High-Level Architecture

Producer
  |
  v
Coordinator / Metadata Service
  |
  v
Broker Cluster
  +---- Broker 1
  +---- Broker 2
  +---- Broker 3
  |
  v
Consumer Groups
  |
  v
Consumers

### Target component responsibilities

- Producer: chooses a partition and sends batches to leader brokers.
- Coordinator: stores cluster metadata, broker health, topic definitions, and leader assignments.
- Broker: owns partition logs, serves fetch and produce requests, replicates to followers, manages retention.
- Consumer group: coordinates assignment of partitions to consumers.
- Consumer: reads from assigned partitions, processes messages, and commits offsets.

## 5. Broker Design

Each broker runs as an independent Java process with its own gRPC server and local disk storage.

The typical broker lifecycle:

1. Broker starts with a configured broker ID and port.
2. It registers itself with the coordinator.
3. It loads its local partitions from disk.
4. It recovers segments and indexes.
5. It sends heartbeats to the coordinator.
6. It becomes available for leadership or follower responsibilities.

Broker responsibilities:

- gRPC server for produce/fetch requests
- partition manager
- log manager
- storage manager
- replica manager
- leader/follower state tracking
- consumer fetch logic
- producer request handling
- offset coordination
- health/heartbeat mechanism

## 6. Topic and Partition Design

A topic contains multiple partitions.

Example:

orders
  |
  +-- partition-0
  +-- partition-1
  +-- partition-2

Each partition keeps:

- partition ID
- leader broker
- replica brokers
- ISR
- current/high-watermark offset
- log segments
- retention configuration

Messages from the same partition preserve ordering.

Partition key behavior:

- key is hashed
- hash is mapped by modulo number of partitions
- the formula is: hash(key) % numPartitions
- this ensures same key goes to same partition, but does not guarantee global ordering across partitions

## 7. Message Model

The message model is shared via protobuf and reused by producers, brokers, and consumers.

Required fields:

- message_id
- topic
- partition
- offset
- key
- payload
- timestamp
- size
- checksum
- headers

The message format remains immutable once appended to a log.

## 8. Storage Engine

The system uses an append-only log per partition.

Partition
  |
  +-- segment-00001.log
  +-- segment-00002.log
  +-- segment-00003.log

Key properties:

- writes are append-only
- sequential disk I/O is preferred
- segment creation occurs on size threshold or time threshold
- segment rolling keeps logs manageable
- offsets are tracked in sequence
- reads are by offset through index and segment mapping
- recovery after restart replays valid records
- corrupted records are truncated or quarantined
- retention removes old segments by time or size
- fsync is used for durability before acknowledging a write, depending on ack mode

## 9. Producer Design

Producer flow:

Producer
  | get metadata
  v
Coordinator
  | leader info
  v
Producer
  | send batch
  v
Partition Leader
  | replicate
  v
Followers

Producer responsibilities:

- partition selection
- leader discovery
- batch assembly
- retry handling
- timeout policy
- ack mode execution
- idempotency handling where possible
- backpressure control
- connection management

Ack modes:

- ACK=0: fire-and-forget, lowest latency, highest chance of loss
- ACK=1: leader durably writes, follower replication optional
- ACK=ALL: leader and in-sync replicas acknowledge, strongest durability for this design

## 10. Consumer Design

The consumer is pull-based.

The consumer requests messages starting at an offset, optionally in batches, and can long-poll when no new messages are available.

Consumer features:

- fetch by offset
- batch fetch
- long polling
- consumer-group membership
- offset commits
- retries
- backpressure

Pull-based consumption is useful because it lets a consumer control processing rate and avoid becoming overwhelmed.

## 11. Consumer Groups

Consumer Group
  +-- Consumer 1
  +-- Consumer 2
  +-- Consumer 3

Rules:

- each partition belongs to only one consumer in a group at a time
- multiple groups can consume the same topic independently
- each group tracks its own offsets
- rebalancing is triggered when consumers join or leave
- failure causes reassignment

Version 1 uses a simple round-robin assignment strategy.

## 12. Offset Management

Offset data includes:

- consumer group
- topic
- partition
- last committed offset

Current processing position is different from the committed offset.

- current processing position: where processing is right now
- committed offset: the last offset confirmed durable by the consumer

Failure case:

Consumer reads message -> processes -> crashes before offset commit

This can cause duplicate processing after restart because the consumer resumes from the last committed offset. This is the classic at-least-once behavior.

## 13. Replication

Leader
  +---- Follower
  +---- Follower

Producers send writes to the leader. Followers replicate the append sequence from the leader.

Key replication concepts:

- replica state
- replication offset
- ISR
- high-watermark / committed offset
- replica lag
- catch-up
- failure and recovery

A message is considered committed when it is written to the leader and replicated to a quorum of in-sync replicas depending on the configured safety model.

## 14. Leader Election

Leader election is coordinated with ZooKeeper and internal metadata checks.

Example:

Before:
- Partition 0 leader = Broker 1
- Replicas = Broker 1, Broker 2, Broker 3

After broker 1 fails:
- Partition 0 leader = Broker 2
- Replicas = Broker 1, Broker 2, Broker 3

This design avoids split-brain by requiring the coordinator to validate broker liveness and partition state before electing a leader.

## 15. Broker Registration and Failure Detection

The coordinator tracks:

- broker registration
- heartbeats
- broker liveness
- dead broker removal
- leader reelection
- replica reassignment

Example failing broker transition:

Broker 1 -> ALIVE
Broker 2 -> ALIVE
Broker 3 -> ALIVE

Broker 2 fails:

Broker 1 -> ALIVE
Broker 2 -> DEAD
Broker 3 -> ALIVE

Then affected partition leadership is reconsidered.

## 16. Coordinator / Metadata Service

The coordinator exposes APIs for:

- registerBroker()
- heartbeat()
- getLiveBrokers()
- createTopic()
- getTopicMetadata()
- getPartitionMetadata()
- getLeader()
- getReplicas()
- electLeader()

Metadata includes:

- topic definitions
- partition counts
- leaders
- replica assignments
- ISR
- broker info
- retention config

## 17. ZooKeeper

The running coordinator uses ZooKeeper for metadata that needs persistence across coordinator restarts.

Examples:

- broker registration
- broker liveness information
- partition metadata
- leader information
- coordination state

Broker registration is ephemeral. Topic/partition metadata and committed consumer offsets are persistent. Group membership is kept in coordinator memory. Updates currently use ZooKeeper versions for writes but do not implement multi-coordinator leader fencing. Message contents remain on broker disk; ZooKeeper is not a message store.

## 18. Delivery Semantics

### At-most-once

- no duplicate processing
- may lose messages

### At-least-once

- messages are not lost before successful processing
- duplicates can occur

### Exactly-once

This is treated as future work and is explicitly not claimed. To provide true exactly-once semantics, the system would need transactional boundaries across writes, processing, and offset commits.

## 19. Failure Scenarios

The design explicitly covers:

1. Producer crashes
2. Consumer crashes
3. Broker crashes
4. Leader broker crashes
5. Follower crashes
6. Consumer crashes before committing offset
7. Broker restarts
8. Network timeout
9. Duplicate producer retry
10. Slow follower
11. Consumer becomes slow
12. Partition becomes unavailable
13. ZooKeeper becomes unavailable

For each scenario, the system records:

- what happens
- what component detects it
- how state changes
- how recovery proceeds
- whether messages may be lost
- whether duplicates may be created

## 20. Scalability

The system scales by increasing:

- producers
- brokers
- partitions
- consumer count

Within one consumer group, maximum useful parallelism is reached when the number of consumers is less than or equal to the number of partitions.

## 21. API Design

This project defines shared gRPC and protobuf contracts in the proto module, with a single canonical message schema used by all team members.

Service categories:

- Producer -> Broker
- Consumer -> Broker
- Producer -> Coordinator
- Consumer -> Coordinator
- Broker -> Coordinator
- Broker -> Broker

Each API definition includes:

- request
- response
- errors
- timeout
- retry behavior
- idempotency considerations

## 22. Repository Structure

distributed-message-queue/
  |
  +-- common/
  +-- coordinator/
  +-- broker/
  +-- producer/
  +-- consumer/
  +-- proto/
  +-- integration-tests/
  +-- docker/
  +-- docs/
  +-- pom.xml
  +-- docker-compose.yml

Responsibilities:

- common: shared constants, config, exceptions, and model definitions
- coordinator: metadata service and liveness tracking
- broker: partition and log logic
- producer: produce API client
- consumer: consume API client and offset logic
- proto: shared Protocol Buffers and gRPC services
- integration-tests: system verification
- docker: runtime containerization
- docs: architecture, guides, and phase notes

## 23. Team Separation

Member 1: Broker + Storage
Member 2: Producer
Member 3: Consumer + Consumer Groups
Member 4: Coordinator + Metadata + Leader Election + Replication Coordination

Required design constraint: they share the same protobuf schema and interface contracts.

## 24. Testing Strategy

Tests include:

- unit tests
- integration tests
- end-to-end tests
- failure/recovery tests
- performance tests

The key end-to-end scenario will validate:

1. start 3 brokers
2. start coordinator
3. create `orders` topic with 3 partitions
4. configure replication factor 3
5. start producer
6. send 10,000 messages
7. start consumer group
8. consume messages
9. verify ordering within each partition
10. kill partition leader
11. verify leader election
12. continue producing
13. verify consumers continue
14. restart failed broker
15. verify replica catch-up

## 25. Observability

The design adds structured logs, request IDs, broker IDs, topic and partition labels, and metrics.

Example metrics:

- messages/sec
- bytes/sec
- producer latency
- consumer latency
- replication lag
- active brokers
- consumer groups
- consumer lag
- failed requests
- leader elections

These metrics are used to detect bottlenecks, lag, and cluster instability.

## 26. Docker

The project is designed for a Docker Compose environment with:

- ZooKeeper
- Coordinator
- Broker 1
- Broker 2
- Broker 3

The system is intended to be startable with a single command.

## 27. Security

The initial version includes a basic security posture suitable for a student project, not a production enterprise deployment.

Areas considered:

- authentication
- authorization
- TLS
- secrets and environment config
- topic access control
- protection of admin APIs

Production systems would require stronger identities, mutual TLS, access control, secret management, and audit trails.

## 28. Configuration

Configuration values are not hard-coded.

Examples:

- broker IDs
- ports
- replication factor
- ack mode
- batch size
- batch timeout
- segment size
- retention period
- heartbeat interval
- replica lag threshold

A sample configuration file/environment variables will be provided with the implementation.

## 29. Development Process

The project is planned in phases.

Phase 1: system design + repository structure + shared Protobuf definitions
Phase 2: coordinator + broker registration + heartbeat
Phase 3: broker + partition + append-only storage
Phase 4: producer + partition routing + batching
Phase 5: consumer + offset management
Phase 6: consumer groups + rebalancing
Phase 7: replication + ISR
Phase 8: leader election + broker failure recovery
Phase 9: delivery semantics + retries
Phase 10: Docker + observability + security + performance testing

After each phase, the team will explain implementation, files changed, tests, run steps, verification, limitations, and wait for confirmation before moving on.

## 30. Coding Standards

The project should follow Java clean-code and distributed-systems expectations:

- clean architecture
- SOLID principles where appropriate
- interfaces for replaceable parts
- dependency injection
- immutability where practical
- proper exception handling
- timeouts and retries with limits
- thread-safe concurrency
- clear naming
- JavaDoc for important APIs
- no silent failure

## 31. Honest Disclosure

This system is not production-ready, and it must not be represented as such.

For every major feature, the project distinguishes between:

- IMPLEMENTED
- PARTIALLY IMPLEMENTED
- SIMULATED
- NOT IMPLEMENTED

This is especially important for distributed guarantees like exactly-once delivery.

## 32. Version 1 Scope vs Future Scope

Version 1 intentionally focuses on core queue behavior, not full enterprise production operation.

Implemented Version 1 highlights:

- partition-aware log storage
- leader-based writes
- persistent coordinator metadata via ZooKeeper
- synchronous ISR replication and high-watermark-based committed fetch
- consumer groups and offset tracking

Current simplifications:

- single coordinator process, no leader fencing or coordinator failover
- volatile consumer membership and assignment state
- request-triggered retention, no background segment cleaner
- no exactly-once transaction protocol, TLS, authentication, or production metrics

Future enhancements include:

- stronger exactly-once guarantees
- richer security and authorization
- multi-datacenter replication
- stronger admin tooling
- advanced observability
- smarter partition management

## 33. First Deliverable Requirements

This deliverable includes:

1. complete system architecture
2. component diagram
3. data-flow diagram
4. sequence diagrams
5. repository structure
6. Protobuf/API design
7. data models
8. failure-handling strategy
9. team responsibilities
10. development milestones
11. technology justification
12. version-1 scope vs future scope

This design is intentionally written plainly enough that all four team members can work independently without incompatible assumptions.
