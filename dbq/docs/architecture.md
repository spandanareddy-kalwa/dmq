# Target Architecture Overview

This describes the target system and the implemented local cluster. The coordinator, broker, producer, and consumer flows run over gRPC; limitations such as single-coordinator operation and missing production fencing are listed in the repository README.

## Core architecture

The distributed message queue is organized around a metadata coordinator, a broker cluster, and consumer groups. Producers interact with the leader broker for a partition, brokers replicate to followers, and consumers pull data from the assigned partition leader.

## Data flow

Producer -> Coordinator -> Producer chooses partition leader -> Leader broker -> Append to WAL -> Follower replication -> Consumer fetch -> Offset commit

## Simplified component diagram

```text
Producer
   |
   v
Coordinator / Metadata Service
   |
   v
Broker Cluster
   +--> Broker 1 (Leader for P0)
   +--> Broker 2 (Follower for P0)
   +--> Broker 3 (Follower for P0)
   |
   v
Consumer Group A
   +--> Consumer 1
   +--> Consumer 2
```

## Why this design

This structure keeps the system understandable:

- metadata coordination is centralized but lightweight
- append-only logs provide durability and log replay
- leader/follower separation reduces complexity while still modeling real distributed queue behavior
- consumer groups allow independent offset tracking per group

## Key simplifications for the student version

- leader selection is performed by one coordinator using persisted metadata and broker heartbeat timeouts; it is not a consensus protocol
- exactly-once semantics are not claimed
- storage is a local file-based log rather than a distributed storage layer
- ACK_ALL replication is leader-driven and waits for the current ISR, but there is no coordinator fencing token

## Responsibility split

- Coordinator owns cluster metadata and liveness.
- Broker owns partition-local durability and replication.
- Producer owns send orchestration and batch behavior.
- Consumer owns fetch and commit behavior.

## Important constraint

This is not a toy queue. It is a distributed-system learning project, and every design decision is traceable to the behavior of a real queueing system without pretending it is a production-grade Kafka replacement.
