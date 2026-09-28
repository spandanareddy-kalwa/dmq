# Sequence Diagrams

These diagrams describe the implemented local flow where available. Guarantees remain subject to the single-coordinator, no-fencing limitations documented in the repository README.

## Producing a message

```mermaid
sequenceDiagram
    participant P as Producer
    participant C as Coordinator
    participant B as Broker Leader
    participant F as Broker Follower

    P->>C: get topic + partition metadata
    C-->>P: partition count, leader, replicas, ISR
    P->>B: append batch (topic, partition, messages)
    B->>B: append to local log and force
    alt ACK_ALL
        B->>F: replicate missing records to ISR
        F-->>B: append and report log end
        B->>C: update replica progress / high-watermark
    else ACK_1
        Note over B,F: Followers may lag; consumers stop at high-watermark
    end
    B-->>P: produce result
```

## Consuming a message

```mermaid
sequenceDiagram
    participant G as Consumer Group
    participant C as Consumer
    participant B as Partition Leader

    C-->>G: round-robin partition assignment
    C-->>C: read group's committed offset
    C->>B: fetch messages(offset, maxBytes)
    B-->>C: committed batch + next offset
    C->>C: process messages
    C->>C: commitSync()
    C->>G: commit group/topic/partition offset
    G-->>C: commit ack
```

## Broker failure

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant B1 as Broker 1
    participant B2 as Broker 2
    participant B3 as Broker 3

    B1--xC: heartbeats stop
    C->>C: heartbeat timeout expires
    C->>C: mark Broker 1 DEAD
    C->>C: remove Broker 1 from ISR and choose live ISR member
    C->>C: persist updated partition metadata
    Note over B2,B3: Brokers validate leader metadata on each produce/fetch
```

## Leader election

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant B1 as Old Leader
    participant B2 as Candidate Leader
    participant B3 as Follower

    C->>C: detect B1 unavailable
    C->>B2: validate ISR and liveness
    B2-->>C: eligible
    C->>B2: elect leader
    C->>B3: update replica metadata
    B2-->>C: leadership confirmed
```

## Consumer rebalance

```mermaid
sequenceDiagram
    participant G as Coordinator / Group Manager
    participant C1 as Consumer 1
    participant C2 as Consumer 2
    participant C3 as Consumer 3

    C2->>G: join or leave group
    G->>G: compute assignment
    G-->>C1: assign partition set A
    G-->>C2: assign partition set B
    G-->>C3: assign partition set C
    C1->>C1: fetch from new partition
    C2->>C2: fetch from new partition
    C3->>C3: fetch from new partition
```
