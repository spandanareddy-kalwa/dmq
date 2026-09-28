# Distributed Message Queue

This repository implements a student-scale distributed message queue in Java 17 with Maven, gRPC, Protocol Buffers, local append-only logs, and ZooKeeper-backed coordinator metadata.

## Current status

Implemented:

- coordinator gRPC APIs for broker registration, heartbeats, topics, partitions, group membership, replica progress, and offsets
- ZooKeeper persistent topic/partition/offset metadata and ephemeral broker registrations
- broker gRPC produce/fetch/state/replication endpoints and recovered partition logs
- framed Protobuf log records, CRC32C validation, segment rolling, tail recovery/truncation, message-ID deduplication, and sealed-segment retention
- producer routing, explicit batching, cached channels, bounded retries, and in-flight backpressure
- pull consumer, round-robin group assignment, group heartbeats, and explicit offset commits
- ISR replication, high-watermarks, committed-only fetches, follower catch-up, and heartbeat-based leader failover
- one- and three-broker end-to-end tests, including leader failure

Partially implemented or not implemented:

- the coordinator is a single process; ZooKeeper persistence does not make the coordinator highly available or provide leader fencing
- consumer group membership is in memory and re-forms as clients heartbeat after restart
- retention is applied on broker requests, not by a background cleaner
- `ACK_0` is constrained by unary gRPC and still gets a response; all local appends currently force data to disk
- retries can still produce duplicates after failures outside the broker's durable message-ID deduplication window
- exactly-once transactions, TLS/authentication/authorization, production metrics, multi-datacenter replication, and performance certification

See [docs/system-design.md](docs/system-design.md) for the guarantee boundaries and target architecture.

## Repository structure

- `common/` - shared domain classes, config, utils, and common interfaces
- `proto/` - shared Protobuf messages and gRPC service definitions
- `coordinator/` - metadata service, broker registry, heartbeat, leader election coordination
- `broker/` - log storage, partition logic, replication, fetch/produce handling
- `producer/` - message-producing client with partition routing and batching
- `consumer/` - consumer client and offset tracking
- `integration-tests/` - end-to-end and failure-recovery validation
- `docs/` - architecture and system design documentation

## System design summary

The system is designed around four key layers:

1. Producers publish data to topics.
2. A coordinator tracks brokers, metadata, and leader information.
3. Brokers own partitions, append-only logs, and replica management.
4. Consumer groups pull data and track offsets independently.

## Important design rule

This repository is not claiming production readiness. The current version intentionally focuses on a realistic student implementation with explicit limitations and simplified coordination where required.

## Build and test

```sh
mvn test
mvn package
```

Run the local cluster with Docker Compose:

```sh
docker compose up --build
```

Compose starts ZooKeeper, the coordinator, and three brokers. Broker logs and ZooKeeper data use named volumes. Open `http://localhost:8080` for the coordinator admin console, where you can inspect cluster metadata, create topics, publish records, and fetch committed messages. The console is an unauthenticated local-development interface; do not expose it to an untrusted network. The gRPC API remains available on port `9090`.

### Run the console without Docker

After `mvn package`, start the coordinator in one terminal:

```sh
DBQ_ZOOKEEPER_CONNECT_STRING=memory DBQ_COORDINATOR_PORT=9090 DBQ_ADMIN_HTTP_PORT=8080 java -jar coordinator/target/coordinator-0.1.0-SNAPSHOT.jar
```

Start each broker in a separate terminal, changing the ID, port, and data directory:

```sh
DBQ_BROKER_ID=broker-1 DBQ_BROKER_HOST=localhost DBQ_BROKER_PORT=9091 DBQ_DATA_DIR=/tmp/dbq-broker-1 DBQ_COORDINATOR_HOST=localhost DBQ_COORDINATOR_PORT=9090 java -jar broker/target/broker-0.1.0-SNAPSHOT.jar
DBQ_BROKER_ID=broker-2 DBQ_BROKER_HOST=localhost DBQ_BROKER_PORT=9092 DBQ_DATA_DIR=/tmp/dbq-broker-2 DBQ_COORDINATOR_HOST=localhost DBQ_COORDINATOR_PORT=9090 java -jar broker/target/broker-0.1.0-SNAPSHOT.jar
DBQ_BROKER_ID=broker-3 DBQ_BROKER_HOST=localhost DBQ_BROKER_PORT=9093 DBQ_DATA_DIR=/tmp/dbq-broker-3 DBQ_COORDINATOR_HOST=localhost DBQ_COORDINATOR_PORT=9090 java -jar broker/target/broker-0.1.0-SNAPSHOT.jar
```

Open [http://localhost:8080](http://localhost:8080). The `memory` option skips ZooKeeper and loses coordinator metadata when the process stops; use Compose for the persistent local cluster.

The coordinator executable reads `DBQ_COORDINATOR_PORT`, `DBQ_ADMIN_HTTP_PORT`, `DBQ_HEARTBEAT_TIMEOUT_MS`, `DBQ_ZOOKEEPER_CONNECT_STRING`, and `DBQ_ZOOKEEPER_SESSION_TIMEOUT_MS`. Broker settings use `DBQ_BROKER_ID`, `DBQ_BROKER_HOST`, `DBQ_BROKER_PORT`, `DBQ_DATA_DIR`, `DBQ_COORDINATOR_HOST`, `DBQ_COORDINATOR_PORT`, `DBQ_HEARTBEAT_INTERVAL_MS`, `DBQ_SEGMENT_BYTES`, and `DBQ_MAX_RECORD_BYTES`. Producer and consumer settings are exposed through their respective config records and environment factories.

Docker startup was not exercised in this environment because Docker is unavailable. The embedded ZooKeeper test and Maven integration suite run without Docker.
