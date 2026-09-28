package com.dbq.broker;

import com.dbq.broker.storage.StorageManager;
import com.dbq.broker.storage.PartitionLog;
import com.dbq.proto.BrokerInfo;
import com.dbq.proto.HeartbeatRequest;
import com.dbq.proto.RegisterBrokerRequest;
import com.dbq.proto.CoordinatorServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class BrokerServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    private final BrokerConfig config;
    private final StorageManager storage;
    private final ManagedChannel coordinatorChannel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinator;
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
    private final Server server;
    private final BrokerServiceImpl brokerService;

    public BrokerServer(BrokerConfig config) throws IOException {
        this.config = config;
        this.storage = new StorageManager(config.dataDirectory(), config.segmentBytes(), config.maxRecordBytes());
        this.coordinatorChannel = ManagedChannelBuilder.forAddress(config.coordinatorHost(), config.coordinatorPort())
                .usePlaintext().build();
        this.coordinator = CoordinatorServiceGrpc.newBlockingStub(coordinatorChannel);
        this.brokerService = new BrokerServiceImpl(config.brokerId(), coordinator, storage, config.rpcTimeoutMs());
        this.server = ServerBuilder.forPort(config.port())
            .addService(brokerService)
                .build();
    }

    public void start() throws IOException {
        server.start();
        try {
            registerWithRetry();
        } catch (RuntimeException exception) {
            try {
                close();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw new IOException("Broker registration with coordinator failed", exception);
        }
        heartbeats.scheduleWithFixedDelay(this::heartbeatSafely,
                config.heartbeatIntervalMs(), config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        logger.info("Broker {} started on {}:{} using data directory {}", config.brokerId(),
                config.host(), config.port(), config.dataDirectory());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception exception) {
                logger.warn("Broker shutdown encountered an error", exception);
            }
        }));
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void close() throws IOException {
        heartbeats.shutdownNow();
        server.shutdown();
        try {
            if (!server.awaitTermination(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException exception) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
        brokerService.close();
        coordinatorChannel.shutdown();
        try {
            coordinatorChannel.awaitTermination(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        storage.close();
    }

    private void register() {
        BrokerInfo broker = BrokerInfo.newBuilder()
                .setBrokerId(config.brokerId())
                .setHost(config.host())
                .setPort(config.port())
                .setAlive(true)
                .build();
        var response = coordinator.withDeadlineAfter(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                .registerBroker(RegisterBrokerRequest.newBuilder().setBroker(broker).build());
        if (!response.getSuccess()) {
            throw new IllegalStateException(response.getMessage());
        }
        reconcileLocalPartitions();
    }

    private void reconcileLocalPartitions() {
        for (StorageManager.LocalPartition local : storage.localPartitions()) {
            var metadata = coordinator.withDeadlineAfter(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .getPartitionMetadata(com.dbq.proto.GetPartitionMetadataRequest.newBuilder()
                            .setTopicName(local.topic()).setPartitionId(local.partition()).build())
                    .getMetadata();
            if (metadata.getTopic().isBlank() || !metadata.getReplicaBrokerIdsList().contains(config.brokerId())) {
                continue;
            }
            try {
                PartitionLog log = storage.partitionLog(local.topic(), local.partition());
                long highWatermark = metadata.getHighWatermark();
                if (log.endOffset() > highWatermark && highWatermark >= log.logStartOffset()) {
                    log.truncateTo(highWatermark);
                }
                var progress = coordinator.withDeadlineAfter(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                        .updateReplicaProgress(com.dbq.proto.UpdateReplicaProgressRequest.newBuilder()
                                .setBrokerId(config.brokerId())
                                .setTopic(local.topic())
                                .setPartition(local.partition())
                                .setLogEndOffset(log.endOffset())
                                .build());
                if (!progress.getSuccess()) {
                    logger.warn("Broker {} could not report recovered partition {}-{}: {}",
                            config.brokerId(), local.topic(), local.partition(), progress.getMessage());
                }
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to reconcile recovered partition "
                        + local.topic() + "-" + local.partition(), exception);
            }
        }
    }

    private void registerWithRetry() {
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                register();
                return;
            } catch (RuntimeException exception) {
                lastFailure = exception;
                try {
                    Thread.sleep(Math.min(250L * (attempt + 1), config.rpcTimeoutMs()));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while registering broker", interrupted);
                }
            }
        }
        throw new IllegalStateException("Coordinator registration failed after retries", lastFailure);
    }

    private void heartbeatSafely() {
        try {
            var response = coordinator.withDeadlineAfter(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder()
                            .setBrokerId(config.brokerId())
                            .setTimestampMs(System.currentTimeMillis())
                            .build());
            if (!response.getAlive()) {
                register();
            }
        } catch (StatusRuntimeException exception) {
            logger.warn("Broker {} heartbeat failed: {}", config.brokerId(), exception.getStatus());
        } catch (RuntimeException exception) {
            logger.error("Broker {} heartbeat failed", config.brokerId(), exception);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (BrokerServer broker = new BrokerServer(BrokerConfig.fromEnvironment())) {
            broker.start();
            broker.awaitTermination();
        }
    }
}