package com.dbq.coordinator;

import com.dbq.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorServer {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorServer.class);

    private final Server server;
    private final CoordinatorService service;
    private final ZooKeeperMetadataStore metadataStore;
    private final long heartbeatTimeoutMs;
    private final int adminPort;
    private final ScheduledExecutorService failureDetector = Executors.newSingleThreadScheduledExecutor();
    private AdminHttpServer adminServer;
    private boolean stopped;

    public CoordinatorServer(int port) {
        this(port, 15_000);
    }

    public CoordinatorServer(int port, long heartbeatTimeoutMs) {
        this(port, heartbeatTimeoutMs, null);
    }

    public CoordinatorServer(int port, long heartbeatTimeoutMs, ZooKeeperMetadataStore metadataStore) {
        this(port, heartbeatTimeoutMs, metadataStore, -1);
    }

    public CoordinatorServer(int port, long heartbeatTimeoutMs, ZooKeeperMetadataStore metadataStore, int adminPort) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.metadataStore = metadataStore;
        this.adminPort = adminPort;
        try {
            this.service = metadataStore == null
                    ? new CoordinatorService(heartbeatTimeoutMs, System::currentTimeMillis)
                    : new CoordinatorService(heartbeatTimeoutMs, System::currentTimeMillis, metadataStore);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to restore coordinator state", exception);
        }
        this.server = ServerBuilder.forPort(port)
                .addService(new CoordinatorGrpc(service))
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("Coordinator started on port {}", server.getPort());
        if (adminPort >= 0) {
            try {
                adminServer = new AdminHttpServer(adminPort, service);
                adminServer.start();
            } catch (IOException exception) {
                server.shutdownNow();
                throw exception;
            }
        }
        long checkIntervalMs = Math.max(1, heartbeatTimeoutMs / 2);
        failureDetector.scheduleWithFixedDelay(() -> {
            try {
                service.expireBrokers();
            } catch (RuntimeException exception) {
                logger.error("Coordinator failure detector pass failed", exception);
            }
        }, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                CoordinatorServer.this.stop();
            } catch (Exception e) {
                logger.warn("Shutdown interrupted", e);
            }
        }));
    }

    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        failureDetector.shutdownNow();
        if (adminServer != null) {
            adminServer.close();
        }
        if (server != null) {
            server.shutdown();
        }
        if (metadataStore != null) {
            try {
                metadataStore.close();
            } catch (IOException exception) {
                logger.warn("Unable to close ZooKeeper metadata session cleanly", exception);
            }
        }
    }

    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public int port() {
        return server.getPort();
    }

    public int adminPort() {
        if (adminServer == null) {
            throw new IllegalStateException("Admin HTTP server is disabled");
        }
        return adminServer.port();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(System.getenv().getOrDefault("DBQ_COORDINATOR_PORT", "9090"));
        long heartbeatTimeoutMs = Long.parseLong(System.getenv().getOrDefault("DBQ_HEARTBEAT_TIMEOUT_MS", "15000"));
        String zookeeperConnect = System.getenv().getOrDefault("DBQ_ZOOKEEPER_CONNECT_STRING", "localhost:2181");
        int sessionTimeoutMs = Integer.parseInt(System.getenv().getOrDefault("DBQ_ZOOKEEPER_SESSION_TIMEOUT_MS", "10000"));
        int adminPort = Integer.parseInt(System.getenv().getOrDefault("DBQ_ADMIN_HTTP_PORT", "8080"));
        ZooKeeperMetadataStore metadataStore = zookeeperConnect.isBlank() || zookeeperConnect.equalsIgnoreCase("memory")
            ? null
            : new ZooKeeperMetadataStore(zookeeperConnect, sessionTimeoutMs);
        CoordinatorServer coordinator = new CoordinatorServer(port, heartbeatTimeoutMs, metadataStore, adminPort);
        coordinator.start();
        coordinator.awaitTermination();
    }

    public static class CoordinatorGrpc extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
        private final CoordinatorService service;

        public CoordinatorGrpc(CoordinatorService service) {
            this.service = service;
        }

        @Override
        public void registerBroker(RegisterBrokerRequest request, StreamObserver<RegisterBrokerResponse> responseObserver) {
            responseObserver.onNext(service.registerBroker(request));
            responseObserver.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            responseObserver.onNext(service.heartbeat(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getLiveBrokers(Empty request, StreamObserver<GetLiveBrokersResponse> responseObserver) {
            responseObserver.onNext(service.getLiveBrokers(request));
            responseObserver.onCompleted();
        }

        @Override
        public void createTopic(CreateTopicRequest request, StreamObserver<CreateTopicResponse> responseObserver) {
            responseObserver.onNext(service.createTopic(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getTopicMetadata(GetTopicMetadataRequest request, StreamObserver<GetTopicMetadataResponse> responseObserver) {
            responseObserver.onNext(service.getTopicMetadata(request.getTopicName()));
            responseObserver.onCompleted();
        }

        @Override
        public void getPartitionMetadata(GetPartitionMetadataRequest request, StreamObserver<GetPartitionMetadataResponse> responseObserver) {
            responseObserver.onNext(service.getPartitionMetadata(request.getTopicName(), request.getPartitionId()));
            responseObserver.onCompleted();
        }

        @Override
        public void getLeader(GetLeaderRequest request, StreamObserver<GetLeaderResponse> responseObserver) {
            responseObserver.onNext(service.getLeader(request.getTopicName(), request.getPartitionId()));
            responseObserver.onCompleted();
        }

        @Override
        public void getReplicas(GetReplicasRequest request, StreamObserver<GetReplicasResponse> responseObserver) {
            responseObserver.onNext(service.getReplicas(request.getTopicName(), request.getPartitionId()));
            responseObserver.onCompleted();
        }

        @Override
        public void electLeader(ElectLeaderRequest request, StreamObserver<ElectLeaderResponse> responseObserver) {
            responseObserver.onNext(service.electLeader(request.getTopicName(), request.getPartitionId()));
            responseObserver.onCompleted();
        }

        @Override
        public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetResponse> responseObserver) {
            responseObserver.onNext(service.commitOffset(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getCommittedOffset(GetCommittedOffsetRequest request,
                                       StreamObserver<GetCommittedOffsetResponse> responseObserver) {
            responseObserver.onNext(service.getCommittedOffset(request));
            responseObserver.onCompleted();
        }

        @Override
        public void joinConsumerGroup(JoinConsumerGroupRequest request,
                                     StreamObserver<JoinConsumerGroupResponse> responseObserver) {
            responseObserver.onNext(service.joinConsumerGroup(request));
            responseObserver.onCompleted();
        }

        @Override
        public void heartbeatConsumerGroup(HeartbeatConsumerGroupRequest request,
                                           StreamObserver<HeartbeatConsumerGroupResponse> responseObserver) {
            responseObserver.onNext(service.heartbeatConsumerGroup(request));
            responseObserver.onCompleted();
        }

        @Override
        public void leaveConsumerGroup(LeaveConsumerGroupRequest request,
                                       StreamObserver<LeaveConsumerGroupResponse> responseObserver) {
            responseObserver.onNext(service.leaveConsumerGroup(request));
            responseObserver.onCompleted();
        }

        @Override
        public void updateReplicaProgress(UpdateReplicaProgressRequest request,
                                          StreamObserver<UpdateReplicaProgressResponse> responseObserver) {
            responseObserver.onNext(service.updateReplicaProgress(request));
            responseObserver.onCompleted();
        }
    }

}
