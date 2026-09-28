package com.dbq.broker;

import com.dbq.broker.storage.PartitionLog;
import com.dbq.broker.storage.StorageManager;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.FetchRequest;
import com.dbq.proto.FetchResponse;
import com.dbq.proto.GetPartitionStateRequest;
import com.dbq.proto.GetPartitionStateResponse;
import com.dbq.proto.MessageRecord;
import com.dbq.proto.PartitionMetadata;
import com.dbq.proto.ProduceRequest;
import com.dbq.proto.ProduceResponse;
import com.dbq.proto.ReplicateRequest;
import com.dbq.proto.ReplicateResponse;
import com.dbq.proto.BrokerInfo;
import com.dbq.proto.Empty;
import com.dbq.proto.GetLiveBrokersResponse;
import com.dbq.proto.GetTopicMetadataRequest;
import com.dbq.proto.UpdateReplicaProgressRequest;
import com.dbq.proto.CoordinatorServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerServiceImpl extends BrokerServiceGrpc.BrokerServiceImplBase implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private final String brokerId;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinator;
    private final StorageManager storage;
    private final long rpcTimeoutMs;
    private final Map<String, ReplicaConnection> replicaConnections = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();

    public BrokerServiceImpl(String brokerId,
                             CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinator,
                             StorageManager storage,
                             long rpcTimeoutMs) {
        this.brokerId = brokerId;
        this.coordinator = coordinator;
        this.storage = storage;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        try {
            PartitionMetadata metadata = requireLeader(request.getTopic(), request.getPartition());
            String partitionKey = request.getTopic() + "#" + request.getPartition();
            ReentrantLock lock = partitionLocks.computeIfAbsent(partitionKey, ignored -> new ReentrantLock());
            lock.lock();
            try {
                PartitionLog log = storage.partitionLog(request.getTopic(), request.getPartition());
                applyRetention(request.getTopic(), request.getPartition(), log);
                long baseOffset = log.endOffset();
                List<MessageRecord> appended = log.append(request.getMessagesList());
                long responseBaseOffset = appended.isEmpty() ? baseOffset : appended.get(0).getOffset();
                long lastOffset = appended.isEmpty() ? baseOffset - 1 : appended.get(appended.size() - 1).getOffset();
                updateReplicaProgress(request.getTopic(), request.getPartition(), log.endOffset());
                if (request.getAckMode() == com.dbq.proto.ProducerAckMode.AckLevel.ACK_ALL) {
                    try {
                        replicateToReplicas(request, metadata, log);
                    } catch (RuntimeException exception) {
                        responseObserver.onNext(ProduceResponse.newBuilder().setSuccess(false)
                                .setBaseOffset(responseBaseOffset).setLastOffset(lastOffset)
                                .setError("Leader append completed but not all ISR replicas acknowledged; retry may duplicate: "
                                        + exception.getMessage())
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }
                }
                responseObserver.onNext(ProduceResponse.newBuilder()
                        .setSuccess(true)
                    .setBaseOffset(responseBaseOffset)
                        .setLastOffset(lastOffset)
                        .build());
                responseObserver.onCompleted();
            } finally {
                lock.unlock();
            }
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (IOException exception) {
            logger.error("Broker {} failed to append {} partition {}", brokerId, request.getTopic(), request.getPartition(), exception);
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to append partition log")
                    .withCause(exception).asRuntimeException());
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(exception.getMessage())
                    .withCause(exception).asRuntimeException());
        }
    }

    @Override
    public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
        try {
            PartitionMetadata metadata = requireLeader(request.getTopic(), request.getPartition());
            PartitionLog log = storage.partitionLog(request.getTopic(), request.getPartition());
            applyRetention(request.getTopic(), request.getPartition(), log);
            long waitMs = request.getLongPoll() ? request.getLongPollTimeoutMs() : 0;
            List<MessageRecord> messages = log.fetch(request.getOffset(), request.getMaxMessages(), waitMs).stream()
                    .filter(message -> message.getOffset() < visibleEndOffset(metadata, log))
                    .toList();
            long nextOffset = messages.isEmpty()
                    ? request.getOffset()
                    : messages.get(messages.size() - 1).getOffset() + 1;
            responseObserver.onNext(FetchResponse.newBuilder()
                    .addAllMessages(messages)
                    .setNextOffset(nextOffset)
                    .setHasMore(visibleEndOffset(metadata, log) > nextOffset)
                    .build());
            responseObserver.onCompleted();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.CANCELLED.withDescription("Fetch was interrupted").asRuntimeException());
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (IOException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to read partition log")
                    .withCause(exception).asRuntimeException());
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(exception.getMessage())
                    .withCause(exception).asRuntimeException());
        }
    }

    @Override
    public void getPartitionState(GetPartitionStateRequest request,
                                  StreamObserver<GetPartitionStateResponse> responseObserver) {
        try {
            PartitionMetadata metadata = requireHosted(request.getTopic(), request.getPartition());
            PartitionLog log = storage.partitionLog(request.getTopic(), request.getPartition());
                applyRetention(request.getTopic(), request.getPartition(), log);
            long endOffset = log.endOffset();
                long highWatermark = metadata.getHighWatermark();
                PartitionMetadata current = metadata.toBuilder()
                    .setLogEndOffset(endOffset)
                    .setLogStartOffset(log.logStartOffset())
                    .build();
            responseObserver.onNext(GetPartitionStateResponse.newBuilder()
                    .setMetadata(current)
                    .setLastOffset(endOffset - 1)
                    .setHighWatermark(highWatermark)
                    .build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (IOException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to read partition state")
                    .withCause(exception).asRuntimeException());
        }
    }

    @Override
    public void replicate(ReplicateRequest request, StreamObserver<ReplicateResponse> responseObserver) {
        try {
            PartitionMetadata metadata = requireHosted(request.getTopic(), request.getPartition());
                if (metadata.getLeaderBrokerId().equals(brokerId)
                    || !metadata.getLeaderBrokerId().equals(request.getSourceBrokerId())) {
                responseObserver.onNext(ReplicateResponse.newBuilder().setSuccess(false)
                        .setMessage("Replication source is not the current partition leader").build());
                responseObserver.onCompleted();
                return;
            }
            PartitionLog log = storage.partitionLog(request.getTopic(), request.getPartition());
            if (log.endOffset() != request.getStartOffset()) {
                responseObserver.onNext(ReplicateResponse.newBuilder().setSuccess(false)
                        .setMessage("Replica offset does not match replication start offset")
                        .setReplicatedOffset(log.endOffset()).build());
                responseObserver.onCompleted();
                return;
            }
            for (int index = 0; index < request.getMessagesCount(); index++) {
                if (request.getMessages(index).getOffset() != request.getStartOffset() + index) {
                    responseObserver.onNext(ReplicateResponse.newBuilder().setSuccess(false)
                            .setMessage("Replicated records are not contiguous").build());
                    responseObserver.onCompleted();
                    return;
                }
            }
            List<MessageRecord> replicated = log.append(request.getMessagesList());
            applyRetention(request.getTopic(), request.getPartition(), log);
            updateReplicaProgress(request.getTopic(), request.getPartition(), log.endOffset());
            long replicatedOffset = replicated.isEmpty() ? request.getStartOffset() - 1
                    : replicated.get(replicated.size() - 1).getOffset();
            responseObserver.onNext(ReplicateResponse.newBuilder().setSuccess(true)
                    .setMessage("Replica append complete").setReplicatedOffset(replicatedOffset).build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (IOException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription("Failed to append replica records")
                    .withCause(exception).asRuntimeException());
        }
    }

    private void replicateToReplicas(ProduceRequest request, PartitionMetadata metadata,
                                     PartitionLog leaderLog) throws IOException {
        long leaderEndOffset = leaderLog.endOffset();
        GetLiveBrokersResponse live = coordinator.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                .getLiveBrokers(Empty.getDefaultInstance());
        for (String replicaId : metadata.getReplicaBrokerIdsList()) {
            if (replicaId.equals(brokerId)) {
                continue;
            }
            boolean requiredForAckAll = metadata.getIsrBrokerIdsList().contains(replicaId);
            try {
                BrokerInfo replica = live.getBrokersList().stream()
                        .filter(candidate -> candidate.getBrokerId().equals(replicaId))
                        .findFirst()
                        .orElseThrow(() -> Status.UNAVAILABLE.withDescription("Replica broker is not live: " + replicaId)
                                .asRuntimeException());
                BrokerServiceGrpc.BrokerServiceBlockingStub replicaStub = replicaConnection(replica).stub;
                long replicaEndOffset = replicaStub.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .getPartitionState(GetPartitionStateRequest.newBuilder()
                                .setTopic(request.getTopic()).setPartition(request.getPartition()).build())
                        .getMetadata().getLogEndOffset();
                if (replicaEndOffset > leaderEndOffset) {
                    throw Status.FAILED_PRECONDITION.withDescription("Replica broker " + replicaId
                            + " is ahead of the leader log").asRuntimeException();
                }
                while (replicaEndOffset < leaderEndOffset) {
                    int batchSize = (int) Math.min(500, leaderEndOffset - replicaEndOffset);
                    List<MessageRecord> missing;
                    try {
                        missing = leaderLog.fetch(replicaEndOffset, batchSize, 0);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while reading records for replication", exception);
                    }
                    if (missing.isEmpty()) {
                        throw Status.FAILED_PRECONDITION.withDescription("Leader log is missing records required for replica catch-up")
                                .asRuntimeException();
                    }
                    ReplicateResponse response = replicaStub.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                            .replicate(ReplicateRequest.newBuilder()
                                    .setSourceBrokerId(brokerId)
                                    .setTopic(request.getTopic())
                                    .setPartition(request.getPartition())
                                    .setStartOffset(replicaEndOffset)
                                    .addAllMessages(missing)
                                    .build());
                    long expectedEndOffset = replicaEndOffset + missing.size();
                    if (!response.getSuccess() || response.getReplicatedOffset() + 1 != expectedEndOffset) {
                        throw Status.UNAVAILABLE.withDescription("Replica broker " + replicaId + " rejected replication: "
                                + response.getMessage()).asRuntimeException();
                    }
                    replicaEndOffset = expectedEndOffset;
                }
            } catch (RuntimeException exception) {
                if (requiredForAckAll) {
                    throw exception;
                }
                logger.warn("Best-effort catch-up failed for non-ISR replica {}", replicaId, exception);
            }
        }
    }

    private void updateReplicaProgress(String topic, int partition, long endOffset) {
        var response = coordinator.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                .updateReplicaProgress(UpdateReplicaProgressRequest.newBuilder()
                        .setBrokerId(brokerId)
                        .setTopic(topic)
                        .setPartition(partition)
                        .setLogEndOffset(endOffset)
                        .build());
        if (!response.getSuccess()) {
            throw Status.FAILED_PRECONDITION.withDescription(response.getMessage()).asRuntimeException();
        }
    }

    private void applyRetention(String topic, int partition, PartitionLog log) throws IOException {
        long retentionMs = coordinator.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                .getTopicMetadata(GetTopicMetadataRequest.newBuilder().setTopicName(topic).build())
                .getMetadata().getRetentionMs();
        if (retentionMs > 0 && log.deleteExpiredSegments(retentionMs, System.currentTimeMillis()) > 0) {
            updateReplicaProgress(topic, partition, log.endOffset());
        }
    }

    private ReplicaConnection replicaConnection(BrokerInfo broker) {
        return replicaConnections.compute(broker.getBrokerId(), (replicaId, current) -> {
            if (current != null && current.host.equals(broker.getHost()) && current.port == broker.getPort()) {
                return current;
            }
            if (current != null) {
                current.channel.shutdownNow();
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(broker.getHost(), broker.getPort())
                    .usePlaintext().build();
            return new ReplicaConnection(broker.getHost(), broker.getPort(), channel,
                    BrokerServiceGrpc.newBlockingStub(channel));
        });
    }

    @Override
    public void close() {
        replicaConnections.values().forEach(connection -> connection.channel.shutdown());
        replicaConnections.clear();
    }

    private PartitionMetadata requireLeader(String topic, int partition) {
        PartitionMetadata metadata = requireHosted(topic, partition);
        if (!metadata.getLeaderBrokerId().equals(brokerId)) {
            throw Status.FAILED_PRECONDITION.withDescription("Broker is not the current partition leader").asRuntimeException();
        }
        return metadata;
    }

    private PartitionMetadata requireHosted(String topic, int partition) {
        PartitionMetadata metadata = coordinator.withDeadlineAfter(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                .getPartitionMetadata(com.dbq.proto.GetPartitionMetadataRequest.newBuilder()
                        .setTopicName(topic)
                        .setPartitionId(partition)
                        .build())
                .getMetadata();
        if (metadata.getTopic().isBlank()) {
            throw Status.NOT_FOUND.withDescription("Partition metadata was not found").asRuntimeException();
        }
        if (!metadata.getReplicaBrokerIdsList().contains(brokerId)) {
            throw Status.PERMISSION_DENIED.withDescription("Partition is not assigned to this broker").asRuntimeException();
        }
        return metadata;
    }

    private long visibleEndOffset(PartitionMetadata metadata, PartitionLog log) {
        return metadata.getReplicaBrokerIdsCount() == 1 ? log.endOffset() : metadata.getHighWatermark();
    }

    private record ReplicaConnection(String host, int port, ManagedChannel channel,
                                     BrokerServiceGrpc.BrokerServiceBlockingStub stub) {
    }
}