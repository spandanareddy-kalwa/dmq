package com.dbq.producer;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.CoordinatorServiceGrpc;
import com.dbq.proto.Empty;
import com.dbq.proto.GetPartitionMetadataRequest;
import com.dbq.proto.GetTopicMetadataRequest;
import com.dbq.proto.MessageRecord;
import com.dbq.proto.PartitionMetadata;
import com.dbq.proto.ProduceRequest;
import com.dbq.proto.ProduceResponse;
import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class MessageProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final ProducerConfig config;
    private final String producerId = UUID.randomUUID().toString();
    private final ManagedChannel coordinatorChannel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinator;
    private final Map<String, BrokerConnection> brokerConnections = new ConcurrentHashMap<>();
    private final AtomicInteger unkeyedPartition = new AtomicInteger();
    private final Semaphore inFlightBatches;
    private volatile boolean closed;

    public MessageProducer(ProducerConfig config) {
        this.config = config;
        this.coordinatorChannel = ManagedChannelBuilder.forAddress(config.coordinatorHost(), config.coordinatorPort())
                .usePlaintext().build();
        this.coordinator = CoordinatorServiceGrpc.newBlockingStub(coordinatorChannel);
        this.inFlightBatches = new Semaphore(config.maxInFlightBatches());
    }

    public ProduceResponse send(String topic, ProducerRecord record) {
        return sendBatch(topic, List.of(record)).get(0);
    }

    public List<ProduceResponse> sendBatch(String topic, List<ProducerRecord> records) {
        ensureOpen();
        if (topic == null || topic.isBlank() || records == null) {
            throw new IllegalArgumentException("Topic and records are required");
        }
        if (records.isEmpty()) {
            return List.of();
        }
        if (records.size() > config.maxBatchMessages()) {
            throw new IllegalArgumentException("Batch exceeds configured message limit");
        }
        acquireCapacity();
        try {
            int partitionCount = topicPartitionCount(topic);
            Map<Integer, List<MessageRecord>> partitionBatches = new LinkedHashMap<>();
            for (ProducerRecord record : records) {
                int partition = selectPartition(record.key(), partitionCount, unkeyedPartition.getAndIncrement());
                MessageRecord message = MessageRecord.newBuilder()
                        .setMessageId(UUID.randomUUID().toString())
                        .setTopic(topic)
                        .setPartition(partition)
                        .setKey(record.key() == null ? "" : record.key())
                        .setPayload(record.payload())
                        .setTimestamp(System.currentTimeMillis())
                        .setSize(record.payload().size())
                        .putAllHeaders(record.headers())
                        .build();
                partitionBatches.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(message);
            }

            List<ProduceResponse> responses = new ArrayList<>(partitionBatches.size());
            for (Map.Entry<Integer, List<MessageRecord>> batch : partitionBatches.entrySet()) {
                responses.add(sendPartitionBatch(topic, batch.getKey(), batch.getValue()));
            }
            return List.copyOf(responses);
        } finally {
            inFlightBatches.release();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        coordinatorChannel.shutdown();
        closeChannelConnections();
        try {
            coordinatorChannel.awaitTermination(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    public static int selectPartition(String key, int partitionCount, int unkeyedSequence) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }
        int hash = key == null || key.isEmpty() ? unkeyedSequence : key.hashCode();
        return Math.floorMod(hash, partitionCount);
    }

    private int topicPartitionCount(String topic) {
        var response = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .getTopicMetadata(GetTopicMetadataRequest.newBuilder().setTopicName(topic).build());
        int count = response.getMetadata().getPartitionCount();
        if (count <= 0) {
            throw new IllegalArgumentException("Topic does not exist or has no partitions: " + topic);
        }
        return count;
    }

    private ProduceResponse sendPartitionBatch(String topic, int partition, List<MessageRecord> records) {
        StatusRuntimeException lastFailure = null;
        for (int attempt = 0; attempt <= config.maxRetries(); attempt++) {
            try {
                PartitionMetadata metadata = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                        .getPartitionMetadata(GetPartitionMetadataRequest.newBuilder()
                                .setTopicName(topic).setPartitionId(partition).build())
                        .getMetadata();
                if (metadata.getLeaderBrokerId().isBlank()) {
                    throw new IllegalStateException("Partition has no live leader: " + topic + "-" + partition);
                }
                BrokerInfo leader = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                        .getLiveBrokers(Empty.getDefaultInstance()).getBrokersList().stream()
                        .filter(broker -> broker.getBrokerId().equals(metadata.getLeaderBrokerId()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Leader is not registered as live: " + metadata.getLeaderBrokerId()));
                ProduceRequest request = ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .addAllMessages(records)
                        .setProducerId(producerId)
                        .setAckMode(config.ackMode())
                        .build();
                ProduceResponse response = brokerConnection(leader).stub
                        .withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                        .produce(request);
                if (!response.getSuccess()) {
                    throw Status.UNAVAILABLE.withDescription(response.getError()).asRuntimeException();
                }
                return response;
            } catch (StatusRuntimeException exception) {
                lastFailure = exception;
                if (attempt == config.maxRetries()) {
                    throw exception;
                }
                logger.warn("Produce attempt {} failed for {}-{}; refreshing metadata", attempt + 1, topic, partition);
                pauseBeforeRetry(attempt);
            }
        }
        throw lastFailure == null ? new IllegalStateException("Produce failed without a response") : lastFailure;
    }

    private BrokerConnection brokerConnection(BrokerInfo broker) {
        return brokerConnections.compute(broker.getBrokerId(), (brokerId, current) -> {
            if (current != null && current.host.equals(broker.getHost()) && current.port == broker.getPort()) {
                return current;
            }
            if (current != null) {
                current.channel.shutdownNow();
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(broker.getHost(), broker.getPort())
                    .usePlaintext().build();
            return new BrokerConnection(broker.getHost(), broker.getPort(), channel,
                    BrokerServiceGrpc.newBlockingStub(channel));
        });
    }

    private void acquireCapacity() {
        try {
            if (!inFlightBatches.tryAcquire(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Producer backpressure limit reached");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for producer capacity", exception);
        }
    }

    private void pauseBeforeRetry(int attempt) {
        long backoff = Math.min(config.retryBackoffMs() * (1L << Math.min(attempt, 20)), config.requestTimeoutMs());
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying produce", exception);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
    }

    private void closeChannelConnections() {
        for (BrokerConnection connection : brokerConnections.values()) {
            connection.channel.shutdown();
        }
        brokerConnections.clear();
    }

    private record BrokerConnection(String host, int port, ManagedChannel channel,
                                    BrokerServiceGrpc.BrokerServiceBlockingStub stub) {
    }
}