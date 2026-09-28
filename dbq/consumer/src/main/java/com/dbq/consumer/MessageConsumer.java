package com.dbq.consumer;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.CommitOffsetRequest;
import com.dbq.proto.CoordinatorServiceGrpc;
import com.dbq.proto.Empty;
import com.dbq.proto.FetchRequest;
import com.dbq.proto.GetCommittedOffsetRequest;
import com.dbq.proto.GetPartitionMetadataRequest;
import com.dbq.proto.HeartbeatConsumerGroupRequest;
import com.dbq.proto.JoinConsumerGroupRequest;
import com.dbq.proto.LeaveConsumerGroupRequest;
import com.dbq.proto.MessageRecord;
import com.dbq.proto.TopicPartition;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class MessageConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    private final ConsumerConfig config;
    private final ManagedChannel coordinatorChannel;
    private final CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinator;
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, BrokerConnection> brokerConnections = new ConcurrentHashMap<>();
    private final Map<PartitionKey, Long> positions = new HashMap<>();
    private final Set<PartitionKey> assignedPartitions = ConcurrentHashMap.newKeySet();
    private volatile List<String> subscribedTopics = List.of();
    private volatile boolean closed;

    public MessageConsumer(ConsumerConfig config) {
        this.config = config;
        this.coordinatorChannel = ManagedChannelBuilder.forAddress(config.coordinatorHost(), config.coordinatorPort())
                .usePlaintext().build();
        this.coordinator = CoordinatorServiceGrpc.newBlockingStub(coordinatorChannel);
    }

    public synchronized List<TopicPartition> subscribe(List<String> topics) {
        ensureOpen();
        if (topics == null || topics.isEmpty() || topics.stream().anyMatch(topic -> topic == null || topic.isBlank())) {
            throw new IllegalArgumentException("At least one non-empty topic is required");
        }
        subscribedTopics = topics.stream().distinct().sorted().toList();
        var response = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .joinConsumerGroup(JoinConsumerGroupRequest.newBuilder()
                        .setGroupId(config.groupId())
                        .setConsumerId(config.consumerId())
                        .addAllTopicNames(subscribedTopics)
                        .build());
        if (!response.getSuccess()) {
            throw new IllegalStateException(response.getMessage());
        }
        applyAssignment(response.getAssignedPartitionsList());
        heartbeats.scheduleWithFixedDelay(this::heartbeatSafely,
                config.heartbeatIntervalMs(), config.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        return response.getAssignedPartitionsList();
    }

    public synchronized List<MessageRecord> poll() {
        ensureOpen();
        List<PartitionKey> assigned = assignedPartitions.stream()
                .sorted().toList();
        if (assigned.isEmpty()) {
            return List.of();
        }
        List<MessageRecord> result = new ArrayList<>();
        int remaining = config.maxMessagesPerPoll();
        for (int index = 0; index < assigned.size() && remaining > 0; index++) {
            PartitionKey partition = assigned.get(index);
            int fairShare = Math.max(1, remaining / (assigned.size() - index));
            long position = positions.computeIfAbsent(partition, this::loadCommittedPosition);
            result.addAll(fetch(partition, position, Math.min(fairShare, remaining), index == 0));
            remaining = config.maxMessagesPerPoll() - result.size();
        }
        return List.copyOf(result);
    }

    public synchronized boolean commitSync() {
        ensureOpen();
        boolean committed = true;
        for (PartitionKey partition : assignedPartitions.stream().sorted().toList()) {
            long offset = positions.computeIfAbsent(partition, this::loadCommittedPosition);
            var response = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                    .commitOffset(CommitOffsetRequest.newBuilder()
                            .setConsumerGroup(config.groupId())
                            .setTopic(partition.topic())
                            .setPartition(partition.partition())
                            .setOffset(offset)
                            .build());
            committed &= response.getSuccess();
        }
        return committed;
    }

    public synchronized long position(String topic, int partition) {
        PartitionKey key = new PartitionKey(topic, partition);
        return positions.computeIfAbsent(key, this::loadCommittedPosition);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        heartbeats.shutdownNow();
        try {
            coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                    .leaveConsumerGroup(LeaveConsumerGroupRequest.newBuilder()
                            .setGroupId(config.groupId()).setConsumerId(config.consumerId()).build());
        } catch (RuntimeException exception) {
            logger.warn("Unable to leave consumer group {} cleanly", config.groupId(), exception);
        }
        coordinatorChannel.shutdown();
        for (BrokerConnection connection : brokerConnections.values()) {
            connection.channel.shutdown();
        }
        brokerConnections.clear();
    }

    private List<MessageRecord> fetch(PartitionKey partition, long offset, int maxMessages, boolean allowLongPoll) {
        var metadata = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .getPartitionMetadata(GetPartitionMetadataRequest.newBuilder()
                        .setTopicName(partition.topic()).setPartitionId(partition.partition()).build())
                .getMetadata();
        if (metadata.getLeaderBrokerId().isBlank()) {
            return List.of();
        }
        BrokerInfo leader = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .getLiveBrokers(Empty.getDefaultInstance()).getBrokersList().stream()
                .filter(broker -> broker.getBrokerId().equals(metadata.getLeaderBrokerId()))
                .findFirst()
                .orElse(null);
        if (leader == null) {
            return List.of();
        }
        long waitMs = allowLongPoll ? config.longPollTimeoutMs() : 0;
        long deadlineMs = Math.max(config.requestTimeoutMs(), waitMs + config.requestTimeoutMs());
        var response = brokerConnection(leader).stub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                .fetch(FetchRequest.newBuilder()
                        .setTopic(partition.topic())
                        .setPartition(partition.partition())
                        .setOffset(offset)
                        .setMaxMessages(maxMessages)
                        .setLongPoll(waitMs > 0)
                        .setLongPollTimeoutMs(waitMs)
                        .setConsumerGroup(config.groupId())
                        .build());
        positions.put(partition, response.getNextOffset());
        return response.getMessagesList();
    }

    private long loadCommittedPosition(PartitionKey partition) {
        var committed = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .getCommittedOffset(GetCommittedOffsetRequest.newBuilder()
                        .setConsumerGroup(config.groupId())
                        .setTopic(partition.topic())
                        .setPartition(partition.partition())
                        .build());
        return committed.getFound() ? committed.getOffset() : 0;
    }

    private void heartbeatSafely() {
        try {
            var response = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                    .heartbeatConsumerGroup(HeartbeatConsumerGroupRequest.newBuilder()
                            .setGroupId(config.groupId()).setConsumerId(config.consumerId()).build());
            if (!response.getAlive()) {
                rejoin();
            } else {
                applyAssignment(response.getAssignedPartitionsList());
            }
        } catch (StatusRuntimeException exception) {
            logger.warn("Consumer group heartbeat failed for {}: {}", config.groupId(), exception.getStatus());
        } catch (RuntimeException exception) {
            logger.error("Consumer group heartbeat failed for {}", config.groupId(), exception);
        }
    }

    private synchronized void rejoin() {
        if (closed || subscribedTopics.isEmpty()) {
            return;
        }
        var response = coordinator.withDeadlineAfter(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .joinConsumerGroup(JoinConsumerGroupRequest.newBuilder()
                        .setGroupId(config.groupId()).setConsumerId(config.consumerId())
                        .addAllTopicNames(subscribedTopics).build());
        if (response.getSuccess()) {
            applyAssignment(response.getAssignedPartitionsList());
        }
    }

    private synchronized void applyAssignment(List<TopicPartition> assignment) {
        Set<PartitionKey> updated = ConcurrentHashMap.newKeySet();
        for (TopicPartition topicPartition : assignment) {
            PartitionKey key = new PartitionKey(topicPartition.getTopic(), topicPartition.getPartition());
            updated.add(key);
            positions.computeIfAbsent(key, this::loadCommittedPosition);
        }
        assignedPartitions.clear();
        assignedPartitions.addAll(updated);
    }

    private BrokerConnection brokerConnection(BrokerInfo broker) {
        return brokerConnections.compute(broker.getBrokerId(), (brokerId, current) -> {
            if (current != null && current.host.equals(broker.getHost()) && current.port == broker.getPort()) {
                return current;
            }
            if (current != null) {
                current.channel.shutdownNow();
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(broker.getHost(), broker.getPort()).usePlaintext().build();
            return new BrokerConnection(broker.getHost(), broker.getPort(), channel,
                    BrokerServiceGrpc.newBlockingStub(channel));
        });
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
    }

    private record PartitionKey(String topic, int partition) implements Comparable<PartitionKey> {
        @Override
        public int compareTo(PartitionKey other) {
            int topicOrder = topic.compareTo(other.topic);
            return topicOrder == 0 ? Integer.compare(partition, other.partition) : topicOrder;
        }
    }

    private record BrokerConnection(String host, int port, ManagedChannel channel,
                                    BrokerServiceGrpc.BrokerServiceBlockingStub stub) {
    }
}