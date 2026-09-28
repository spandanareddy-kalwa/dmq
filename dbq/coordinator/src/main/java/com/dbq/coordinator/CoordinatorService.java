package com.dbq.coordinator;

import com.dbq.proto.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class CoordinatorService {

    private final long heartbeatTimeoutMs;
    private final LongSupplier currentTimeMs;
    private final ZooKeeperMetadataStore metadataStore;
    private final Map<String, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final Map<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> partitionsByTopic = new ConcurrentHashMap<>();
    private final Map<String, String> leaderAssignments = new ConcurrentHashMap<>();
    private final Map<String, List<String>> replicaAssignments = new ConcurrentHashMap<>();
    private final Map<OffsetKey, Long> committedOffsets = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> replicaProgress = new ConcurrentHashMap<>();
    private final Map<String, ConsumerGroupState> consumerGroups = new HashMap<>();

    public CoordinatorService() {
        this(15_000, System::currentTimeMillis);
    }

    public CoordinatorService(long heartbeatTimeoutMs, LongSupplier currentTimeMs) {
        if (heartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException("Heartbeat timeout must be positive");
        }
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.currentTimeMs = Objects.requireNonNull(currentTimeMs, "currentTimeMs");
        this.metadataStore = null;
    }

    public CoordinatorService(long heartbeatTimeoutMs, LongSupplier currentTimeMs,
                              ZooKeeperMetadataStore metadataStore) throws java.io.IOException {
        if (heartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException("Heartbeat timeout must be positive");
        }
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.currentTimeMs = Objects.requireNonNull(currentTimeMs, "currentTimeMs");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        restoreMetadata();
    }

    public synchronized RegisterBrokerResponse registerBroker(RegisterBrokerRequest request) {
        return registerBroker(request.getBroker());
    }

    public synchronized RegisterBrokerResponse registerBroker(BrokerInfo broker) {
        if (broker == null || broker.getBrokerId().isBlank()) {
            return RegisterBrokerResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Broker id is required")
                    .build();
        }

        BrokerInfo registered = broker.toBuilder()
                .setAlive(true)
                .setLastHeartbeatMs(currentTimeMs.getAsLong())
            .build();
        persistBroker(registered);
        brokers.put(broker.getBrokerId(), registered);
        return RegisterBrokerResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Broker registered")
                .build();
    }

    public synchronized HeartbeatResponse heartbeat(HeartbeatRequest request) {
        String brokerId = request.getBrokerId();
        BrokerInfo existing = brokers.get(brokerId);
        if (existing == null) {
            return HeartbeatResponse.newBuilder().setAlive(false).setMessage("Unknown broker").build();
        }

        BrokerInfo updated = existing.toBuilder()
                .setAlive(true)
            .setLastHeartbeatMs(currentTimeMs.getAsLong())
                .build();
        persistBroker(updated);
        brokers.put(brokerId, updated);

        return HeartbeatResponse.newBuilder().setAlive(true).setMessage("Heartbeat accepted").build();
    }

    public synchronized int expireBrokers() {
        long now = currentTimeMs.getAsLong();
        expireConsumerMembers(now);
        Set<String> expiredBrokerIds = new HashSet<>();
        brokers.replaceAll((brokerId, broker) -> {
            if (broker.getAlive() && now - broker.getLastHeartbeatMs() > heartbeatTimeoutMs) {
                expiredBrokerIds.add(brokerId);
                return broker.toBuilder().setAlive(false).build();
            }
            return broker;
        });

        if (expiredBrokerIds.isEmpty()) {
            return 0;
        }
        expiredBrokerIds.forEach(this::removePersistedBroker);

        for (Map.Entry<String, Map<Integer, PartitionMetadata>> topic : partitionsByTopic.entrySet()) {
            topic.getValue().replaceAll((partitionId, partition) -> {
                PartitionMetadata updated = updatePartitionAfterFailure(topic.getKey(), partition, expiredBrokerIds);
                persistPartition(updated);
                return updated;
            });
        }
        return expiredBrokerIds.size();
    }

    private PartitionMetadata updatePartitionAfterFailure(String topicName,
                                                          PartitionMetadata partition,
                                                          Set<String> expiredBrokerIds) {
        List<String> inSyncReplicas = partition.getIsrBrokerIdsList().stream()
                .filter(brokerId -> !expiredBrokerIds.contains(brokerId))
                .filter(brokerId -> {
                    BrokerInfo broker = brokers.get(brokerId);
                    return broker != null && broker.getAlive();
                })
                .toList();
        String leader = inSyncReplicas.contains(partition.getLeaderBrokerId())
                ? partition.getLeaderBrokerId()
                : inSyncReplicas.stream().findFirst().orElse("");

        String key = topicKey(topicName, partition.getPartitionId());
        long highWatermark = Math.max(partition.getHighWatermark(), inSyncReplicas.stream()
            .mapToLong(brokerId -> replicaProgress.getOrDefault(key, Collections.emptyMap())
                .getOrDefault(brokerId, 0L))
            .min().orElse(partition.getHighWatermark()));
        leaderAssignments.put(key, leader);
        return partition.toBuilder()
                .setLeaderBrokerId(leader)
            .setHighWatermark(highWatermark)
                .clearIsrBrokerIds()
                .addAllIsrBrokerIds(inSyncReplicas)
                .build();
    }

    public GetLiveBrokersResponse getLiveBrokers(Empty request) {
        List<BrokerInfo> live = new ArrayList<>();
        for (BrokerInfo broker : brokers.values()) {
            if (broker.getAlive()) {
                live.add(broker);
            }
        }
        live.sort(Comparator.comparing(BrokerInfo::getBrokerId));
        return GetLiveBrokersResponse.newBuilder().addAllBrokers(live).build();
    }

        public synchronized AdminOverview adminOverview() {
        List<BrokerView> brokerViews = brokers.values().stream()
            .sorted(Comparator.comparing(BrokerInfo::getBrokerId))
            .map(broker -> new BrokerView(broker.getBrokerId(), broker.getHost(), broker.getPort(),
                broker.getAlive(), broker.getLastHeartbeatMs()))
            .toList();
        List<TopicView> topicViews = topics.values().stream()
            .sorted(Comparator.comparing(TopicMetadata::getName))
            .map(topic -> new TopicView(topic.getName(), topic.getPartitionCount(),
                topic.getReplicationFactor(), topic.getRetentionMs(), topic.getSegmentSizeBytes()))
            .toList();
        List<PartitionView> partitionViews = partitionsByTopic.values().stream()
            .flatMap(partitions -> partitions.values().stream())
            .sorted(Comparator.comparing(PartitionMetadata::getTopic)
                .thenComparingInt(PartitionMetadata::getPartitionId))
            .map(partition -> {
                String key = topicKey(partition.getTopic(), partition.getPartitionId());
                long logEndOffset = replicaProgress.getOrDefault(key, Collections.emptyMap()).values().stream()
                        .mapToLong(Long::longValue).max().orElse(partition.getLogEndOffset());
                return new PartitionView(partition.getTopic(), partition.getPartitionId(),
                        partition.getLeaderBrokerId(), partition.getReplicaBrokerIdsList(),
                        partition.getIsrBrokerIdsList(), partition.getHighWatermark(),
                        partition.getLogStartOffset(), logEndOffset);
            })
            .toList();
        List<ConsumerGroupView> groupViews = consumerGroups.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> new ConsumerGroupView(entry.getKey(), entry.getValue().members.size(),
                entry.getValue().generationId,
                entry.getValue().assignments.values().stream().mapToInt(List::size).sum()))
            .toList();
        List<OffsetView> offsetViews = committedOffsets.entrySet().stream()
            .sorted(Comparator.comparing((Map.Entry<OffsetKey, Long> entry) -> entry.getKey().consumerGroup())
                .thenComparing(entry -> entry.getKey().topic())
                .thenComparingInt(entry -> entry.getKey().partition()))
            .map(entry -> new OffsetView(entry.getKey().consumerGroup(), entry.getKey().topic(),
                entry.getKey().partition(), entry.getValue()))
            .toList();
        return new AdminOverview(currentTimeMs.getAsLong(), brokerViews, topicViews,
            partitionViews, groupViews, offsetViews);
        }

    public synchronized CreateTopicResponse createTopic(CreateTopicRequest request) {
        if (request.getTopicName().isBlank()) {
            return CreateTopicResponse.newBuilder().setSuccess(false).setMessage("Topic name is required").build();
        }

        if (request.getPartitionCount() <= 0) {
            return CreateTopicResponse.newBuilder().setSuccess(false).setMessage("Partition count must be positive").build();
        }

        if (request.getReplicationFactor() <= 0) {
            return CreateTopicResponse.newBuilder().setSuccess(false).setMessage("Replication factor must be positive").build();
        }
        if (topics.containsKey(request.getTopicName())) {
            return CreateTopicResponse.newBuilder().setSuccess(false).setMessage("Topic already exists").build();
        }

        List<String> liveBrokers = brokers.values().stream()
                .filter(BrokerInfo::getAlive)
                .map(BrokerInfo::getBrokerId)
                .sorted()
                .toList();
        if (liveBrokers.isEmpty()) {
            return CreateTopicResponse.newBuilder().setSuccess(false).setMessage("No live brokers are registered").build();
        }
        if (request.getReplicationFactor() > liveBrokers.size()) {
            return CreateTopicResponse.newBuilder().setSuccess(false)
                    .setMessage("Replication factor exceeds the number of live brokers").build();
        }

        TopicMetadata metadata = TopicMetadata.newBuilder()
                .setName(request.getTopicName())
                .setPartitionCount(request.getPartitionCount())
                .setReplicationFactor(request.getReplicationFactor())
                .setRetentionMs(request.getRetentionMs())
                .setSegmentSizeBytes(request.getSegmentSizeBytes())
                .build();

        topics.put(request.getTopicName(), metadata);

        Map<Integer, PartitionMetadata> partitions = new HashMap<>();
        for (int i = 0; i < request.getPartitionCount(); i++) {
            List<String> replicas = new ArrayList<>();
            for (int r = 0; r < request.getReplicationFactor(); r++) {
                int index = (i + r) % liveBrokers.size();
                replicas.add(liveBrokers.get(index));
            }
            String leader = replicas.get(0);

            PartitionMetadata partition = PartitionMetadata.newBuilder()
                    .setTopic(request.getTopicName())
                    .setPartitionId(i)
                    .setLeaderBrokerId(leader)
                    .addAllReplicaBrokerIds(replicas)
                    .addAllIsrBrokerIds(replicas)
                    .setHighWatermark(0)
                    .setLogStartOffset(0)
                    .setLogEndOffset(0)
                    .build();

            partitions.put(i, partition);
            leaderAssignments.put(topicKey(request.getTopicName(), i), leader);
            replicaAssignments.put(topicKey(request.getTopicName(), i), replicas);
            Map<String, Long> progress = new ConcurrentHashMap<>();
            replicas.forEach(brokerId -> progress.put(brokerId, 0L));
            replicaProgress.put(topicKey(request.getTopicName(), i), progress);
        }

        partitionsByTopic.put(request.getTopicName(), partitions);
        persistTopic(metadata);
        partitions.values().forEach(this::persistPartition);

        return CreateTopicResponse.newBuilder().setSuccess(true).setMessage("Topic created").build();
    }

    public GetTopicMetadataResponse getTopicMetadata(String topicName) {
        TopicMetadata metadata = topics.get(topicName);
        if (metadata == null) {
            return GetTopicMetadataResponse.newBuilder().setMetadata(TopicMetadata.newBuilder().setName(topicName).setPartitionCount(0)).build();
        }
        return GetTopicMetadataResponse.newBuilder().setMetadata(metadata).build();
    }

    public GetPartitionMetadataResponse getPartitionMetadata(String topicName, int partitionId) {
        PartitionMetadata metadata = partitionsByTopic.getOrDefault(topicName, Collections.emptyMap()).get(partitionId);
        if (metadata == null) {
            return GetPartitionMetadataResponse.newBuilder().build();
        }
        return GetPartitionMetadataResponse.newBuilder().setMetadata(metadata).build();
    }

    public GetLeaderResponse getLeader(String topicName, int partitionId) {
        String leader = leaderAssignments.get(topicKey(topicName, partitionId));
        if (leader == null) {
            return GetLeaderResponse.newBuilder().setLeaderBrokerId("").build();
        }
        return GetLeaderResponse.newBuilder().setLeaderBrokerId(leader).build();
    }

    public GetReplicasResponse getReplicas(String topicName, int partitionId) {
        List<String> replicas = replicaAssignments.getOrDefault(topicKey(topicName, partitionId), Collections.emptyList());
        return GetReplicasResponse.newBuilder().addAllBrokerIds(replicas).build();
    }

    public synchronized ElectLeaderResponse electLeader(String topicName, int partitionId) {
        List<String> replicas = replicaAssignments.getOrDefault(topicKey(topicName, partitionId), Collections.emptyList());
        if (replicas.isEmpty()) {
            return ElectLeaderResponse.newBuilder().setSuccess(false).setLeaderBrokerId("").build();
        }

        PartitionMetadata partition = partitionsByTopic.getOrDefault(topicName, Collections.emptyMap()).get(partitionId);
        List<String> eligibleReplicas = partition == null ? Collections.emptyList() : partition.getIsrBrokerIdsList();
        for (String brokerId : eligibleReplicas) {
            BrokerInfo broker = brokers.get(brokerId);
            if (broker != null && broker.getAlive()) {
                leaderAssignments.put(topicKey(topicName, partitionId), brokerId);
                if (partition != null) {
                    PartitionMetadata updated = partition.toBuilder().setLeaderBrokerId(brokerId).build();
                    partitionsByTopic.get(topicName).put(partitionId, updated);
                    persistPartition(updated);
                }
                return ElectLeaderResponse.newBuilder().setLeaderBrokerId(brokerId).setSuccess(true).build();
            }
        }

        return ElectLeaderResponse.newBuilder().setLeaderBrokerId("").setSuccess(false).build();
    }

    public synchronized CommitOffsetResponse commitOffset(CommitOffsetRequest request) {
        if (request.getConsumerGroup().isBlank() || request.getTopic().isBlank()
                || request.getPartition() < 0 || request.getOffset() < 0) {
            return CommitOffsetResponse.newBuilder().setSuccess(false)
                    .setMessage("Group, topic, partition, and offset must be valid").build();
        }
        OffsetKey key = new OffsetKey(request.getConsumerGroup(), request.getTopic(), request.getPartition());
        Long previousOffset = committedOffsets.get(key);
        if (previousOffset != null && request.getOffset() < previousOffset) {
            return CommitOffsetResponse.newBuilder().setSuccess(false)
                    .setMessage("Committed offset cannot move backwards").build();
        }
        persistOffset(key, request.getOffset());
        committedOffsets.put(key, request.getOffset());
        return CommitOffsetResponse.newBuilder().setSuccess(true).setMessage("Offset committed").build();
    }

    public GetCommittedOffsetResponse getCommittedOffset(GetCommittedOffsetRequest request) {
        Long offset = committedOffsets.get(new OffsetKey(request.getConsumerGroup(), request.getTopic(), request.getPartition()));
        if (offset == null) {
            return GetCommittedOffsetResponse.newBuilder().setFound(false).build();
        }
        return GetCommittedOffsetResponse.newBuilder().setOffset(offset).setFound(true).build();
    }

    public synchronized JoinConsumerGroupResponse joinConsumerGroup(JoinConsumerGroupRequest request) {
        if (request.getGroupId().isBlank() || request.getConsumerId().isBlank() || request.getTopicNamesCount() == 0) {
            return JoinConsumerGroupResponse.newBuilder().setSuccess(false)
                    .setMessage("Group, consumer, and at least one topic are required").build();
        }
        List<String> requestedTopics = request.getTopicNamesList().stream().distinct().sorted().toList();
        for (String topicName : requestedTopics) {
            if (!topics.containsKey(topicName)) {
                return JoinConsumerGroupResponse.newBuilder().setSuccess(false)
                        .setMessage("Unknown topic: " + topicName).build();
            }
        }

        ConsumerGroupState group = consumerGroups.computeIfAbsent(request.getGroupId(), ignored -> new ConsumerGroupState());
        ConsumerMember existing = group.members.get(request.getConsumerId());
        ConsumerMember updated = new ConsumerMember(requestedTopics, currentTimeMs.getAsLong());
        group.members.put(request.getConsumerId(), updated);
        if (existing == null || !existing.topics().equals(requestedTopics)) {
            rebalance(request.getGroupId(), group);
        }
        return assignmentFor(request.getConsumerId(), group);
    }

    public synchronized HeartbeatConsumerGroupResponse heartbeatConsumerGroup(HeartbeatConsumerGroupRequest request) {
        ConsumerGroupState group = consumerGroups.get(request.getGroupId());
        if (group == null || !group.members.containsKey(request.getConsumerId())) {
            return HeartbeatConsumerGroupResponse.newBuilder().setAlive(false).build();
        }
        ConsumerMember member = group.members.get(request.getConsumerId());
        group.members.put(request.getConsumerId(), new ConsumerMember(member.topics(), currentTimeMs.getAsLong()));
        return HeartbeatConsumerGroupResponse.newBuilder()
                .setAlive(true)
                .setGenerationId(group.generationId)
                .addAllAssignedPartitions(group.assignments.getOrDefault(request.getConsumerId(), List.of()))
                .build();
    }

    public synchronized LeaveConsumerGroupResponse leaveConsumerGroup(LeaveConsumerGroupRequest request) {
        ConsumerGroupState group = consumerGroups.get(request.getGroupId());
        if (group == null || group.members.remove(request.getConsumerId()) == null) {
            return LeaveConsumerGroupResponse.newBuilder().setSuccess(false).setMessage("Consumer is not a group member").build();
        }
        if (group.members.isEmpty()) {
            consumerGroups.remove(request.getGroupId());
        } else {
            rebalance(request.getGroupId(), group);
        }
        return LeaveConsumerGroupResponse.newBuilder().setSuccess(true).setMessage("Consumer left group").build();
    }

        public synchronized UpdateReplicaProgressResponse updateReplicaProgress(UpdateReplicaProgressRequest request) {
        String key = topicKey(request.getTopic(), request.getPartition());
        PartitionMetadata partition = partitionsByTopic.getOrDefault(request.getTopic(), Collections.emptyMap())
                    .get(request.getPartition());
        BrokerInfo replica = brokers.get(request.getBrokerId());
        if (partition == null || !partition.getReplicaBrokerIdsList().contains(request.getBrokerId())
                    || replica == null || !replica.getAlive() || request.getLogEndOffset() < 0) {
            return UpdateReplicaProgressResponse.newBuilder().setSuccess(false)
                        .setMessage("Unknown partition, replica, or invalid log end offset").build();
        }
        Map<String, Long> progress = replicaProgress.computeIfAbsent(key, ignored -> new ConcurrentHashMap<>());
        Long previous = progress.get(request.getBrokerId());
        if (previous != null && request.getLogEndOffset() < previous) {
            return UpdateReplicaProgressResponse.newBuilder().setSuccess(false)
                        .setMessage("Replica progress cannot move backwards").build();
        }
        progress.put(request.getBrokerId(), request.getLogEndOffset());
        List<String> inSyncReplicas = new ArrayList<>(partition.getIsrBrokerIdsList());
        long leaderEndOffset = progress.getOrDefault(partition.getLeaderBrokerId(), partition.getHighWatermark());
        if (!inSyncReplicas.contains(request.getBrokerId())
            && request.getLogEndOffset() >= Math.max(partition.getHighWatermark(), leaderEndOffset)) {
            inSyncReplicas.add(request.getBrokerId());
        }
        long highWatermark = Math.max(partition.getHighWatermark(), inSyncReplicas.stream()
                .mapToLong(brokerId -> progress.getOrDefault(brokerId, 0L))
                .min().orElse(partition.getHighWatermark()));
        long logEndOffset = replicaAssignments.getOrDefault(key, List.of()).stream()
                .mapToLong(brokerId -> progress.getOrDefault(brokerId, 0L))
                .max().orElse(request.getLogEndOffset());
        String leader = partition.getLeaderBrokerId();
        BrokerInfo currentLeader = brokers.get(leader);
        if (leader.isBlank() || currentLeader == null || !currentLeader.getAlive() || !inSyncReplicas.contains(leader)) {
            leader = partition.getReplicaBrokerIdsList().stream()
                    .filter(inSyncReplicas::contains)
                    .filter(brokerId -> {
                        BrokerInfo candidate = brokers.get(brokerId);
                        return candidate != null && candidate.getAlive();
                    })
                    .findFirst().orElse("");
        }
        PartitionMetadata updated = partition.toBuilder()
                .setLeaderBrokerId(leader)
                .setHighWatermark(highWatermark)
                .setLogEndOffset(logEndOffset)
                .clearIsrBrokerIds()
                .addAllIsrBrokerIds(inSyncReplicas)
                .build();
        partitionsByTopic.get(request.getTopic()).put(request.getPartition(), updated);
        leaderAssignments.put(key, leader);
        persistPartition(updated);
        return UpdateReplicaProgressResponse.newBuilder().setSuccess(true)
                .setHighWatermark(highWatermark).setMessage("Replica progress updated").build();
    }

    private void expireConsumerMembers(long now) {
        for (Map.Entry<String, ConsumerGroupState> entry : consumerGroups.entrySet()) {
            ConsumerGroupState group = entry.getValue();
            boolean changed = group.members.entrySet().removeIf(member ->
                    now - member.getValue().lastHeartbeatMs() > heartbeatTimeoutMs);
            if (changed) {
                if (group.members.isEmpty()) {
                    consumerGroups.remove(entry.getKey());
                } else {
                    rebalance(entry.getKey(), group);
                }
            }
        }
    }

    private void rebalance(String groupId, ConsumerGroupState group) {
        Map<String, List<TopicPartition>> assignments = new HashMap<>();
        group.members.keySet().forEach(consumerId -> assignments.put(consumerId, new ArrayList<>()));
        List<String> topicNames = group.members.values().stream()
                .flatMap(member -> member.topics().stream()).distinct().sorted().toList();
        for (String topicName : topicNames) {
            List<String> members = group.members.entrySet().stream()
                    .filter(member -> member.getValue().topics().contains(topicName))
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
            int partitionCount = topics.get(topicName).getPartitionCount();
            for (int partition = 0; partition < partitionCount; partition++) {
                String consumerId = members.get(partition % members.size());
                assignments.get(consumerId).add(TopicPartition.newBuilder()
                        .setTopic(topicName).setPartition(partition).build());
            }
        }
        group.assignments = assignments;
        group.generationId++;
    }

    private JoinConsumerGroupResponse assignmentFor(String consumerId, ConsumerGroupState group) {
        return JoinConsumerGroupResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Consumer joined group")
                .setGenerationId(group.generationId)
                .addAllAssignedPartitions(group.assignments.getOrDefault(consumerId, List.of()))
                .build();
    }

    private String topicKey(String topicName, int partitionId) {
        return topicName + "#" + partitionId;
    }

    private void restoreMetadata() throws java.io.IOException {
        for (BrokerInfo broker : metadataStore.loadBrokers()) {
            brokers.put(broker.getBrokerId(), broker.toBuilder().setAlive(true).build());
        }
        for (TopicMetadata topic : metadataStore.loadTopics()) {
            topics.put(topic.getName(), topic);
            Map<Integer, PartitionMetadata> partitions = new ConcurrentHashMap<>();
            for (PartitionMetadata partition : metadataStore.loadPartitions(topic.getName())) {
                partitions.put(partition.getPartitionId(), partition);
                String key = topicKey(topic.getName(), partition.getPartitionId());
                leaderAssignments.put(key, partition.getLeaderBrokerId());
                replicaAssignments.put(key, partition.getReplicaBrokerIdsList());
                Map<String, Long> progress = new ConcurrentHashMap<>();
                partition.getReplicaBrokerIdsList().forEach(brokerId ->
                        progress.put(brokerId, partition.getHighWatermark()));
                replicaProgress.put(key, progress);
            }
            partitionsByTopic.put(topic.getName(), partitions);
        }
        for (ZooKeeperMetadataStore.CommittedOffset offset : metadataStore.loadOffsets()) {
            committedOffsets.put(new OffsetKey(offset.groupId(), offset.topic(), offset.partition()), offset.offset());
        }
    }

    private void persistBroker(BrokerInfo broker) {
        if (metadataStore != null) {
            persist(() -> metadataStore.registerBroker(broker));
        }
    }

    private void removePersistedBroker(String brokerId) {
        if (metadataStore != null) {
            persist(() -> metadataStore.removeBroker(brokerId));
        }
    }

    private void persistTopic(TopicMetadata topic) {
        if (metadataStore != null) {
            persist(() -> metadataStore.saveTopic(topic));
        }
    }

    private void persistPartition(PartitionMetadata partition) {
        if (metadataStore != null) {
            persist(() -> metadataStore.savePartition(partition));
        }
    }

    private void persistOffset(OffsetKey key, long offset) {
        if (metadataStore != null) {
            persist(() -> metadataStore.saveOffset(key.consumerGroup(), key.topic(), key.partition(), offset));
        }
    }

    private void persist(IoOperation operation) {
        try {
            operation.run();
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Coordinator metadata persistence failed", exception);
        }
    }

    private record OffsetKey(String consumerGroup, String topic, int partition) {
    }

    public record AdminOverview(long sampledAtMs, List<BrokerView> brokers, List<TopicView> topics,
                                List<PartitionView> partitions, List<ConsumerGroupView> consumerGroups,
                                List<OffsetView> committedOffsets) {
    }

    public record BrokerView(String brokerId, String host, int port, boolean alive, long lastHeartbeatMs) {
    }

    public record TopicView(String name, int partitionCount, int replicationFactor,
                            long retentionMs, long segmentSizeBytes) {
    }

    public record PartitionView(String topic, int partitionId, String leaderBrokerId,
                                List<String> replicas, List<String> isr, long highWatermark,
                                long logStartOffset, long logEndOffset) {
    }

    public record ConsumerGroupView(String groupId, int memberCount, long generationId, int assignedPartitionCount) {
    }

    public record OffsetView(String groupId, String topic, int partition, long offset) {
    }

    private record ConsumerMember(List<String> topics, long lastHeartbeatMs) {
    }

    @FunctionalInterface
    private interface IoOperation {
        void run() throws java.io.IOException;
    }

    private static final class ConsumerGroupState {
        private final Map<String, ConsumerMember> members = new HashMap<>();
        private Map<String, List<TopicPartition>> assignments = new HashMap<>();
        private long generationId;
    }
}
