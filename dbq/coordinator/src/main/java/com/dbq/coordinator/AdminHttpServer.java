package com.dbq.coordinator;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.Empty;
import com.dbq.proto.FetchRequest;
import com.dbq.proto.HeartbeatConsumerGroupRequest;
import com.dbq.proto.JoinConsumerGroupRequest;
import com.dbq.proto.LeaveConsumerGroupRequest;
import com.dbq.proto.MessageRecord;
import com.dbq.proto.ProducerAckMode;
import com.dbq.proto.ProduceRequest;
import com.dbq.proto.TopicPartition;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Local-development HTTP console and JSON gateway for the coordinator. */
public final class AdminHttpServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AdminHttpServer.class);
    private static final int MAX_FETCH_MESSAGES = 200;

    private final CoordinatorService coordinator;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, BrokerConnection> brokerConnections = new ConcurrentHashMap<>();

    public AdminHttpServer(int port, CoordinatorService coordinator) throws IOException {
        this.coordinator = coordinator;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(executor);
        this.server.createContext("/", this::handle);
    }

    public void start() {
        server.start();
        logger.info("Admin console listening at http://localhost:{}", server.getAddress().getPort());
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
        brokerConnections.values().forEach(connection -> connection.channel().shutdownNow());
        brokerConnections.clear();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if (path.startsWith("/api/")) {
                handleApi(exchange, path);
            } else {
                serveAsset(exchange, path);
            }
        } catch (AdminException exception) {
            sendJson(exchange, exception.status(), new ErrorResponse(exception.getMessage()));
        } catch (IllegalArgumentException exception) {
            sendJson(exchange, 400, new ErrorResponse(exception.getMessage()));
        } catch (StatusRuntimeException exception) {
            logger.warn("Admin request failed in broker RPC: {}", exception.getStatus());
            sendJson(exchange, 502, new ErrorResponse("Broker request failed: " + exception.getStatus().getCode()));
        } catch (Exception exception) {
            logger.error("Admin HTTP request failed", exception);
            sendJson(exchange, 500, new ErrorResponse("Admin request failed"));
        } finally {
            exchange.close();
        }
    }

    private void handleApi(HttpExchange exchange, String path) throws IOException {
        String method = exchange.getRequestMethod();
        if (method.equals("GET") && path.equals("/api/overview")) {
            sendJson(exchange, 200, coordinator.adminOverview());
            return;
        }
        if (method.equals("POST") && path.equals("/api/topics")) {
            CreateTopicBody body = objectMapper.readValue(exchange.getRequestBody(), CreateTopicBody.class);
            var response = coordinator.createTopic(com.dbq.proto.CreateTopicRequest.newBuilder()
                    .setTopicName(body.topicName() == null ? "" : body.topicName())
                    .setPartitionCount(body.partitionCount())
                    .setReplicationFactor(body.replicationFactor())
                    .setRetentionMs(body.retentionMs())
                    .setSegmentSizeBytes(body.segmentSizeBytes())
                    .build());
            if (!response.getSuccess()) {
                throw new AdminException(422, response.getMessage());
            }
            sendJson(exchange, 201, new CreatedResponse(body.topicName(), response.getMessage()));
            return;
        }
        if (method.equals("POST") && path.equals("/api/messages")) {
            PublishMessageBody body = objectMapper.readValue(exchange.getRequestBody(), PublishMessageBody.class);
            sendJson(exchange, 200, publish(body));
            return;
        }
        if (method.equals("GET") && path.equals("/api/messages")) {
            sendJson(exchange, 200, fetchMessages(parseQuery(exchange.getRequestURI().getRawQuery())));
            return;
        }
        if (method.equals("POST") && path.equals("/api/groups/join")) {
            GroupJoinBody body = objectMapper.readValue(exchange.getRequestBody(), GroupJoinBody.class);
            sendJson(exchange, 200, joinGroup(body));
            return;
        }
        if (method.equals("POST") && path.equals("/api/groups/heartbeat")) {
            GroupMemberBody body = objectMapper.readValue(exchange.getRequestBody(), GroupMemberBody.class);
            sendJson(exchange, 200, heartbeatGroup(body));
            return;
        }
        if (method.equals("POST") && path.equals("/api/groups/poll")) {
            GroupMemberBody body = objectMapper.readValue(exchange.getRequestBody(), GroupMemberBody.class);
            sendJson(exchange, 200, pollGroup(body));
            return;
        }
        if (method.equals("POST") && path.equals("/api/groups/commit")) {
            GroupCommitBody body = objectMapper.readValue(exchange.getRequestBody(), GroupCommitBody.class);
            sendJson(exchange, 200, commitGroupOffset(body));
            return;
        }
        if (method.equals("POST") && path.equals("/api/groups/leave")) {
            GroupMemberBody body = objectMapper.readValue(exchange.getRequestBody(), GroupMemberBody.class);
            var response = coordinator.leaveConsumerGroup(LeaveConsumerGroupRequest.newBuilder()
                    .setGroupId(required(body.groupId(), "Group id"))
                    .setConsumerId(required(body.consumerId(), "Consumer id"))
                    .build());
            if (!response.getSuccess()) {
                throw new AdminException(409, response.getMessage());
            }
            sendJson(exchange, 200, new CreatedResponse(body.groupId(), response.getMessage()));
            return;
        }
        if (path.startsWith("/api/")) {
            sendJson(exchange, 404, new ErrorResponse("Unknown admin endpoint"));
        }
    }

    private PublishReceipt publish(PublishMessageBody body) {
        if (body.topic() == null || body.topic().isBlank() || body.payload() == null) {
            throw new AdminException(400, "Topic and payload are required");
        }
        var metadata = coordinator.getPartitionMetadata(body.topic(), body.partition()).getMetadata();
        if (metadata.getTopic().isBlank()) {
            throw new AdminException(404, "Topic partition was not found");
        }
        BrokerInfo leader = coordinator.getLiveBrokers(Empty.getDefaultInstance()).getBrokersList().stream()
                .filter(broker -> broker.getBrokerId().equals(metadata.getLeaderBrokerId()))
                .findFirst()
                .orElseThrow(() -> new AdminException(503, "Partition has no live leader"));
        ProducerAckMode.AckLevel ackMode = parseAckMode(body.ackMode());
        byte[] payloadBytes = body.payload().getBytes(StandardCharsets.UTF_8);
        MessageRecord message = MessageRecord.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setTopic(body.topic())
                .setPartition(body.partition())
                .setKey(body.key() == null ? "" : body.key())
                .setPayload(com.google.protobuf.ByteString.copyFrom(payloadBytes))
                .setTimestamp(System.currentTimeMillis())
                .setSize(payloadBytes.length)
                .putHeaders("source", "admin-console")
                .build();
        var response = connection(leader).stub().withDeadlineAfter(5, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(body.topic())
                        .setPartition(body.partition())
                        .addMessages(message)
                        .setProducerId("admin-console")
                        .setAckMode(ackMode)
                        .build());
        if (!response.getSuccess()) {
            throw new AdminException(503, response.getError());
        }
        return new PublishReceipt(body.topic(), body.partition(), response.getBaseOffset(),
                response.getLastOffset(), ackMode.name());
    }

    private MessagePage fetchMessages(Map<String, String> query) {
        String topic = query.getOrDefault("topic", "");
        if (topic.isBlank()) {
            throw new AdminException(400, "Topic is required");
        }
        int partition = parseInt(query.getOrDefault("partition", "0"), "partition");
        long offset = parseLong(query.getOrDefault("offset", "0"), "offset");
        int limit = Math.min(MAX_FETCH_MESSAGES, Math.max(1,
                parseInt(query.getOrDefault("limit", "50"), "limit")));
        if (partition < 0 || offset < 0) {
            throw new AdminException(400, "Partition and offset must be non-negative");
        }

        var metadata = coordinator.getPartitionMetadata(topic, partition).getMetadata();
        if (metadata.getTopic().isBlank()) {
            throw new AdminException(404, "Topic partition was not found");
        }
        BrokerInfo leader = coordinator.getLiveBrokers(Empty.getDefaultInstance()).getBrokersList().stream()
                .filter(broker -> broker.getBrokerId().equals(metadata.getLeaderBrokerId()))
                .findFirst()
                .orElseThrow(() -> new AdminException(503, "Partition has no live leader"));
        var response = connection(leader).stub().withDeadlineAfter(5, TimeUnit.SECONDS)
                .fetch(FetchRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setOffset(offset)
                        .setMaxMessages(limit)
                        .setLongPoll(false)
                        .build());
        List<MessageView> messages = response.getMessagesList().stream()
                .map(message -> new MessageView(message.getTopic(), message.getPartition(), message.getOffset(),
                        message.getMessageId(), message.getKey(), message.getPayload().toStringUtf8(),
                        Base64.getEncoder().encodeToString(message.getPayload().toByteArray()),
                        message.getTimestamp(), message.getSize(), message.getChecksum(), message.getHeadersMap()))
                .toList();
        return new MessagePage(topic, partition, offset, response.getNextOffset(),
                metadata.getHighWatermark(), response.getHasMore(), messages);
    }

    private GroupJoinResponse joinGroup(GroupJoinBody body) {
        String groupId = required(body.groupId(), "Group id");
        String consumerId = required(body.consumerId(), "Consumer id");
        String topic = required(body.topic(), "Topic");
        var response = coordinator.joinConsumerGroup(JoinConsumerGroupRequest.newBuilder()
                .setGroupId(groupId)
                .setConsumerId(consumerId)
                .addTopicNames(topic)
                .build());
        if (!response.getSuccess()) {
            throw new AdminException(422, response.getMessage());
        }
        return new GroupJoinResponse(groupId, consumerId, response.getGenerationId(),
                response.getAssignedPartitionsList().stream().map(this::assignmentView).toList());
    }

    private GroupHeartbeatResponse heartbeatGroup(GroupMemberBody body) {
        var response = coordinator.heartbeatConsumerGroup(HeartbeatConsumerGroupRequest.newBuilder()
                .setGroupId(required(body.groupId(), "Group id"))
                .setConsumerId(required(body.consumerId(), "Consumer id"))
                .build());
        if (!response.getAlive()) {
            throw new AdminException(409, "Consumer is not a member of this group");
        }
        return new GroupHeartbeatResponse(response.getGenerationId(),
                response.getAssignedPartitionsList().stream().map(this::assignmentView).toList());
    }

    private GroupPollResponse pollGroup(GroupMemberBody body) {
        String groupId = required(body.groupId(), "Group id");
        String consumerId = required(body.consumerId(), "Consumer id");
        var heartbeat = coordinator.heartbeatConsumerGroup(HeartbeatConsumerGroupRequest.newBuilder()
                .setGroupId(groupId).setConsumerId(consumerId).build());
        if (!heartbeat.getAlive()) {
            throw new AdminException(409, "Consumer is not a member of this group");
        }
        List<MessageView> messages = new java.util.ArrayList<>();
        List<PendingOffset> positions = new java.util.ArrayList<>();
        for (TopicPartition assigned : heartbeat.getAssignedPartitionsList()) {
            var committed = coordinator.getCommittedOffset(com.dbq.proto.GetCommittedOffsetRequest.newBuilder()
                    .setConsumerGroup(groupId).setTopic(assigned.getTopic())
                    .setPartition(assigned.getPartition()).build());
            long startOffset = committed.getFound() ? committed.getOffset() : 0;
                var metadata = coordinator.getPartitionMetadata(assigned.getTopic(), assigned.getPartition()).getMetadata();
            BrokerInfo leader = coordinator.getLiveBrokers(Empty.getDefaultInstance()).getBrokersList().stream()
                    .filter(broker -> broker.getBrokerId().equals(metadata.getLeaderBrokerId()))
                    .findFirst()
                    .orElseThrow(() -> new AdminException(503, "Partition has no live leader"));
            var response = connection(leader).stub().withDeadlineAfter(5, TimeUnit.SECONDS)
                    .fetch(FetchRequest.newBuilder()
                            .setTopic(assigned.getTopic())
                            .setPartition(assigned.getPartition())
                            .setOffset(startOffset)
                            .setMaxMessages(50)
                            .setConsumerGroup(groupId)
                            .build());
            response.getMessagesList().stream().map(this::messageView).forEach(messages::add);
            positions.add(new PendingOffset(assigned.getTopic(), assigned.getPartition(), response.getNextOffset()));
        }
        return new GroupPollResponse(groupId, consumerId,
                heartbeat.getAssignedPartitionsList().stream().map(this::assignmentView).toList(),
                messages, positions);
    }

    private GroupCommitResponse commitGroupOffset(GroupCommitBody body) {
        String groupId = required(body.groupId(), "Group id");
        String consumerId = required(body.consumerId(), "Consumer id");
        String topic = required(body.topic(), "Topic");
        if (body.partition() < 0 || body.offset() < 0) {
            throw new AdminException(400, "Partition and offset must be non-negative");
        }
        var heartbeat = coordinator.heartbeatConsumerGroup(HeartbeatConsumerGroupRequest.newBuilder()
                .setGroupId(groupId).setConsumerId(consumerId).build());
        boolean assigned = heartbeat.getAlive() && heartbeat.getAssignedPartitionsList().stream()
                .anyMatch(partition -> partition.getTopic().equals(topic)
                        && partition.getPartition() == body.partition());
        if (!assigned) {
            throw new AdminException(409, "Partition is not assigned to this consumer");
        }
        var response = coordinator.commitOffset(com.dbq.proto.CommitOffsetRequest.newBuilder()
                .setConsumerGroup(groupId).setTopic(topic).setPartition(body.partition())
                .setOffset(body.offset()).build());
        if (!response.getSuccess()) {
            throw new AdminException(409, response.getMessage());
        }
        return new GroupCommitResponse(groupId, topic, body.partition(), body.offset());
    }

    private AssignmentView assignmentView(TopicPartition partition) {
        return new AssignmentView(partition.getTopic(), partition.getPartition());
    }

    private MessageView messageView(MessageRecord message) {
        return new MessageView(message.getTopic(), message.getPartition(), message.getOffset(),
                message.getMessageId(), message.getKey(), message.getPayload().toStringUtf8(),
                Base64.getEncoder().encodeToString(message.getPayload().toByteArray()),
                message.getTimestamp(), message.getSize(), message.getChecksum(), message.getHeadersMap());
    }

    private static String required(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new AdminException(400, label + " is required");
        }
        return value;
    }

    private BrokerConnection connection(BrokerInfo broker) {
        return brokerConnections.compute(broker.getBrokerId(), (brokerId, current) -> {
            if (current != null && current.host().equals(broker.getHost()) && current.port() == broker.getPort()) {
                return current;
            }
            if (current != null) {
                current.channel().shutdownNow();
            }
            ManagedChannel channel = ManagedChannelBuilder.forAddress(broker.getHost(), broker.getPort())
                    .usePlaintext().build();
            return new BrokerConnection(broker.getHost(), broker.getPort(), channel,
                    BrokerServiceGrpc.newBlockingStub(channel));
        });
    }

    private void serveAsset(HttpExchange exchange, String path) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            sendJson(exchange, 405, new ErrorResponse("Method not allowed"));
            return;
        }
        String asset = path.equals("/") ? "index.html" : path.substring(1);
        if (!asset.matches("[A-Za-z0-9._-]+")) {
            sendJson(exchange, 404, new ErrorResponse("Asset not found"));
            return;
        }
        try (var input = AdminHttpServer.class.getClassLoader().getResourceAsStream("static/" + asset)) {
            if (input == null) {
                sendJson(exchange, 404, new ErrorResponse("Asset not found"));
                return;
            }
            byte[] bytes = input.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType(asset));
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> values = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return values;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            values.put(key, value);
        }
        return values;
    }

    private static int parseInt(String value, String name) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new AdminException(400, name + " must be an integer");
        }
    }

    private static long parseLong(String value, String name) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new AdminException(400, name + " must be an integer");
        }
    }

    private static ProducerAckMode.AckLevel parseAckMode(String mode) {
        if (mode == null || mode.isBlank()) {
            return ProducerAckMode.AckLevel.ACK_1;
        }
        try {
            return ProducerAckMode.AckLevel.valueOf(mode);
        } catch (IllegalArgumentException exception) {
            throw new AdminException(400, "Unknown acknowledgement mode");
        }
    }

    private static String contentType(String asset) {
        if (asset.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (asset.endsWith(".js")) {
            return "text/javascript; charset=utf-8";
        }
        return "text/html; charset=utf-8";
    }

    private record BrokerConnection(String host, int port, ManagedChannel channel,
                                    BrokerServiceGrpc.BrokerServiceBlockingStub stub) {
    }

    public record CreateTopicBody(String topicName, int partitionCount, int replicationFactor,
                                  long retentionMs, long segmentSizeBytes) {
    }

    public record PublishMessageBody(String topic, int partition, String key, String payload, String ackMode) {
    }

    public record CreatedResponse(String topicName, String message) {
    }

    public record PublishReceipt(String topic, int partition, long baseOffset, long lastOffset, String ackMode) {
    }

    public record MessagePage(String topic, int partition, long requestedOffset, long nextOffset,
                              long highWatermark, boolean hasMore, List<MessageView> messages) {
    }

    public record MessageView(String topic, int partition, long offset, String messageId, String key,
                              String payload, String payloadBase64, long timestamp, long size,
                              String checksum, Map<String, String> headers) {
    }

    public record ErrorResponse(String error) {
    }

    public record GroupJoinBody(String groupId, String consumerId, String topic) {
    }

    public record GroupMemberBody(String groupId, String consumerId) {
    }

    public record GroupCommitBody(String groupId, String consumerId, String topic, int partition, long offset) {
    }

    public record AssignmentView(String topic, int partition) {
    }

    public record GroupJoinResponse(String groupId, String consumerId, long generationId,
                                    List<AssignmentView> assignedPartitions) {
    }

    public record GroupHeartbeatResponse(long generationId, List<AssignmentView> assignedPartitions) {
    }

    public record PendingOffset(String topic, int partition, long offset) {
    }

    public record GroupPollResponse(String groupId, String consumerId, List<AssignmentView> assignedPartitions,
                                    List<MessageView> messages, List<PendingOffset> nextOffsets) {
    }

    public record GroupCommitResponse(String groupId, String topic, int partition, long committedOffset) {
    }

    private static final class AdminException extends RuntimeException {
        private final int status;

        private AdminException(int status, String message) {
            super(message);
            this.status = status;
        }

        private int status() {
            return status;
        }
    }
}
