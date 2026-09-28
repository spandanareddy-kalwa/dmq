package com.dbq.consumer;

public record ConsumerConfig(String coordinatorHost, int coordinatorPort, String groupId,
                             String consumerId, int maxMessagesPerPoll, long longPollTimeoutMs,
                             long requestTimeoutMs, long heartbeatIntervalMs) {
    public ConsumerConfig {
        if (coordinatorHost == null || coordinatorHost.isBlank() || coordinatorPort < 1 || coordinatorPort > 65535
                || groupId == null || groupId.isBlank() || consumerId == null || consumerId.isBlank()
                || maxMessagesPerPoll <= 0 || longPollTimeoutMs < 0 || requestTimeoutMs <= 0
                || heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("Invalid consumer configuration");
        }
    }

    public static ConsumerConfig fromEnvironment() {
        return new ConsumerConfig(
                System.getenv().getOrDefault("DBQ_COORDINATOR_HOST", "localhost"),
                integer("DBQ_COORDINATOR_PORT", 9090),
                required("DBQ_CONSUMER_GROUP"),
                System.getenv().getOrDefault("DBQ_CONSUMER_ID", java.util.UUID.randomUUID().toString()),
                integer("DBQ_CONSUMER_MAX_MESSAGES", 100),
                longValue("DBQ_CONSUMER_LONG_POLL_MS", 1_000),
                longValue("DBQ_RPC_TIMEOUT_MS", 3_000),
                longValue("DBQ_CONSUMER_HEARTBEAT_INTERVAL_MS", 5_000));
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be configured");
        }
        return value;
    }

    private static int integer(String name, int defaultValue) {
        return Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(defaultValue)));
    }

    private static long longValue(String name, long defaultValue) {
        return Long.parseLong(System.getenv().getOrDefault(name, Long.toString(defaultValue)));
    }
}