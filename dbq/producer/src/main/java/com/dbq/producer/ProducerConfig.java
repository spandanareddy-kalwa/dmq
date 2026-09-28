package com.dbq.producer;

public record ProducerConfig(String coordinatorHost, int coordinatorPort,
                             int maxBatchMessages, int maxRetries, long retryBackoffMs,
                             long requestTimeoutMs, int maxInFlightBatches,
                             com.dbq.proto.ProducerAckMode.AckLevel ackMode) {
    public ProducerConfig {
        if (coordinatorHost == null || coordinatorHost.isBlank() || coordinatorPort < 1 || coordinatorPort > 65535
                || maxBatchMessages <= 0 || maxRetries < 0 || retryBackoffMs < 0
                || requestTimeoutMs <= 0 || maxInFlightBatches <= 0 || ackMode == null) {
            throw new IllegalArgumentException("Invalid producer configuration");
        }
    }

    public static ProducerConfig fromEnvironment() {
        return new ProducerConfig(
                System.getenv().getOrDefault("DBQ_COORDINATOR_HOST", "localhost"),
                integer("DBQ_COORDINATOR_PORT", 9090),
                integer("DBQ_BATCH_MAX_MESSAGES", 500),
                integer("DBQ_PRODUCER_MAX_RETRIES", 3),
                longValue("DBQ_PRODUCER_RETRY_BACKOFF_MS", 100),
                longValue("DBQ_RPC_TIMEOUT_MS", 3_000),
                integer("DBQ_PRODUCER_MAX_IN_FLIGHT", 8),
                ackMode(System.getenv().getOrDefault("DBQ_PRODUCER_ACK_MODE", "ACK_1")));
    }

    private static int integer(String name, int defaultValue) {
        return Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(defaultValue)));
    }

    private static long longValue(String name, long defaultValue) {
        return Long.parseLong(System.getenv().getOrDefault(name, Long.toString(defaultValue)));
    }

    private static com.dbq.proto.ProducerAckMode.AckLevel ackMode(String value) {
        return switch (value.toUpperCase()) {
            case "ACK_0" -> com.dbq.proto.ProducerAckMode.AckLevel.ACK_0;
            case "ACK_1" -> com.dbq.proto.ProducerAckMode.AckLevel.ACK_1;
            case "ACK_ALL" -> com.dbq.proto.ProducerAckMode.AckLevel.ACK_ALL;
            default -> throw new IllegalArgumentException("Unknown producer acknowledgement mode: " + value);
        };
    }
}