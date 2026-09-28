package com.dbq.broker;

import java.nio.file.Path;

public record BrokerConfig(String brokerId, String host, int port, Path dataDirectory,
                           String coordinatorHost, int coordinatorPort, long segmentBytes,
                           int maxRecordBytes, long heartbeatIntervalMs, long rpcTimeoutMs) {
    public BrokerConfig {
        if (brokerId == null || brokerId.isBlank() || host == null || host.isBlank()
                || coordinatorHost == null || coordinatorHost.isBlank()) {
            throw new IllegalArgumentException("Broker id and advertised/coordinator hosts are required");
        }
        if (port < 1 || port > 65535 || coordinatorPort < 1 || coordinatorPort > 65535
                || segmentBytes <= 0 || maxRecordBytes <= 0 || heartbeatIntervalMs <= 0 || rpcTimeoutMs <= 0) {
            throw new IllegalArgumentException("Broker ports, sizes, and intervals must be positive");
        }
    }

    public static BrokerConfig fromEnvironment() {
        String brokerId = required("DBQ_BROKER_ID");
        return new BrokerConfig(
                brokerId,
                value("DBQ_BROKER_HOST", "localhost"),
                integer("DBQ_BROKER_PORT", 9091),
                Path.of(value("DBQ_DATA_DIR", "data/" + brokerId)),
                value("DBQ_COORDINATOR_HOST", "localhost"),
                integer("DBQ_COORDINATOR_PORT", 9090),
                longValue("DBQ_SEGMENT_BYTES", 64L * 1024 * 1024),
                integer("DBQ_MAX_RECORD_BYTES", 16 * 1024 * 1024),
                longValue("DBQ_HEARTBEAT_INTERVAL_MS", 5_000),
                longValue("DBQ_RPC_TIMEOUT_MS", 3_000));
    }

    private static String required(String name) {
        String configured = System.getenv(name);
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException(name + " must be configured");
        }
        return configured;
    }

    private static String value(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }

    private static int integer(String name, int defaultValue) {
        return Integer.parseInt(value(name, Integer.toString(defaultValue)));
    }

    private static long longValue(String name, long defaultValue) {
        return Long.parseLong(value(name, Long.toString(defaultValue)));
    }
}