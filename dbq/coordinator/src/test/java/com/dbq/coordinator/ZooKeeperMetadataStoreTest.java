package com.dbq.coordinator;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.PartitionMetadata;
import com.dbq.proto.TopicMetadata;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZooKeeperMetadataStoreTest {
    @TempDir
    Path dataDirectory;

    private ZooKeeperServer server;
    private ServerCnxnFactory connectionFactory;
    private String connectString;

    @BeforeEach
    void startZooKeeper() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path snapshotDirectory = dataDirectory.resolve("snapshots");
        Path logDirectory = dataDirectory.resolve("transactions");
        java.nio.file.Files.createDirectories(snapshotDirectory);
        java.nio.file.Files.createDirectories(logDirectory);
        server = new ZooKeeperServer(snapshotDirectory.toFile(), logDirectory.toFile(), 2_000);
        connectionFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", port), 50);
        connectionFactory.startup(server);
        connectString = "127.0.0.1:" + port;
    }

    @AfterEach
    void stopZooKeeper() {
        if (connectionFactory != null) {
            connectionFactory.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    void persistsMetadataAndOffsetsButKeepsBrokerRegistrationEphemeral() throws Exception {
        try (ZooKeeperMetadataStore store = new ZooKeeperMetadataStore(connectString, 5_000)) {
            store.registerBroker(BrokerInfo.newBuilder()
                    .setBrokerId("broker-1").setHost("localhost").setPort(9091).setAlive(true).build());
            store.saveTopic(TopicMetadata.newBuilder()
                    .setName("orders").setPartitionCount(1).setReplicationFactor(1).build());
            store.savePartition(PartitionMetadata.newBuilder()
                    .setTopic("orders").setPartitionId(0).setLeaderBrokerId("broker-1")
                    .addReplicaBrokerIds("broker-1").addIsrBrokerIds("broker-1").setHighWatermark(7).build());
            store.saveOffset("workers", "orders", 0, 5);

            assertEquals(1, store.loadBrokers().size());
            assertEquals(1, store.loadTopics().size());
            assertEquals(7, store.loadPartitions("orders").get(0).getHighWatermark());
            assertEquals(5, store.loadOffsets().get(0).offset());
        }

        try (ZooKeeperMetadataStore restarted = new ZooKeeperMetadataStore(connectString, 5_000)) {
            assertTrue(restarted.loadBrokers().isEmpty());
            assertEquals("orders", restarted.loadTopics().get(0).getName());
            assertEquals(5, restarted.loadOffsets().get(0).offset());
            CoordinatorService restoredService = new CoordinatorService(
                15_000, System::currentTimeMillis, restarted);
            assertEquals(1, restoredService.getTopicMetadata("orders").getMetadata().getPartitionCount());
            assertEquals("broker-1", restoredService.getPartitionMetadata("orders", 0)
                .getMetadata().getLeaderBrokerId());
            assertEquals(5, restoredService.getCommittedOffset(com.dbq.proto.GetCommittedOffsetRequest.newBuilder()
                .setConsumerGroup("workers").setTopic("orders").setPartition(0).build()).getOffset());
        }
    }
}
