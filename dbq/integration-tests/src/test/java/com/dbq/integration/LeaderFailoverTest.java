package com.dbq.integration;

import com.dbq.broker.BrokerConfig;
import com.dbq.broker.BrokerServer;
import com.dbq.coordinator.CoordinatorServer;
import com.dbq.producer.MessageProducer;
import com.dbq.producer.ProducerConfig;
import com.dbq.producer.ProducerRecord;
import com.dbq.proto.CreateTopicRequest;
import com.dbq.proto.GetPartitionMetadataRequest;
import com.dbq.proto.ProducerAckMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderFailoverTest {
    @TempDir
    Path dataDirectory;

    @Test
    void removesFailedLeaderFromIsrAndContinuesAckAllWrites() throws Exception {
        CoordinatorServer coordinator = new CoordinatorServer(0, 1_200);
        List<BrokerServer> brokers = new ArrayList<>();
        ManagedChannel adminChannel = null;
        coordinator.start();
        try {
            for (int index = 0; index < 3; index++) {
                String brokerId = "failover-broker-" + index;
                brokers.add(new BrokerServer(new BrokerConfig(
                        brokerId, "localhost", freePort(), dataDirectory.resolve(brokerId),
                        "localhost", coordinator.port(), 1024 * 1024, 1024 * 1024,
                        200, 1_000)));
                brokers.get(index).start();
            }

            adminChannel = ManagedChannelBuilder.forAddress("localhost", coordinator.port()).usePlaintext().build();
            var admin = com.dbq.proto.CoordinatorServiceGrpc.newBlockingStub(adminChannel);
            assertTrue(admin.createTopic(CreateTopicRequest.newBuilder()
                    .setTopicName("failover-events").setPartitionCount(1).setReplicationFactor(3).build())
                    .getSuccess());
            try (MessageProducer producer = producer(coordinator.port(), ProducerAckMode.AckLevel.ACK_ALL)) {
                producer.send("failover-events", ProducerRecord.of("one", "before".getBytes()));
            }

            var before = admin.getPartitionMetadata(GetPartitionMetadataRequest.newBuilder()
                    .setTopicName("failover-events").setPartitionId(0).build()).getMetadata();
            String failedLeader = before.getLeaderBrokerId();
            int failedIndex = Integer.parseInt(failedLeader.substring(failedLeader.lastIndexOf('-') + 1));
            brokers.get(failedIndex).close();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            var after = before;
            while (System.nanoTime() < deadline && after.getLeaderBrokerId().equals(failedLeader)) {
                Thread.sleep(100);
                after = admin.getPartitionMetadata(GetPartitionMetadataRequest.newBuilder()
                        .setTopicName("failover-events").setPartitionId(0).build()).getMetadata();
            }
            assertNotEquals(failedLeader, after.getLeaderBrokerId());
            assertEquals(2, after.getIsrBrokerIdsCount());

            try (MessageProducer producer = producer(coordinator.port(), ProducerAckMode.AckLevel.ACK_ALL)) {
                assertEquals(1, producer.send("failover-events",
                        ProducerRecord.of("two", "after".getBytes())).getLastOffset());
            }
            var committed = admin.getPartitionMetadata(GetPartitionMetadataRequest.newBuilder()
                    .setTopicName("failover-events").setPartitionId(0).build()).getMetadata();
            assertEquals(2, committed.getHighWatermark());
        } finally {
            if (adminChannel != null) {
                adminChannel.shutdownNow();
            }
            for (int index = brokers.size() - 1; index >= 0; index--) {
                brokers.get(index).close();
            }
            coordinator.stop();
        }
    }

    private MessageProducer producer(int coordinatorPort, ProducerAckMode.AckLevel ackMode) {
        return new MessageProducer(new ProducerConfig("localhost", coordinatorPort,
                100, 0, 0, 1_000, 2, ackMode));
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}