package com.dbq.integration;

import com.dbq.broker.BrokerConfig;
import com.dbq.broker.BrokerServer;
import com.dbq.consumer.ConsumerConfig;
import com.dbq.consumer.MessageConsumer;
import com.dbq.coordinator.CoordinatorServer;
import com.dbq.producer.MessageProducer;
import com.dbq.producer.ProducerConfig;
import com.dbq.producer.ProducerRecord;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.CreateTopicRequest;
import com.dbq.proto.GetPartitionStateRequest;
import com.dbq.proto.ProducerAckMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreeBrokerReplicationTest {
    @TempDir
    Path dataDirectory;

    @Test
    void ackAllWaitsForAndPersistsOnEveryReplica() throws Exception {
        CoordinatorServer coordinator = new CoordinatorServer(0, 10_000);
        List<BrokerServer> brokers = new ArrayList<>();
        List<ManagedChannel> channels = new ArrayList<>();
        coordinator.start();
        try {
            for (int index = 0; index < 3; index++) {
                String brokerId = "broker-" + index;
                int port = freePort();
                BrokerServer broker = new BrokerServer(new BrokerConfig(
                        brokerId, "localhost", port, dataDirectory.resolve(brokerId),
                        "localhost", coordinator.port(), 1024 * 1024, 1024 * 1024,
                        500, 2_000));
                broker.start();
                brokers.add(broker);
            }

            ManagedChannel adminChannel = ManagedChannelBuilder.forAddress("localhost", coordinator.port()).usePlaintext().build();
            channels.add(adminChannel);
            var admin = com.dbq.proto.CoordinatorServiceGrpc.newBlockingStub(adminChannel);
            assertTrue(admin.createTopic(CreateTopicRequest.newBuilder()
                    .setTopicName("replicated-orders")
                    .setPartitionCount(1)
                    .setReplicationFactor(3)
                    .build()).getSuccess());

                try (MessageProducer producer = new MessageProducer(new ProducerConfig(
                    "localhost", coordinator.port(), 100, 0, 0, 2_000, 2,
                    ProducerAckMode.AckLevel.ACK_1))) {
                assertEquals(0, producer.send("replicated-orders",
                    ProducerRecord.of("key-1", "first".getBytes())).getLastOffset());
                }
                try (MessageProducer producer = new MessageProducer(new ProducerConfig(
                    "localhost", coordinator.port(), 100, 0, 0, 2_000, 2,
                    ProducerAckMode.AckLevel.ACK_ALL))) {
                assertEquals(1, producer.send("replicated-orders",
                    ProducerRecord.of("key-2", "second".getBytes())).getLastOffset());
            }

            for (BrokerServer broker : brokers) {
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", broker.port()).usePlaintext().build();
                channels.add(channel);
                var state = BrokerServiceGrpc.newBlockingStub(channel).getPartitionState(
                        GetPartitionStateRequest.newBuilder().setTopic("replicated-orders").setPartition(0).build());
                assertEquals(2, state.getMetadata().getLogEndOffset());
                assertEquals(2, state.getHighWatermark());
            }

            try (MessageConsumer consumer = new MessageConsumer(new ConsumerConfig(
                    "localhost", coordinator.port(), "replication-check", "consumer-1",
                    10, 10, 2_000, 500))) {
                consumer.subscribe(List.of("replicated-orders"));
                var records = consumer.poll();
                assertEquals(2, records.size());
                assertEquals("first", records.get(0).getPayload().toStringUtf8());
                assertEquals("second", records.get(1).getPayload().toStringUtf8());
            }
        } finally {
            channels.forEach(ManagedChannel::shutdownNow);
            for (int index = brokers.size() - 1; index >= 0; index--) {
                brokers.get(index).close();
            }
            coordinator.stop();
        }
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}