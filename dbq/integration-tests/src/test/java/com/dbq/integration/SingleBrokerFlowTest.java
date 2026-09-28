package com.dbq.integration;

import com.dbq.broker.BrokerConfig;
import com.dbq.broker.BrokerServer;
import com.dbq.consumer.ConsumerConfig;
import com.dbq.consumer.MessageConsumer;
import com.dbq.coordinator.CoordinatorServer;
import com.dbq.producer.MessageProducer;
import com.dbq.producer.ProducerConfig;
import com.dbq.producer.ProducerRecord;
import com.dbq.proto.CoordinatorServiceGrpc;
import com.dbq.proto.CreateTopicRequest;
import com.dbq.proto.ProducerAckMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleBrokerFlowTest {
    @TempDir
    Path dataDirectory;

    @Test
    void producesConsumesAndResumesFromCommittedOffset() throws Exception {
        int brokerPort = freePort();
        CoordinatorServer coordinatorServer = new CoordinatorServer(0, 10_000);
        coordinatorServer.start();
        BrokerServer brokerServer = new BrokerServer(new BrokerConfig(
                "broker-1", "localhost", brokerPort, dataDirectory,
                "localhost", coordinatorServer.port(), 1024 * 1024, 1024 * 1024,
                500, 2_000));
        ManagedChannel adminChannel = ManagedChannelBuilder.forAddress("localhost", coordinatorServer.port())
                .usePlaintext().build();
        try {
            brokerServer.start();
            var admin = CoordinatorServiceGrpc.newBlockingStub(adminChannel);
            var topic = admin.createTopic(CreateTopicRequest.newBuilder()
                    .setTopicName("orders")
                    .setPartitionCount(1)
                    .setReplicationFactor(1)
                    .setSegmentSizeBytes(1024 * 1024)
                    .build());
            assertTrue(topic.getSuccess());

            try (MessageProducer producer = new MessageProducer(new ProducerConfig(
                    "localhost", coordinatorServer.port(), 20, 1, 10, 2_000, 2,
                    ProducerAckMode.AckLevel.ACK_ALL))) {
                var produce = producer.send("orders", ProducerRecord.of("order-1", "created".getBytes()));
                assertEquals(0, produce.getLastOffset());
            }

            try (MessageConsumer consumer = new MessageConsumer(new ConsumerConfig(
                    "localhost", coordinatorServer.port(), "order-workers", "consumer-a",
                    10, 10, 2_000, 500))) {
                assertEquals(1, consumer.subscribe(List.of("orders")).size());
                var messages = consumer.poll();
                assertEquals(1, messages.size());
                assertEquals("created", messages.get(0).getPayload().toStringUtf8());
                assertEquals(1, consumer.position("orders", 0));
                assertTrue(consumer.commitSync());
            }

            try (MessageConsumer resumed = new MessageConsumer(new ConsumerConfig(
                    "localhost", coordinatorServer.port(), "order-workers", "consumer-b",
                    10, 10, 2_000, 500))) {
                resumed.subscribe(List.of("orders"));
                assertEquals(1, resumed.position("orders", 0));
                assertTrue(resumed.poll().isEmpty());
            }
        } finally {
            adminChannel.shutdownNow();
            brokerServer.close();
            coordinatorServer.stop();
        }
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}