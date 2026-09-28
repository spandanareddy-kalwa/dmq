package com.dbq.integration;

import com.dbq.broker.BrokerConfig;
import com.dbq.broker.BrokerServer;
import com.dbq.coordinator.CoordinatorServer;
import com.dbq.proto.BrokerServiceGrpc;
import com.dbq.proto.CreateTopicRequest;
import com.dbq.proto.GetPartitionStateRequest;
import com.dbq.proto.MessageRecord;
import com.dbq.proto.ProduceRequest;
import com.dbq.proto.ProducerAckMode;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionFlowTest {
    @TempDir
    Path dataDirectory;

    @Test
    void brokerDeletesExpiredSealedSegmentAndReportsNewStartOffset() throws Exception {
        CoordinatorServer coordinator = new CoordinatorServer(0, 10_000);
        BrokerServer broker = null;
        ManagedChannel adminChannel = null;
        try {
            coordinator.start();
            broker = new BrokerServer(new BrokerConfig(
                    "retention-broker", "localhost", freePort(), dataDirectory,
                    "localhost", coordinator.port(), 1024 * 1024, 1024 * 1024, 500, 2_000));
            broker.start();
            adminChannel = ManagedChannelBuilder.forAddress("localhost", coordinator.port())
                    .usePlaintext().build();
            var admin = com.dbq.proto.CoordinatorServiceGrpc.newBlockingStub(adminChannel);
            assertTrue(admin.createTopic(CreateTopicRequest.newBuilder()
                    .setTopicName("retained-events")
                    .setPartitionCount(1)
                    .setReplicationFactor(1)
                    .setRetentionMs(1)
                    .build()).getSuccess());
                        ManagedChannel brokerChannel = ManagedChannelBuilder.forAddress("localhost", broker.port()).usePlaintext().build();
                        try {
                var brokerStub = BrokerServiceGrpc.newBlockingStub(brokerChannel);
                long now = System.currentTimeMillis();
                assertTrue(brokerStub.produce(produceRequest("old", "expired", now - 60_000)).getSuccess());
                assertTrue(brokerStub.produce(produceRequest("new", "retained", now + 60_000)).getSuccess());
                var state = brokerStub.getPartitionState(GetPartitionStateRequest.newBuilder()
                        .setTopic("retained-events").setPartition(0).build());
                                assertEquals(1, state.getMetadata().getLogStartOffset());
                                assertEquals(2, state.getMetadata().getLogEndOffset());
                        } finally {
                                brokerChannel.shutdownNow();
                        }
        } finally {
                        if (adminChannel != null) {
                                adminChannel.shutdownNow();
                        }
                        if (broker != null) {
                                broker.close();
                        }
            coordinator.stop();
        }
    }

    private int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

        private ProduceRequest produceRequest(String id, String payload, long timestamp) {
                return ProduceRequest.newBuilder()
                                .setTopic("retained-events")
                                .setPartition(0)
                                .setAckMode(ProducerAckMode.AckLevel.ACK_1)
                                .addMessages(MessageRecord.newBuilder()
                                                .setMessageId(id)
                                                .setKey(id)
                                                .setPayload(ByteString.copyFromUtf8(payload))
                                                .setTimestamp(timestamp)
                                                .build())
                                .build();
        }
}
