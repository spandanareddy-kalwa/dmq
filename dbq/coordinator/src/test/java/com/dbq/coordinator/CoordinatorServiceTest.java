package com.dbq.coordinator;

import com.dbq.proto.BrokerInfo;
import com.dbq.proto.CommitOffsetRequest;
import com.dbq.proto.CreateTopicRequest;
import com.dbq.proto.Empty;
import com.dbq.proto.GetCommittedOffsetRequest;
import com.dbq.proto.GetLiveBrokersResponse;
import com.dbq.proto.RegisterBrokerRequest;
import com.dbq.proto.HeartbeatRequest;
import com.dbq.proto.JoinConsumerGroupRequest;
import com.dbq.proto.LeaveConsumerGroupRequest;
import com.dbq.proto.UpdateReplicaProgressRequest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorServiceTest {

    @Test
    void registerBrokerAndTrackHeartbeat() {
        CoordinatorService service = new CoordinatorService();

        BrokerInfo broker = BrokerInfo.newBuilder()
                .setBrokerId("broker-1")
                .setHost("localhost")
                .setPort(9091)
                .setAlive(true)
                .build();

        RegisterBrokerRequest request = RegisterBrokerRequest.newBuilder().setBroker(broker).build();
        var response = service.registerBroker(request);

        assertTrue(response.getSuccess());

        HeartbeatRequest heartbeat = HeartbeatRequest.newBuilder()
                .setBrokerId("broker-1")
                .setTimestampMs(System.currentTimeMillis())
                .build();

        var heartbeatResponse = service.heartbeat(heartbeat);
        assertTrue(heartbeatResponse.getAlive());

        GetLiveBrokersResponse live = service.getLiveBrokers(Empty.getDefaultInstance());
        assertEquals(1, live.getBrokersCount());
        assertEquals("broker-1", live.getBrokers(0).getBrokerId());
    }

    @Test
    void createTopicCreatesPartitionMetadata() {
        CoordinatorService service = new CoordinatorService();

        service.registerBroker(BrokerInfo.newBuilder().setBrokerId("broker-1").setHost("localhost").setPort(9091).setAlive(true).build());
        service.registerBroker(BrokerInfo.newBuilder().setBrokerId("broker-2").setHost("localhost").setPort(9092).setAlive(true).build());
        service.registerBroker(BrokerInfo.newBuilder().setBrokerId("broker-3").setHost("localhost").setPort(9093).setAlive(true).build());

        var createResponse = service.createTopic(CreateTopicRequest.newBuilder()
                .setTopicName("orders")
                .setPartitionCount(3)
                .setReplicationFactor(3)
                .setRetentionMs(86400000)
                .setSegmentSizeBytes(1048576)
                .build());

        assertTrue(createResponse.getSuccess());
        assertEquals(3, service.getTopicMetadata("orders").getMetadata().getPartitionCount());
        assertEquals("broker-1", service.getLeader("orders", 0).getLeaderBrokerId());
    }

    @Test
    void expiresSilentLeaderAndElectsLiveInSyncReplica() {
        AtomicLong now = new AtomicLong(1_000);
        CoordinatorService service = new CoordinatorService(1_000, now::get);
        service.registerBroker(broker("broker-1", 9091));
        service.registerBroker(broker("broker-2", 9092));
        service.registerBroker(broker("broker-3", 9093));
        service.createTopic(topic("orders", 1, 3));

        now.set(2_000);
        service.heartbeat(heartbeat("broker-2"));
        service.heartbeat(heartbeat("broker-3"));
        now.set(3_000);

        assertEquals(1, service.expireBrokers());
        assertEquals("broker-2", service.getLeader("orders", 0).getLeaderBrokerId());
        var metadata = service.getPartitionMetadata("orders", 0).getMetadata();
        assertEquals(java.util.List.of("broker-2", "broker-3"), metadata.getIsrBrokerIdsList());
        assertEquals(2, service.getLiveBrokers(Empty.getDefaultInstance()).getBrokersCount());
    }

    @Test
    void rejectsTopicWhenReplicationFactorExceedsLiveBrokers() {
        CoordinatorService service = new CoordinatorService();
        service.registerBroker(broker("broker-1", 9091));

        var response = service.createTopic(topic("orders", 1, 2));

        assertFalse(response.getSuccess());
        assertEquals(0, service.getTopicMetadata("orders").getMetadata().getPartitionCount());
    }

    @Test
    void commitsOffsetsIndependentlyPerGroupAndPartitionWithoutMovingBackward() {
        CoordinatorService service = new CoordinatorService();
        CommitOffsetRequest commit = CommitOffsetRequest.newBuilder()
                .setConsumerGroup("billing")
                .setTopic("orders")
                .setPartition(0)
                .setOffset(12)
                .build();

        assertTrue(service.commitOffset(commit).getSuccess());
        assertTrue(service.commitOffset(commit.toBuilder().setConsumerGroup("shipping").setOffset(3).build()).getSuccess());
        var stored = service.getCommittedOffset(GetCommittedOffsetRequest.newBuilder()
                .setConsumerGroup("billing").setTopic("orders").setPartition(0).build());
        assertTrue(stored.getFound());
        assertEquals(12, stored.getOffset());
        assertFalse(service.commitOffset(commit.toBuilder().setOffset(11).build()).getSuccess());
    }

        @Test
        void rebalancesPartitionsWithinEachGroupWhenMembersChange() {
        CoordinatorService service = new CoordinatorService();
        service.registerBroker(broker("broker-1", 9091));
        assertTrue(service.createTopic(topic("orders", 2, 1)).getSuccess());

        var firstJoin = service.joinConsumerGroup(join("fulfillment", "consumer-1", "orders"));
        assertEquals(2, firstJoin.getAssignedPartitionsCount());

        var secondJoin = service.joinConsumerGroup(join("fulfillment", "consumer-2", "orders"));
        assertEquals(1, secondJoin.getAssignedPartitionsCount());
        assertEquals(1, secondJoin.getAssignedPartitions(0).getPartition());
        assertEquals(2, service.joinConsumerGroup(join("audit", "consumer-3", "orders"))
            .getAssignedPartitionsCount());

        assertTrue(service.leaveConsumerGroup(LeaveConsumerGroupRequest.newBuilder()
            .setGroupId("fulfillment").setConsumerId("consumer-2").build()).getSuccess());
        var reassigned = service.heartbeatConsumerGroup(com.dbq.proto.HeartbeatConsumerGroupRequest.newBuilder()
            .setGroupId("fulfillment").setConsumerId("consumer-1").build());
        assertEquals(2, reassigned.getAssignedPartitionsCount());
        }

    @Test
    void highWatermarkTracksMinimumInSyncReplicaEndOffset() {
        CoordinatorService service = new CoordinatorService();
        service.registerBroker(broker("broker-1", 9091));
        service.registerBroker(broker("broker-2", 9092));
        assertTrue(service.createTopic(topic("orders", 1, 2)).getSuccess());

        assertEquals(0, service.updateReplicaProgress(progress("broker-1", 8)).getHighWatermark());
        assertEquals(5, service.updateReplicaProgress(progress("broker-2", 5)).getHighWatermark());
        assertEquals(8, service.updateReplicaProgress(progress("broker-2", 8)).getHighWatermark());
        assertEquals(8, service.getPartitionMetadata("orders", 0).getMetadata().getHighWatermark());
    }

    private BrokerInfo broker(String brokerId, int port) {
        return BrokerInfo.newBuilder().setBrokerId(brokerId).setHost("localhost").setPort(port).build();
    }

    private CreateTopicRequest topic(String name, int partitions, int replicationFactor) {
        return CreateTopicRequest.newBuilder()
                .setTopicName(name)
                .setPartitionCount(partitions)
                .setReplicationFactor(replicationFactor)
                .build();
    }

    private HeartbeatRequest heartbeat(String brokerId) {
        return HeartbeatRequest.newBuilder().setBrokerId(brokerId).build();
    }

    private JoinConsumerGroupRequest join(String groupId, String consumerId, String topic) {
        return JoinConsumerGroupRequest.newBuilder().setGroupId(groupId)
                .setConsumerId(consumerId).addTopicNames(topic).build();
    }

    private UpdateReplicaProgressRequest progress(String brokerId, long endOffset) {
        return UpdateReplicaProgressRequest.newBuilder().setBrokerId(brokerId)
                .setTopic("orders").setPartition(0).setLogEndOffset(endOffset).build();
    }
}
