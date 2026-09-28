package com.dbq.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.65.0)",
    comments = "Source: coordinator.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class CoordinatorServiceGrpc {

  private CoordinatorServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dbq.CoordinatorService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.RegisterBrokerRequest,
      com.dbq.proto.RegisterBrokerResponse> getRegisterBrokerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RegisterBroker",
      requestType = com.dbq.proto.RegisterBrokerRequest.class,
      responseType = com.dbq.proto.RegisterBrokerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.RegisterBrokerRequest,
      com.dbq.proto.RegisterBrokerResponse> getRegisterBrokerMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.RegisterBrokerRequest, com.dbq.proto.RegisterBrokerResponse> getRegisterBrokerMethod;
    if ((getRegisterBrokerMethod = CoordinatorServiceGrpc.getRegisterBrokerMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getRegisterBrokerMethod = CoordinatorServiceGrpc.getRegisterBrokerMethod) == null) {
          CoordinatorServiceGrpc.getRegisterBrokerMethod = getRegisterBrokerMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.RegisterBrokerRequest, com.dbq.proto.RegisterBrokerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RegisterBroker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.RegisterBrokerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.RegisterBrokerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("RegisterBroker"))
              .build();
        }
      }
    }
    return getRegisterBrokerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatRequest,
      com.dbq.proto.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = com.dbq.proto.HeartbeatRequest.class,
      responseType = com.dbq.proto.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatRequest,
      com.dbq.proto.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatRequest, com.dbq.proto.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = CoordinatorServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getHeartbeatMethod = CoordinatorServiceGrpc.getHeartbeatMethod) == null) {
          CoordinatorServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.HeartbeatRequest, com.dbq.proto.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.Empty,
      com.dbq.proto.GetLiveBrokersResponse> getGetLiveBrokersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLiveBrokers",
      requestType = com.dbq.proto.Empty.class,
      responseType = com.dbq.proto.GetLiveBrokersResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.Empty,
      com.dbq.proto.GetLiveBrokersResponse> getGetLiveBrokersMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.Empty, com.dbq.proto.GetLiveBrokersResponse> getGetLiveBrokersMethod;
    if ((getGetLiveBrokersMethod = CoordinatorServiceGrpc.getGetLiveBrokersMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetLiveBrokersMethod = CoordinatorServiceGrpc.getGetLiveBrokersMethod) == null) {
          CoordinatorServiceGrpc.getGetLiveBrokersMethod = getGetLiveBrokersMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.Empty, com.dbq.proto.GetLiveBrokersResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLiveBrokers"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetLiveBrokersResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetLiveBrokers"))
              .build();
        }
      }
    }
    return getGetLiveBrokersMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.CreateTopicRequest,
      com.dbq.proto.CreateTopicResponse> getCreateTopicMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTopic",
      requestType = com.dbq.proto.CreateTopicRequest.class,
      responseType = com.dbq.proto.CreateTopicResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.CreateTopicRequest,
      com.dbq.proto.CreateTopicResponse> getCreateTopicMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.CreateTopicRequest, com.dbq.proto.CreateTopicResponse> getCreateTopicMethod;
    if ((getCreateTopicMethod = CoordinatorServiceGrpc.getCreateTopicMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getCreateTopicMethod = CoordinatorServiceGrpc.getCreateTopicMethod) == null) {
          CoordinatorServiceGrpc.getCreateTopicMethod = getCreateTopicMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.CreateTopicRequest, com.dbq.proto.CreateTopicResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTopic"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.CreateTopicRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.CreateTopicResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("CreateTopic"))
              .build();
        }
      }
    }
    return getCreateTopicMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetTopicMetadataRequest,
      com.dbq.proto.GetTopicMetadataResponse> getGetTopicMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTopicMetadata",
      requestType = com.dbq.proto.GetTopicMetadataRequest.class,
      responseType = com.dbq.proto.GetTopicMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetTopicMetadataRequest,
      com.dbq.proto.GetTopicMetadataResponse> getGetTopicMetadataMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetTopicMetadataRequest, com.dbq.proto.GetTopicMetadataResponse> getGetTopicMetadataMethod;
    if ((getGetTopicMetadataMethod = CoordinatorServiceGrpc.getGetTopicMetadataMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetTopicMetadataMethod = CoordinatorServiceGrpc.getGetTopicMetadataMethod) == null) {
          CoordinatorServiceGrpc.getGetTopicMetadataMethod = getGetTopicMetadataMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetTopicMetadataRequest, com.dbq.proto.GetTopicMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTopicMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetTopicMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetTopicMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetTopicMetadata"))
              .build();
        }
      }
    }
    return getGetTopicMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionMetadataRequest,
      com.dbq.proto.GetPartitionMetadataResponse> getGetPartitionMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionMetadata",
      requestType = com.dbq.proto.GetPartitionMetadataRequest.class,
      responseType = com.dbq.proto.GetPartitionMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionMetadataRequest,
      com.dbq.proto.GetPartitionMetadataResponse> getGetPartitionMetadataMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionMetadataRequest, com.dbq.proto.GetPartitionMetadataResponse> getGetPartitionMetadataMethod;
    if ((getGetPartitionMetadataMethod = CoordinatorServiceGrpc.getGetPartitionMetadataMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetPartitionMetadataMethod = CoordinatorServiceGrpc.getGetPartitionMetadataMethod) == null) {
          CoordinatorServiceGrpc.getGetPartitionMetadataMethod = getGetPartitionMetadataMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetPartitionMetadataRequest, com.dbq.proto.GetPartitionMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPartitionMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetPartitionMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetPartitionMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetPartitionMetadata"))
              .build();
        }
      }
    }
    return getGetPartitionMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetLeaderRequest,
      com.dbq.proto.GetLeaderResponse> getGetLeaderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLeader",
      requestType = com.dbq.proto.GetLeaderRequest.class,
      responseType = com.dbq.proto.GetLeaderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetLeaderRequest,
      com.dbq.proto.GetLeaderResponse> getGetLeaderMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetLeaderRequest, com.dbq.proto.GetLeaderResponse> getGetLeaderMethod;
    if ((getGetLeaderMethod = CoordinatorServiceGrpc.getGetLeaderMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetLeaderMethod = CoordinatorServiceGrpc.getGetLeaderMethod) == null) {
          CoordinatorServiceGrpc.getGetLeaderMethod = getGetLeaderMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetLeaderRequest, com.dbq.proto.GetLeaderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLeader"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetLeaderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetLeaderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetLeader"))
              .build();
        }
      }
    }
    return getGetLeaderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetReplicasRequest,
      com.dbq.proto.GetReplicasResponse> getGetReplicasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetReplicas",
      requestType = com.dbq.proto.GetReplicasRequest.class,
      responseType = com.dbq.proto.GetReplicasResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetReplicasRequest,
      com.dbq.proto.GetReplicasResponse> getGetReplicasMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetReplicasRequest, com.dbq.proto.GetReplicasResponse> getGetReplicasMethod;
    if ((getGetReplicasMethod = CoordinatorServiceGrpc.getGetReplicasMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetReplicasMethod = CoordinatorServiceGrpc.getGetReplicasMethod) == null) {
          CoordinatorServiceGrpc.getGetReplicasMethod = getGetReplicasMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetReplicasRequest, com.dbq.proto.GetReplicasResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetReplicas"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetReplicasRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetReplicasResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetReplicas"))
              .build();
        }
      }
    }
    return getGetReplicasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.ElectLeaderRequest,
      com.dbq.proto.ElectLeaderResponse> getElectLeaderMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ElectLeader",
      requestType = com.dbq.proto.ElectLeaderRequest.class,
      responseType = com.dbq.proto.ElectLeaderResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.ElectLeaderRequest,
      com.dbq.proto.ElectLeaderResponse> getElectLeaderMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.ElectLeaderRequest, com.dbq.proto.ElectLeaderResponse> getElectLeaderMethod;
    if ((getElectLeaderMethod = CoordinatorServiceGrpc.getElectLeaderMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getElectLeaderMethod = CoordinatorServiceGrpc.getElectLeaderMethod) == null) {
          CoordinatorServiceGrpc.getElectLeaderMethod = getElectLeaderMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.ElectLeaderRequest, com.dbq.proto.ElectLeaderResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ElectLeader"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ElectLeaderRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ElectLeaderResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("ElectLeader"))
              .build();
        }
      }
    }
    return getElectLeaderMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.CommitOffsetRequest,
      com.dbq.proto.CommitOffsetResponse> getCommitOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitOffset",
      requestType = com.dbq.proto.CommitOffsetRequest.class,
      responseType = com.dbq.proto.CommitOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.CommitOffsetRequest,
      com.dbq.proto.CommitOffsetResponse> getCommitOffsetMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.CommitOffsetRequest, com.dbq.proto.CommitOffsetResponse> getCommitOffsetMethod;
    if ((getCommitOffsetMethod = CoordinatorServiceGrpc.getCommitOffsetMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getCommitOffsetMethod = CoordinatorServiceGrpc.getCommitOffsetMethod) == null) {
          CoordinatorServiceGrpc.getCommitOffsetMethod = getCommitOffsetMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.CommitOffsetRequest, com.dbq.proto.CommitOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CommitOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.CommitOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.CommitOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("CommitOffset"))
              .build();
        }
      }
    }
    return getCommitOffsetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetCommittedOffsetRequest,
      com.dbq.proto.GetCommittedOffsetResponse> getGetCommittedOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCommittedOffset",
      requestType = com.dbq.proto.GetCommittedOffsetRequest.class,
      responseType = com.dbq.proto.GetCommittedOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetCommittedOffsetRequest,
      com.dbq.proto.GetCommittedOffsetResponse> getGetCommittedOffsetMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetCommittedOffsetRequest, com.dbq.proto.GetCommittedOffsetResponse> getGetCommittedOffsetMethod;
    if ((getGetCommittedOffsetMethod = CoordinatorServiceGrpc.getGetCommittedOffsetMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getGetCommittedOffsetMethod = CoordinatorServiceGrpc.getGetCommittedOffsetMethod) == null) {
          CoordinatorServiceGrpc.getGetCommittedOffsetMethod = getGetCommittedOffsetMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetCommittedOffsetRequest, com.dbq.proto.GetCommittedOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCommittedOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetCommittedOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetCommittedOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("GetCommittedOffset"))
              .build();
        }
      }
    }
    return getGetCommittedOffsetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.JoinConsumerGroupRequest,
      com.dbq.proto.JoinConsumerGroupResponse> getJoinConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "JoinConsumerGroup",
      requestType = com.dbq.proto.JoinConsumerGroupRequest.class,
      responseType = com.dbq.proto.JoinConsumerGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.JoinConsumerGroupRequest,
      com.dbq.proto.JoinConsumerGroupResponse> getJoinConsumerGroupMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.JoinConsumerGroupRequest, com.dbq.proto.JoinConsumerGroupResponse> getJoinConsumerGroupMethod;
    if ((getJoinConsumerGroupMethod = CoordinatorServiceGrpc.getJoinConsumerGroupMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getJoinConsumerGroupMethod = CoordinatorServiceGrpc.getJoinConsumerGroupMethod) == null) {
          CoordinatorServiceGrpc.getJoinConsumerGroupMethod = getJoinConsumerGroupMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.JoinConsumerGroupRequest, com.dbq.proto.JoinConsumerGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "JoinConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.JoinConsumerGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.JoinConsumerGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("JoinConsumerGroup"))
              .build();
        }
      }
    }
    return getJoinConsumerGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatConsumerGroupRequest,
      com.dbq.proto.HeartbeatConsumerGroupResponse> getHeartbeatConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HeartbeatConsumerGroup",
      requestType = com.dbq.proto.HeartbeatConsumerGroupRequest.class,
      responseType = com.dbq.proto.HeartbeatConsumerGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatConsumerGroupRequest,
      com.dbq.proto.HeartbeatConsumerGroupResponse> getHeartbeatConsumerGroupMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.HeartbeatConsumerGroupRequest, com.dbq.proto.HeartbeatConsumerGroupResponse> getHeartbeatConsumerGroupMethod;
    if ((getHeartbeatConsumerGroupMethod = CoordinatorServiceGrpc.getHeartbeatConsumerGroupMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getHeartbeatConsumerGroupMethod = CoordinatorServiceGrpc.getHeartbeatConsumerGroupMethod) == null) {
          CoordinatorServiceGrpc.getHeartbeatConsumerGroupMethod = getHeartbeatConsumerGroupMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.HeartbeatConsumerGroupRequest, com.dbq.proto.HeartbeatConsumerGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HeartbeatConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.HeartbeatConsumerGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.HeartbeatConsumerGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("HeartbeatConsumerGroup"))
              .build();
        }
      }
    }
    return getHeartbeatConsumerGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.LeaveConsumerGroupRequest,
      com.dbq.proto.LeaveConsumerGroupResponse> getLeaveConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LeaveConsumerGroup",
      requestType = com.dbq.proto.LeaveConsumerGroupRequest.class,
      responseType = com.dbq.proto.LeaveConsumerGroupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.LeaveConsumerGroupRequest,
      com.dbq.proto.LeaveConsumerGroupResponse> getLeaveConsumerGroupMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.LeaveConsumerGroupRequest, com.dbq.proto.LeaveConsumerGroupResponse> getLeaveConsumerGroupMethod;
    if ((getLeaveConsumerGroupMethod = CoordinatorServiceGrpc.getLeaveConsumerGroupMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getLeaveConsumerGroupMethod = CoordinatorServiceGrpc.getLeaveConsumerGroupMethod) == null) {
          CoordinatorServiceGrpc.getLeaveConsumerGroupMethod = getLeaveConsumerGroupMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.LeaveConsumerGroupRequest, com.dbq.proto.LeaveConsumerGroupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "LeaveConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.LeaveConsumerGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.LeaveConsumerGroupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("LeaveConsumerGroup"))
              .build();
        }
      }
    }
    return getLeaveConsumerGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.UpdateReplicaProgressRequest,
      com.dbq.proto.UpdateReplicaProgressResponse> getUpdateReplicaProgressMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateReplicaProgress",
      requestType = com.dbq.proto.UpdateReplicaProgressRequest.class,
      responseType = com.dbq.proto.UpdateReplicaProgressResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.UpdateReplicaProgressRequest,
      com.dbq.proto.UpdateReplicaProgressResponse> getUpdateReplicaProgressMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.UpdateReplicaProgressRequest, com.dbq.proto.UpdateReplicaProgressResponse> getUpdateReplicaProgressMethod;
    if ((getUpdateReplicaProgressMethod = CoordinatorServiceGrpc.getUpdateReplicaProgressMethod) == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        if ((getUpdateReplicaProgressMethod = CoordinatorServiceGrpc.getUpdateReplicaProgressMethod) == null) {
          CoordinatorServiceGrpc.getUpdateReplicaProgressMethod = getUpdateReplicaProgressMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.UpdateReplicaProgressRequest, com.dbq.proto.UpdateReplicaProgressResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateReplicaProgress"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.UpdateReplicaProgressRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.UpdateReplicaProgressResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CoordinatorServiceMethodDescriptorSupplier("UpdateReplicaProgress"))
              .build();
        }
      }
    }
    return getUpdateReplicaProgressMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CoordinatorServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceStub>() {
        @java.lang.Override
        public CoordinatorServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CoordinatorServiceStub(channel, callOptions);
        }
      };
    return CoordinatorServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CoordinatorServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceBlockingStub>() {
        @java.lang.Override
        public CoordinatorServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CoordinatorServiceBlockingStub(channel, callOptions);
        }
      };
    return CoordinatorServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CoordinatorServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CoordinatorServiceFutureStub>() {
        @java.lang.Override
        public CoordinatorServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CoordinatorServiceFutureStub(channel, callOptions);
        }
      };
    return CoordinatorServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void registerBroker(com.dbq.proto.RegisterBrokerRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.RegisterBrokerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRegisterBrokerMethod(), responseObserver);
    }

    /**
     */
    default void heartbeat(com.dbq.proto.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    default void getLiveBrokers(com.dbq.proto.Empty request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetLiveBrokersResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLiveBrokersMethod(), responseObserver);
    }

    /**
     */
    default void createTopic(com.dbq.proto.CreateTopicRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.CreateTopicResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTopicMethod(), responseObserver);
    }

    /**
     */
    default void getTopicMetadata(com.dbq.proto.GetTopicMetadataRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetTopicMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTopicMetadataMethod(), responseObserver);
    }

    /**
     */
    default void getPartitionMetadata(com.dbq.proto.GetPartitionMetadataRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPartitionMetadataMethod(), responseObserver);
    }

    /**
     */
    default void getLeader(com.dbq.proto.GetLeaderRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetLeaderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLeaderMethod(), responseObserver);
    }

    /**
     */
    default void getReplicas(com.dbq.proto.GetReplicasRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetReplicasResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetReplicasMethod(), responseObserver);
    }

    /**
     */
    default void electLeader(com.dbq.proto.ElectLeaderRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ElectLeaderResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getElectLeaderMethod(), responseObserver);
    }

    /**
     */
    default void commitOffset(com.dbq.proto.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitOffsetMethod(), responseObserver);
    }

    /**
     */
    default void getCommittedOffset(com.dbq.proto.GetCommittedOffsetRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetCommittedOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCommittedOffsetMethod(), responseObserver);
    }

    /**
     */
    default void joinConsumerGroup(com.dbq.proto.JoinConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.JoinConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getJoinConsumerGroupMethod(), responseObserver);
    }

    /**
     */
    default void heartbeatConsumerGroup(com.dbq.proto.HeartbeatConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatConsumerGroupMethod(), responseObserver);
    }

    /**
     */
    default void leaveConsumerGroup(com.dbq.proto.LeaveConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.LeaveConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLeaveConsumerGroupMethod(), responseObserver);
    }

    /**
     */
    default void updateReplicaProgress(com.dbq.proto.UpdateReplicaProgressRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.UpdateReplicaProgressResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateReplicaProgressMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service CoordinatorService.
   */
  public static abstract class CoordinatorServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return CoordinatorServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service CoordinatorService.
   */
  public static final class CoordinatorServiceStub
      extends io.grpc.stub.AbstractAsyncStub<CoordinatorServiceStub> {
    private CoordinatorServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CoordinatorServiceStub(channel, callOptions);
    }

    /**
     */
    public void registerBroker(com.dbq.proto.RegisterBrokerRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.RegisterBrokerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRegisterBrokerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(com.dbq.proto.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLiveBrokers(com.dbq.proto.Empty request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetLiveBrokersResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLiveBrokersMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createTopic(com.dbq.proto.CreateTopicRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.CreateTopicResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTopicMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTopicMetadata(com.dbq.proto.GetTopicMetadataRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetTopicMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTopicMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionMetadata(com.dbq.proto.GetPartitionMetadataRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPartitionMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLeader(com.dbq.proto.GetLeaderRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetLeaderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLeaderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getReplicas(com.dbq.proto.GetReplicasRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetReplicasResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetReplicasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void electLeader(com.dbq.proto.ElectLeaderRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ElectLeaderResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getElectLeaderMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commitOffset(com.dbq.proto.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCommittedOffset(com.dbq.proto.GetCommittedOffsetRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetCommittedOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCommittedOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void joinConsumerGroup(com.dbq.proto.JoinConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.JoinConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getJoinConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeatConsumerGroup(com.dbq.proto.HeartbeatConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void leaveConsumerGroup(com.dbq.proto.LeaveConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.LeaveConsumerGroupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLeaveConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateReplicaProgress(com.dbq.proto.UpdateReplicaProgressRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.UpdateReplicaProgressResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateReplicaProgressMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service CoordinatorService.
   */
  public static final class CoordinatorServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<CoordinatorServiceBlockingStub> {
    private CoordinatorServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CoordinatorServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.dbq.proto.RegisterBrokerResponse registerBroker(com.dbq.proto.RegisterBrokerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRegisterBrokerMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.HeartbeatResponse heartbeat(com.dbq.proto.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetLiveBrokersResponse getLiveBrokers(com.dbq.proto.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLiveBrokersMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.CreateTopicResponse createTopic(com.dbq.proto.CreateTopicRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTopicMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetTopicMetadataResponse getTopicMetadata(com.dbq.proto.GetTopicMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTopicMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetPartitionMetadataResponse getPartitionMetadata(com.dbq.proto.GetPartitionMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPartitionMetadataMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetLeaderResponse getLeader(com.dbq.proto.GetLeaderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLeaderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetReplicasResponse getReplicas(com.dbq.proto.GetReplicasRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetReplicasMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.ElectLeaderResponse electLeader(com.dbq.proto.ElectLeaderRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getElectLeaderMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.CommitOffsetResponse commitOffset(com.dbq.proto.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitOffsetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetCommittedOffsetResponse getCommittedOffset(com.dbq.proto.GetCommittedOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCommittedOffsetMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.JoinConsumerGroupResponse joinConsumerGroup(com.dbq.proto.JoinConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinConsumerGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.HeartbeatConsumerGroupResponse heartbeatConsumerGroup(com.dbq.proto.HeartbeatConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatConsumerGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.LeaveConsumerGroupResponse leaveConsumerGroup(com.dbq.proto.LeaveConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLeaveConsumerGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.UpdateReplicaProgressResponse updateReplicaProgress(com.dbq.proto.UpdateReplicaProgressRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateReplicaProgressMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service CoordinatorService.
   */
  public static final class CoordinatorServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<CoordinatorServiceFutureStub> {
    private CoordinatorServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CoordinatorServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CoordinatorServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.RegisterBrokerResponse> registerBroker(
        com.dbq.proto.RegisterBrokerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRegisterBrokerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.HeartbeatResponse> heartbeat(
        com.dbq.proto.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetLiveBrokersResponse> getLiveBrokers(
        com.dbq.proto.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLiveBrokersMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.CreateTopicResponse> createTopic(
        com.dbq.proto.CreateTopicRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTopicMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetTopicMetadataResponse> getTopicMetadata(
        com.dbq.proto.GetTopicMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTopicMetadataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetPartitionMetadataResponse> getPartitionMetadata(
        com.dbq.proto.GetPartitionMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPartitionMetadataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetLeaderResponse> getLeader(
        com.dbq.proto.GetLeaderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLeaderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetReplicasResponse> getReplicas(
        com.dbq.proto.GetReplicasRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetReplicasMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.ElectLeaderResponse> electLeader(
        com.dbq.proto.ElectLeaderRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getElectLeaderMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.CommitOffsetResponse> commitOffset(
        com.dbq.proto.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetCommittedOffsetResponse> getCommittedOffset(
        com.dbq.proto.GetCommittedOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCommittedOffsetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.JoinConsumerGroupResponse> joinConsumerGroup(
        com.dbq.proto.JoinConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getJoinConsumerGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.HeartbeatConsumerGroupResponse> heartbeatConsumerGroup(
        com.dbq.proto.HeartbeatConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatConsumerGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.LeaveConsumerGroupResponse> leaveConsumerGroup(
        com.dbq.proto.LeaveConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLeaveConsumerGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.UpdateReplicaProgressResponse> updateReplicaProgress(
        com.dbq.proto.UpdateReplicaProgressRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateReplicaProgressMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER_BROKER = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_GET_LIVE_BROKERS = 2;
  private static final int METHODID_CREATE_TOPIC = 3;
  private static final int METHODID_GET_TOPIC_METADATA = 4;
  private static final int METHODID_GET_PARTITION_METADATA = 5;
  private static final int METHODID_GET_LEADER = 6;
  private static final int METHODID_GET_REPLICAS = 7;
  private static final int METHODID_ELECT_LEADER = 8;
  private static final int METHODID_COMMIT_OFFSET = 9;
  private static final int METHODID_GET_COMMITTED_OFFSET = 10;
  private static final int METHODID_JOIN_CONSUMER_GROUP = 11;
  private static final int METHODID_HEARTBEAT_CONSUMER_GROUP = 12;
  private static final int METHODID_LEAVE_CONSUMER_GROUP = 13;
  private static final int METHODID_UPDATE_REPLICA_PROGRESS = 14;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER_BROKER:
          serviceImpl.registerBroker((com.dbq.proto.RegisterBrokerRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.RegisterBrokerResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.dbq.proto.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_GET_LIVE_BROKERS:
          serviceImpl.getLiveBrokers((com.dbq.proto.Empty) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetLiveBrokersResponse>) responseObserver);
          break;
        case METHODID_CREATE_TOPIC:
          serviceImpl.createTopic((com.dbq.proto.CreateTopicRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.CreateTopicResponse>) responseObserver);
          break;
        case METHODID_GET_TOPIC_METADATA:
          serviceImpl.getTopicMetadata((com.dbq.proto.GetTopicMetadataRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetTopicMetadataResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_METADATA:
          serviceImpl.getPartitionMetadata((com.dbq.proto.GetPartitionMetadataRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionMetadataResponse>) responseObserver);
          break;
        case METHODID_GET_LEADER:
          serviceImpl.getLeader((com.dbq.proto.GetLeaderRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetLeaderResponse>) responseObserver);
          break;
        case METHODID_GET_REPLICAS:
          serviceImpl.getReplicas((com.dbq.proto.GetReplicasRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetReplicasResponse>) responseObserver);
          break;
        case METHODID_ELECT_LEADER:
          serviceImpl.electLeader((com.dbq.proto.ElectLeaderRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.ElectLeaderResponse>) responseObserver);
          break;
        case METHODID_COMMIT_OFFSET:
          serviceImpl.commitOffset((com.dbq.proto.CommitOffsetRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.CommitOffsetResponse>) responseObserver);
          break;
        case METHODID_GET_COMMITTED_OFFSET:
          serviceImpl.getCommittedOffset((com.dbq.proto.GetCommittedOffsetRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetCommittedOffsetResponse>) responseObserver);
          break;
        case METHODID_JOIN_CONSUMER_GROUP:
          serviceImpl.joinConsumerGroup((com.dbq.proto.JoinConsumerGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.JoinConsumerGroupResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT_CONSUMER_GROUP:
          serviceImpl.heartbeatConsumerGroup((com.dbq.proto.HeartbeatConsumerGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.HeartbeatConsumerGroupResponse>) responseObserver);
          break;
        case METHODID_LEAVE_CONSUMER_GROUP:
          serviceImpl.leaveConsumerGroup((com.dbq.proto.LeaveConsumerGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.LeaveConsumerGroupResponse>) responseObserver);
          break;
        case METHODID_UPDATE_REPLICA_PROGRESS:
          serviceImpl.updateReplicaProgress((com.dbq.proto.UpdateReplicaProgressRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.UpdateReplicaProgressResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getRegisterBrokerMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.RegisterBrokerRequest,
              com.dbq.proto.RegisterBrokerResponse>(
                service, METHODID_REGISTER_BROKER)))
        .addMethod(
          getHeartbeatMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.HeartbeatRequest,
              com.dbq.proto.HeartbeatResponse>(
                service, METHODID_HEARTBEAT)))
        .addMethod(
          getGetLiveBrokersMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.Empty,
              com.dbq.proto.GetLiveBrokersResponse>(
                service, METHODID_GET_LIVE_BROKERS)))
        .addMethod(
          getCreateTopicMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.CreateTopicRequest,
              com.dbq.proto.CreateTopicResponse>(
                service, METHODID_CREATE_TOPIC)))
        .addMethod(
          getGetTopicMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetTopicMetadataRequest,
              com.dbq.proto.GetTopicMetadataResponse>(
                service, METHODID_GET_TOPIC_METADATA)))
        .addMethod(
          getGetPartitionMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetPartitionMetadataRequest,
              com.dbq.proto.GetPartitionMetadataResponse>(
                service, METHODID_GET_PARTITION_METADATA)))
        .addMethod(
          getGetLeaderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetLeaderRequest,
              com.dbq.proto.GetLeaderResponse>(
                service, METHODID_GET_LEADER)))
        .addMethod(
          getGetReplicasMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetReplicasRequest,
              com.dbq.proto.GetReplicasResponse>(
                service, METHODID_GET_REPLICAS)))
        .addMethod(
          getElectLeaderMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.ElectLeaderRequest,
              com.dbq.proto.ElectLeaderResponse>(
                service, METHODID_ELECT_LEADER)))
        .addMethod(
          getCommitOffsetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.CommitOffsetRequest,
              com.dbq.proto.CommitOffsetResponse>(
                service, METHODID_COMMIT_OFFSET)))
        .addMethod(
          getGetCommittedOffsetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetCommittedOffsetRequest,
              com.dbq.proto.GetCommittedOffsetResponse>(
                service, METHODID_GET_COMMITTED_OFFSET)))
        .addMethod(
          getJoinConsumerGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.JoinConsumerGroupRequest,
              com.dbq.proto.JoinConsumerGroupResponse>(
                service, METHODID_JOIN_CONSUMER_GROUP)))
        .addMethod(
          getHeartbeatConsumerGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.HeartbeatConsumerGroupRequest,
              com.dbq.proto.HeartbeatConsumerGroupResponse>(
                service, METHODID_HEARTBEAT_CONSUMER_GROUP)))
        .addMethod(
          getLeaveConsumerGroupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.LeaveConsumerGroupRequest,
              com.dbq.proto.LeaveConsumerGroupResponse>(
                service, METHODID_LEAVE_CONSUMER_GROUP)))
        .addMethod(
          getUpdateReplicaProgressMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.UpdateReplicaProgressRequest,
              com.dbq.proto.UpdateReplicaProgressResponse>(
                service, METHODID_UPDATE_REPLICA_PROGRESS)))
        .build();
  }

  private static abstract class CoordinatorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CoordinatorServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.dbq.proto.Coordinator.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CoordinatorService");
    }
  }

  private static final class CoordinatorServiceFileDescriptorSupplier
      extends CoordinatorServiceBaseDescriptorSupplier {
    CoordinatorServiceFileDescriptorSupplier() {}
  }

  private static final class CoordinatorServiceMethodDescriptorSupplier
      extends CoordinatorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    CoordinatorServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CoordinatorServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CoordinatorServiceFileDescriptorSupplier())
              .addMethod(getRegisterBrokerMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getGetLiveBrokersMethod())
              .addMethod(getCreateTopicMethod())
              .addMethod(getGetTopicMetadataMethod())
              .addMethod(getGetPartitionMetadataMethod())
              .addMethod(getGetLeaderMethod())
              .addMethod(getGetReplicasMethod())
              .addMethod(getElectLeaderMethod())
              .addMethod(getCommitOffsetMethod())
              .addMethod(getGetCommittedOffsetMethod())
              .addMethod(getJoinConsumerGroupMethod())
              .addMethod(getHeartbeatConsumerGroupMethod())
              .addMethod(getLeaveConsumerGroupMethod())
              .addMethod(getUpdateReplicaProgressMethod())
              .build();
        }
      }
    }
    return result;
  }
}
