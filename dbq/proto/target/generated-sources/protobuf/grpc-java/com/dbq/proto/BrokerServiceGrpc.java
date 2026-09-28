package com.dbq.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.65.0)",
    comments = "Source: broker.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BrokerServiceGrpc {

  private BrokerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "dbq.BrokerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.ProduceRequest,
      com.dbq.proto.ProduceResponse> getProduceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Produce",
      requestType = com.dbq.proto.ProduceRequest.class,
      responseType = com.dbq.proto.ProduceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.ProduceRequest,
      com.dbq.proto.ProduceResponse> getProduceMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.ProduceRequest, com.dbq.proto.ProduceResponse> getProduceMethod;
    if ((getProduceMethod = BrokerServiceGrpc.getProduceMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getProduceMethod = BrokerServiceGrpc.getProduceMethod) == null) {
          BrokerServiceGrpc.getProduceMethod = getProduceMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.ProduceRequest, com.dbq.proto.ProduceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Produce"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ProduceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ProduceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("Produce"))
              .build();
        }
      }
    }
    return getProduceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.FetchRequest,
      com.dbq.proto.FetchResponse> getFetchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Fetch",
      requestType = com.dbq.proto.FetchRequest.class,
      responseType = com.dbq.proto.FetchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.FetchRequest,
      com.dbq.proto.FetchResponse> getFetchMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.FetchRequest, com.dbq.proto.FetchResponse> getFetchMethod;
    if ((getFetchMethod = BrokerServiceGrpc.getFetchMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getFetchMethod = BrokerServiceGrpc.getFetchMethod) == null) {
          BrokerServiceGrpc.getFetchMethod = getFetchMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.FetchRequest, com.dbq.proto.FetchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Fetch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.FetchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.FetchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("Fetch"))
              .build();
        }
      }
    }
    return getFetchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionStateRequest,
      com.dbq.proto.GetPartitionStateResponse> getGetPartitionStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionState",
      requestType = com.dbq.proto.GetPartitionStateRequest.class,
      responseType = com.dbq.proto.GetPartitionStateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionStateRequest,
      com.dbq.proto.GetPartitionStateResponse> getGetPartitionStateMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.GetPartitionStateRequest, com.dbq.proto.GetPartitionStateResponse> getGetPartitionStateMethod;
    if ((getGetPartitionStateMethod = BrokerServiceGrpc.getGetPartitionStateMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getGetPartitionStateMethod = BrokerServiceGrpc.getGetPartitionStateMethod) == null) {
          BrokerServiceGrpc.getGetPartitionStateMethod = getGetPartitionStateMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.GetPartitionStateRequest, com.dbq.proto.GetPartitionStateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPartitionState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetPartitionStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.GetPartitionStateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("GetPartitionState"))
              .build();
        }
      }
    }
    return getGetPartitionStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dbq.proto.ReplicateRequest,
      com.dbq.proto.ReplicateResponse> getReplicateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Replicate",
      requestType = com.dbq.proto.ReplicateRequest.class,
      responseType = com.dbq.proto.ReplicateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dbq.proto.ReplicateRequest,
      com.dbq.proto.ReplicateResponse> getReplicateMethod() {
    io.grpc.MethodDescriptor<com.dbq.proto.ReplicateRequest, com.dbq.proto.ReplicateResponse> getReplicateMethod;
    if ((getReplicateMethod = BrokerServiceGrpc.getReplicateMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getReplicateMethod = BrokerServiceGrpc.getReplicateMethod) == null) {
          BrokerServiceGrpc.getReplicateMethod = getReplicateMethod =
              io.grpc.MethodDescriptor.<com.dbq.proto.ReplicateRequest, com.dbq.proto.ReplicateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Replicate"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ReplicateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dbq.proto.ReplicateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("Replicate"))
              .build();
        }
      }
    }
    return getReplicateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BrokerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceStub>() {
        @java.lang.Override
        public BrokerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceStub(channel, callOptions);
        }
      };
    return BrokerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BrokerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceBlockingStub>() {
        @java.lang.Override
        public BrokerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceBlockingStub(channel, callOptions);
        }
      };
    return BrokerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BrokerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceFutureStub>() {
        @java.lang.Override
        public BrokerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceFutureStub(channel, callOptions);
        }
      };
    return BrokerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void produce(com.dbq.proto.ProduceRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ProduceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getProduceMethod(), responseObserver);
    }

    /**
     */
    default void fetch(com.dbq.proto.FetchRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.FetchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFetchMethod(), responseObserver);
    }

    /**
     */
    default void getPartitionState(com.dbq.proto.GetPartitionStateRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionStateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPartitionStateMethod(), responseObserver);
    }

    /**
     */
    default void replicate(com.dbq.proto.ReplicateRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ReplicateResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReplicateMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service BrokerService.
   */
  public static abstract class BrokerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return BrokerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service BrokerService.
   */
  public static final class BrokerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<BrokerServiceStub> {
    private BrokerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceStub(channel, callOptions);
    }

    /**
     */
    public void produce(com.dbq.proto.ProduceRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ProduceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getProduceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void fetch(com.dbq.proto.FetchRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.FetchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFetchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionState(com.dbq.proto.GetPartitionStateRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionStateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPartitionStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void replicate(com.dbq.proto.ReplicateRequest request,
        io.grpc.stub.StreamObserver<com.dbq.proto.ReplicateResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReplicateMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service BrokerService.
   */
  public static final class BrokerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<BrokerServiceBlockingStub> {
    private BrokerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.dbq.proto.ProduceResponse produce(com.dbq.proto.ProduceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getProduceMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.FetchResponse fetch(com.dbq.proto.FetchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFetchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.GetPartitionStateResponse getPartitionState(com.dbq.proto.GetPartitionStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPartitionStateMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dbq.proto.ReplicateResponse replicate(com.dbq.proto.ReplicateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReplicateMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service BrokerService.
   */
  public static final class BrokerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<BrokerServiceFutureStub> {
    private BrokerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.ProduceResponse> produce(
        com.dbq.proto.ProduceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getProduceMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.FetchResponse> fetch(
        com.dbq.proto.FetchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFetchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.GetPartitionStateResponse> getPartitionState(
        com.dbq.proto.GetPartitionStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPartitionStateMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dbq.proto.ReplicateResponse> replicate(
        com.dbq.proto.ReplicateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReplicateMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PRODUCE = 0;
  private static final int METHODID_FETCH = 1;
  private static final int METHODID_GET_PARTITION_STATE = 2;
  private static final int METHODID_REPLICATE = 3;

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
        case METHODID_PRODUCE:
          serviceImpl.produce((com.dbq.proto.ProduceRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.ProduceResponse>) responseObserver);
          break;
        case METHODID_FETCH:
          serviceImpl.fetch((com.dbq.proto.FetchRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.FetchResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_STATE:
          serviceImpl.getPartitionState((com.dbq.proto.GetPartitionStateRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.GetPartitionStateResponse>) responseObserver);
          break;
        case METHODID_REPLICATE:
          serviceImpl.replicate((com.dbq.proto.ReplicateRequest) request,
              (io.grpc.stub.StreamObserver<com.dbq.proto.ReplicateResponse>) responseObserver);
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
          getProduceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.ProduceRequest,
              com.dbq.proto.ProduceResponse>(
                service, METHODID_PRODUCE)))
        .addMethod(
          getFetchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.FetchRequest,
              com.dbq.proto.FetchResponse>(
                service, METHODID_FETCH)))
        .addMethod(
          getGetPartitionStateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.GetPartitionStateRequest,
              com.dbq.proto.GetPartitionStateResponse>(
                service, METHODID_GET_PARTITION_STATE)))
        .addMethod(
          getReplicateMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dbq.proto.ReplicateRequest,
              com.dbq.proto.ReplicateResponse>(
                service, METHODID_REPLICATE)))
        .build();
  }

  private static abstract class BrokerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BrokerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.dbq.proto.Broker.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BrokerService");
    }
  }

  private static final class BrokerServiceFileDescriptorSupplier
      extends BrokerServiceBaseDescriptorSupplier {
    BrokerServiceFileDescriptorSupplier() {}
  }

  private static final class BrokerServiceMethodDescriptorSupplier
      extends BrokerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    BrokerServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (BrokerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BrokerServiceFileDescriptorSupplier())
              .addMethod(getProduceMethod())
              .addMethod(getFetchMethod())
              .addMethod(getGetPartitionStateMethod())
              .addMethod(getReplicateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
