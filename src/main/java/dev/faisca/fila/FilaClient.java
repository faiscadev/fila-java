package dev.faisca.fila;

import fila.v1.FilaServiceGrpc;
import fila.v1.Messages;
import fila.v1.Service;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Client for the Fila message broker.
 *
 * <p>Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.
 *
 * <pre>{@code
 * try (FilaClient client = FilaClient.builder("localhost:5555").build()) {
 *     String msgId = client.enqueue("my-queue", Map.of("tenant", "acme"), "hello".getBytes());
 *     ConsumerHandle handle = client.consume("my-queue", msg -> {
 *         System.out.println("Received: " + msg.getId());
 *         client.ack("my-queue", msg.getId());
 *     });
 *     // ... later
 *     handle.cancel();
 * }
 * }</pre>
 */
public final class FilaClient implements AutoCloseable {
  private final ManagedChannel channel;
  private final FilaServiceGrpc.FilaServiceBlockingStub blockingStub;

  private FilaClient(ManagedChannel channel) {
    this.channel = channel;
    this.blockingStub = FilaServiceGrpc.newBlockingStub(channel);
  }

  /** Returns a new builder for configuring a {@link FilaClient}. */
  public static Builder builder(String address) {
    return new Builder(address);
  }

  /**
   * Enqueue a message to the specified queue.
   *
   * @param queue target queue name
   * @param headers message headers (may be empty)
   * @param payload message payload bytes
   * @return the broker-assigned message ID (UUIDv7)
   * @throws QueueNotFoundException if the queue does not exist
   * @throws RpcException for unexpected gRPC failures
   */
  public String enqueue(String queue, Map<String, String> headers, byte[] payload) {
    Service.EnqueueRequest req =
        Service.EnqueueRequest.newBuilder()
            .setQueue(queue)
            .putAllHeaders(headers)
            .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
            .build();
    try {
      Service.EnqueueResponse resp = blockingStub.enqueue(req);
      return resp.getMessageId();
    } catch (StatusRuntimeException e) {
      throw mapEnqueueError(e);
    }
  }

  /**
   * Open a streaming consumer on the specified queue.
   *
   * <p>Messages are delivered to the handler on a background thread. Nacked messages are
   * redelivered on the same stream. Call {@link ConsumerHandle#cancel()} to stop consuming.
   *
   * @param queue queue to consume from
   * @param handler callback invoked for each message
   * @return a handle to cancel the consumer
   * @throws QueueNotFoundException if the queue does not exist
   * @throws RpcException for unexpected gRPC failures
   */
  public ConsumerHandle consume(String queue, Consumer<ConsumeMessage> handler) {
    Service.ConsumeRequest req = Service.ConsumeRequest.newBuilder().setQueue(queue).build();

    Context.CancellableContext ctx = Context.current().withCancellation();
    Thread thread =
        new Thread(
            () -> {
              ctx.run(
                  () -> {
                    try {
                      Iterator<Service.ConsumeResponse> stream = blockingStub.consume(req);
                      while (stream.hasNext()) {
                        Service.ConsumeResponse resp = stream.next();
                        if (!resp.hasMessage() || resp.getMessage().getId().isEmpty()) {
                          continue;
                        }
                        handler.accept(buildConsumeMessage(resp.getMessage()));
                      }
                    } catch (StatusRuntimeException e) {
                      if (e.getStatus().getCode() != io.grpc.Status.Code.CANCELLED) {
                        throw mapConsumeError(e);
                      }
                    }
                  });
            },
            "fila-consumer-" + queue);
    thread.setDaemon(true);
    thread.start();
    return new ConsumerHandle(ctx, thread);
  }

  /**
   * Acknowledge a successfully processed message.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to acknowledge
   * @throws MessageNotFoundException if the message does not exist
   * @throws RpcException for unexpected gRPC failures
   */
  public void ack(String queue, String msgId) {
    Service.AckRequest req =
        Service.AckRequest.newBuilder().setQueue(queue).setMessageId(msgId).build();
    try {
      blockingStub.ack(req);
    } catch (StatusRuntimeException e) {
      throw mapAckError(e);
    }
  }

  /**
   * Negatively acknowledge a message that failed processing.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to nack
   * @param error description of the failure
   * @throws MessageNotFoundException if the message does not exist
   * @throws RpcException for unexpected gRPC failures
   */
  public void nack(String queue, String msgId, String error) {
    Service.NackRequest req =
        Service.NackRequest.newBuilder()
            .setQueue(queue)
            .setMessageId(msgId)
            .setError(error)
            .build();
    try {
      blockingStub.nack(req);
    } catch (StatusRuntimeException e) {
      throw mapNackError(e);
    }
  }

  /** Shut down the underlying gRPC channel. */
  @Override
  public void close() {
    channel.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    } catch (InterruptedException e) {
      channel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static ConsumeMessage buildConsumeMessage(Messages.Message msg) {
    Messages.MessageMetadata meta = msg.getMetadata();
    return new ConsumeMessage(
        msg.getId(),
        msg.getHeadersMap(),
        msg.getPayload().toByteArray(),
        meta.getFairnessKey(),
        meta.getAttemptCount(),
        meta.getQueueId());
  }

  private static FilaException mapEnqueueError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new QueueNotFoundException("enqueue: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  private static FilaException mapConsumeError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new QueueNotFoundException("consume: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  private static FilaException mapAckError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new MessageNotFoundException("ack: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  private static FilaException mapNackError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new MessageNotFoundException("nack: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  /** Builder for {@link FilaClient}. */
  public static final class Builder {
    private final String address;

    private Builder(String address) {
      this.address = address;
    }

    /** Build and connect the client. */
    public FilaClient build() {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
      return new FilaClient(channel);
    }
  }
}
