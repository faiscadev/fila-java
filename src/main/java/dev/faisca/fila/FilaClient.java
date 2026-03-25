package dev.faisca.fila;

import fila.v1.FilaServiceGrpc;
import fila.v1.Messages;
import fila.v1.Service;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Client for the Fila message broker.
 *
 * <p>Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.
 *
 * <p>By default, {@code enqueue()} routes through an opportunistic batcher that coalesces messages
 * at high load without adding latency at low load. Use {@link Builder#withBatchMode(BatchMode)} to
 * configure batching behavior.
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
  private static final Metadata.Key<String> LEADER_ADDR_KEY =
      Metadata.Key.of("x-fila-leader-addr", Metadata.ASCII_STRING_MARSHALLER);

  private final ManagedChannel channel;
  private final FilaServiceGrpc.FilaServiceBlockingStub blockingStub;
  private final byte[] caCertPem;
  private final byte[] clientCertPem;
  private final byte[] clientKeyPem;
  private final String apiKey;
  private final Batcher batcher;

  private FilaClient(
      ManagedChannel channel,
      byte[] caCertPem,
      byte[] clientCertPem,
      byte[] clientKeyPem,
      String apiKey,
      Batcher batcher) {
    this.channel = channel;
    this.blockingStub = FilaServiceGrpc.newBlockingStub(channel);
    this.caCertPem = caCertPem;
    this.clientCertPem = clientCertPem;
    this.clientKeyPem = clientKeyPem;
    this.apiKey = apiKey;
    this.batcher = batcher;
  }

  /** Returns a new builder for configuring a {@link FilaClient}. */
  public static Builder builder(String address) {
    return new Builder(address);
  }

  /**
   * Enqueue a message to the specified queue.
   *
   * <p>When batching is enabled (the default), the message is submitted to the background batcher
   * and may be coalesced with other messages. The method blocks until the message is acknowledged
   * by the broker.
   *
   * @param queue target queue name
   * @param headers message headers (may be empty)
   * @param payload message payload bytes
   * @return the broker-assigned message ID (UUIDv7)
   * @throws QueueNotFoundException if the queue does not exist
   * @throws RpcException for unexpected gRPC failures
   */
  public String enqueue(String queue, Map<String, String> headers, byte[] payload) {
    if (batcher != null) {
      CompletableFuture<String> future =
          batcher.submit(new EnqueueMessage(queue, headers, payload));
      try {
        return future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FilaException("enqueue interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof FilaException) {
          throw (FilaException) cause;
        }
        throw new RpcException(io.grpc.Status.Code.INTERNAL, cause.getMessage());
      }
    }

    return enqueueDirect(queue, headers, payload);
  }

  /**
   * Enqueue multiple messages in a single RPC call.
   *
   * <p>Each message is independently validated and processed. A failed message does not affect the
   * others in the batch. Returns a list of results with one entry per input message, in the same
   * order.
   *
   * <p>This bypasses the batcher and always uses the {@code Enqueue} RPC directly.
   *
   * @param messages the messages to enqueue
   * @return a list of results, one per input message
   * @throws RpcException for transport-level failures affecting the entire batch
   */
  public List<EnqueueResult> enqueueMany(List<EnqueueMessage> messages) {
    Service.EnqueueRequest.Builder reqBuilder = Service.EnqueueRequest.newBuilder();
    for (EnqueueMessage msg : messages) {
      reqBuilder.addMessages(
          Service.EnqueueMessage.newBuilder()
              .setQueue(msg.getQueue())
              .putAllHeaders(msg.getHeaders())
              .setPayload(com.google.protobuf.ByteString.copyFrom(msg.getPayload()))
              .build());
    }

    try {
      Service.EnqueueResponse resp = blockingStub.enqueue(reqBuilder.build());
      List<Service.EnqueueResult> protoResults = resp.getResultsList();
      List<EnqueueResult> results = new ArrayList<>(protoResults.size());
      for (Service.EnqueueResult r : protoResults) {
        switch (r.getResultCase()) {
          case MESSAGE_ID:
            results.add(EnqueueResult.success(r.getMessageId()));
            break;
          case ERROR:
            results.add(EnqueueResult.error(r.getError().getMessage()));
            break;
          default:
            results.add(EnqueueResult.error("no result from server"));
            break;
        }
      }
      return results;
    } catch (StatusRuntimeException e) {
      throw mapEnqueueError(e);
    }
  }

  /**
   * Open a streaming consumer on the specified queue.
   *
   * <p>Messages are delivered to the handler on a background thread. The handler transparently
   * receives messages from batched server responses. Nacked messages are redelivered on the same
   * stream. Call {@link ConsumerHandle#cancel()} to stop consuming.
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
                      consumeStream(stream, handler);
                    } catch (StatusRuntimeException e) {
                      if (e.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                        return;
                      }
                      String leaderAddr = extractLeaderAddr(e);
                      if (leaderAddr != null) {
                        retryOnLeader(leaderAddr, req, handler);
                      } else {
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
        Service.AckRequest.newBuilder()
            .addMessages(
                Service.AckMessage.newBuilder().setQueue(queue).setMessageId(msgId).build())
            .build();
    try {
      Service.AckResponse resp = blockingStub.ack(req);
      List<Service.AckResult> results = resp.getResultsList();
      if (results.size() != 1) {
        throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
      Service.AckResult first = results.get(0);
      if (first.getResultCase() == Service.AckResult.ResultCase.ERROR) {
        throw mapAckResultError(first.getError());
      }
      if (first.getResultCase() == Service.AckResult.ResultCase.RESULT_NOT_SET) {
        throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
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
            .addMessages(
                Service.NackMessage.newBuilder()
                    .setQueue(queue)
                    .setMessageId(msgId)
                    .setError(error)
                    .build())
            .build();
    try {
      Service.NackResponse resp = blockingStub.nack(req);
      List<Service.NackResult> results = resp.getResultsList();
      if (results.size() != 1) {
        throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
      Service.NackResult first = results.get(0);
      if (first.getResultCase() == Service.NackResult.ResultCase.ERROR) {
        throw mapNackResultError(first.getError());
      }
      if (first.getResultCase() == Service.NackResult.ResultCase.RESULT_NOT_SET) {
        throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
    } catch (StatusRuntimeException e) {
      throw mapNackError(e);
    }
  }

  /**
   * Shut down the client, draining any pending batched messages before disconnecting.
   *
   * <p>If a batcher is running, pending messages are flushed before the gRPC channel is closed.
   */
  @Override
  public void close() {
    if (batcher != null) {
      batcher.shutdown();
    }
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

  /** Direct single-message enqueue RPC (no batcher). */
  private String enqueueDirect(String queue, Map<String, String> headers, byte[] payload) {
    Service.EnqueueRequest req =
        Service.EnqueueRequest.newBuilder()
            .addMessages(
                Service.EnqueueMessage.newBuilder()
                    .setQueue(queue)
                    .putAllHeaders(headers)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .build())
            .build();
    try {
      Service.EnqueueResponse resp = blockingStub.enqueue(req);
      List<Service.EnqueueResult> results = resp.getResultsList();
      if (results.isEmpty()) {
        throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
      Service.EnqueueResult first = results.get(0);
      switch (first.getResultCase()) {
        case MESSAGE_ID:
          return first.getMessageId();
        case ERROR:
          throw mapEnqueueResultError(first.getError());
        default:
          throw new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server");
      }
    } catch (StatusRuntimeException e) {
      throw mapEnqueueError(e);
    }
  }

  /** Consume a stream, unpacking batched responses into individual messages. */
  private static void consumeStream(
      Iterator<Service.ConsumeResponse> stream, Consumer<ConsumeMessage> handler) {
    while (stream.hasNext()) {
      Service.ConsumeResponse resp = stream.next();

      List<Messages.Message> messages = resp.getMessagesList();
      for (Messages.Message msg : messages) {
        if (msg.getId().isEmpty()) {
          continue;
        }
        handler.accept(buildConsumeMessage(msg));
      }
    }
  }

  private static String extractLeaderAddr(StatusRuntimeException e) {
    if (e.getStatus().getCode() != io.grpc.Status.Code.UNAVAILABLE) {
      return null;
    }
    Metadata trailers = e.getTrailers();
    if (trailers == null) {
      return null;
    }
    return trailers.get(LEADER_ADDR_KEY);
  }

  private static void validateLeaderAddr(String addr) {
    if (addr == null || addr.isEmpty()) {
      throw new FilaException("invalid leader address: empty");
    }
    // Must not contain scheme (e.g. "http://") or path (e.g. "/foo")
    if (addr.contains("//") || addr.contains("/")) {
      throw new FilaException("invalid leader address: must be host:port, got: " + addr);
    }
    int colonIdx = addr.lastIndexOf(':');
    if (colonIdx < 0) {
      throw new FilaException("invalid leader address: missing port, got: " + addr);
    }
    String host = addr.substring(0, colonIdx);
    String portStr = addr.substring(colonIdx + 1);
    if (host.isEmpty()) {
      throw new FilaException("invalid leader address: empty host, got: " + addr);
    }
    int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (NumberFormatException ex) {
      throw new FilaException("invalid leader address: non-numeric port, got: " + addr);
    }
    if (port < 1 || port > 65535) {
      throw new FilaException("invalid leader address: port out of range, got: " + addr);
    }
  }

  private void retryOnLeader(
      String leaderAddr, Service.ConsumeRequest req, Consumer<ConsumeMessage> handler) {
    validateLeaderAddr(leaderAddr);
    ManagedChannel leaderChannel = buildChannel(leaderAddr);
    try {
      FilaServiceGrpc.FilaServiceBlockingStub leaderStub =
          FilaServiceGrpc.newBlockingStub(leaderChannel);
      Iterator<Service.ConsumeResponse> stream = leaderStub.consume(req);
      consumeStream(stream, handler);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != io.grpc.Status.Code.CANCELLED) {
        throw mapConsumeError(e);
      }
    } finally {
      leaderChannel.shutdown();
    }
  }

  private ManagedChannel buildChannel(String address) {
    if (caCertPem != null) {
      try {
        TlsChannelCredentials.Builder tlsBuilder =
            TlsChannelCredentials.newBuilder().trustManager(new ByteArrayInputStream(caCertPem));
        if (clientCertPem != null && clientKeyPem != null) {
          tlsBuilder.keyManager(
              new ByteArrayInputStream(clientCertPem), new ByteArrayInputStream(clientKeyPem));
        }
        ChannelCredentials creds = tlsBuilder.build();
        var channelBuilder =
            Grpc.newChannelBuilderForAddress(
                Builder.parseHost(address), Builder.parsePort(address), creds);
        if (apiKey != null) {
          channelBuilder.intercept(new ApiKeyInterceptor(apiKey));
        }
        return channelBuilder.build();
      } catch (IOException e) {
        throw new FilaException("failed to configure TLS for leader redirect", e);
      }
    } else {
      var channelBuilder = ManagedChannelBuilder.forTarget(address).usePlaintext();
      if (apiKey != null) {
        channelBuilder.intercept(new ApiKeyInterceptor(apiKey));
      }
      return channelBuilder.build();
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

  static FilaException mapEnqueueError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new QueueNotFoundException("enqueue: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  private static FilaException mapEnqueueResultError(Service.EnqueueError error) {
    return switch (error.getCode()) {
      case ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND ->
          new QueueNotFoundException("enqueue: " + error.getMessage());
      case ENQUEUE_ERROR_CODE_PERMISSION_DENIED ->
          new RpcException(io.grpc.Status.Code.PERMISSION_DENIED, error.getMessage());
      default -> new RpcException(io.grpc.Status.Code.INTERNAL, error.getMessage());
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

  private static FilaException mapAckResultError(Service.AckError error) {
    return switch (error.getCode()) {
      case ACK_ERROR_CODE_MESSAGE_NOT_FOUND ->
          new MessageNotFoundException("ack: " + error.getMessage());
      case ACK_ERROR_CODE_PERMISSION_DENIED ->
          new RpcException(io.grpc.Status.Code.PERMISSION_DENIED, error.getMessage());
      default -> new RpcException(io.grpc.Status.Code.INTERNAL, error.getMessage());
    };
  }

  private static FilaException mapNackError(StatusRuntimeException e) {
    return switch (e.getStatus().getCode()) {
      case NOT_FOUND -> new MessageNotFoundException("nack: " + e.getStatus().getDescription());
      default -> new RpcException(e.getStatus().getCode(), e.getStatus().getDescription());
    };
  }

  private static FilaException mapNackResultError(Service.NackError error) {
    return switch (error.getCode()) {
      case NACK_ERROR_CODE_MESSAGE_NOT_FOUND ->
          new MessageNotFoundException("nack: " + error.getMessage());
      case NACK_ERROR_CODE_PERMISSION_DENIED ->
          new RpcException(io.grpc.Status.Code.PERMISSION_DENIED, error.getMessage());
      default -> new RpcException(io.grpc.Status.Code.INTERNAL, error.getMessage());
    };
  }

  /** Builder for {@link FilaClient}. */
  public static final class Builder {
    private final String address;
    private boolean tlsEnabled;
    private byte[] caCertPem;
    private byte[] clientCertPem;
    private byte[] clientKeyPem;
    private String apiKey;
    private BatchMode batchMode = BatchMode.auto();

    private Builder(String address) {
      this.address = address;
    }

    /**
     * Enable TLS using the JVM's default trust store (cacerts).
     *
     * <p>Use this when the Fila server's certificate is issued by a public CA already trusted by
     * the JVM. For servers using self-signed or private CA certificates, use {@link
     * #withTlsCaCert(byte[])} instead.
     *
     * @return this builder
     */
    public Builder withTls() {
      this.tlsEnabled = true;
      return this;
    }

    /**
     * Set the CA certificate for TLS server verification.
     *
     * <p>When set, the client connects over TLS instead of plaintext. The CA certificate is used to
     * verify the server's identity. Implies {@link #withTls()}.
     *
     * @param caCertPem PEM-encoded CA certificate bytes
     * @return this builder
     */
    public Builder withTlsCaCert(byte[] caCertPem) {
      this.caCertPem = caCertPem;
      this.tlsEnabled = true;
      return this;
    }

    /**
     * Set the client certificate and key for mutual TLS (mTLS).
     *
     * <p>Requires either {@link #withTls()} or {@link #withTlsCaCert(byte[])} to be called first.
     * When provided, the client presents its certificate to the server for mutual authentication.
     *
     * @param certPem PEM-encoded client certificate bytes
     * @param keyPem PEM-encoded client private key bytes
     * @return this builder
     */
    public Builder withTlsClientCert(byte[] certPem, byte[] keyPem) {
      this.clientCertPem = certPem;
      this.clientKeyPem = keyPem;
      return this;
    }

    /**
     * Set an API key for authentication.
     *
     * <p>When set, the key is sent as a {@code Bearer} token in the {@code authorization} metadata
     * header on every outgoing RPC.
     *
     * @param apiKey the API key string
     * @return this builder
     */
    public Builder withApiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    /**
     * Set the batching mode for {@link FilaClient#enqueue} calls.
     *
     * <p>Default is {@link BatchMode#auto()} -- opportunistic batching. Use {@link
     * BatchMode#disabled()} to turn off batching entirely.
     *
     * @param batchMode the batch mode
     * @return this builder
     */
    public Builder withBatchMode(BatchMode batchMode) {
      this.batchMode = batchMode;
      return this;
    }

    /** Build and connect the client. */
    public FilaClient build() {
      if (clientCertPem != null && !tlsEnabled) {
        throw new FilaException(
            "client certificate requires TLS — call withTls() or withTlsCaCert() first");
      }

      ManagedChannel channel;

      if (tlsEnabled) {
        // Parse host/port before the TLS try block so that NumberFormatException
        // (a subclass of IllegalArgumentException) from address parsing is not
        // misreported as "invalid certificate".
        String host = parseHost(address);
        int port = parsePort(address);

        try {
          TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder();

          if (caCertPem != null) {
            tlsBuilder.trustManager(new ByteArrayInputStream(caCertPem));
          }

          if (clientCertPem != null && clientKeyPem != null) {
            tlsBuilder.keyManager(
                new ByteArrayInputStream(clientCertPem), new ByteArrayInputStream(clientKeyPem));
          }

          ChannelCredentials creds = tlsBuilder.build();
          var channelBuilder = Grpc.newChannelBuilderForAddress(host, port, creds);

          if (apiKey != null) {
            channelBuilder.intercept(new ApiKeyInterceptor(apiKey));
          }

          channel = channelBuilder.build();
        } catch (IllegalArgumentException e) {
          throw new FilaException("failed to configure TLS: invalid certificate", e);
        } catch (IOException e) {
          throw new FilaException("failed to configure TLS", e);
        }
      } else {
        var channelBuilder = ManagedChannelBuilder.forTarget(address).usePlaintext();

        if (apiKey != null) {
          channelBuilder.intercept(new ApiKeyInterceptor(apiKey));
        }

        channel = channelBuilder.build();
      }

      Batcher batcherInstance = null;
      if (batchMode.getKind() != BatchMode.Kind.DISABLED) {
        FilaServiceGrpc.FilaServiceBlockingStub batcherStub =
            FilaServiceGrpc.newBlockingStub(channel);
        if (apiKey != null) {
          // The stub needs the interceptor applied at channel level (already done above).
          // No additional interceptor needed on the stub.
        }
        batcherInstance = new Batcher(batcherStub, batchMode);
      }

      return new FilaClient(
          channel, caCertPem, clientCertPem, clientKeyPem, apiKey, batcherInstance);
    }

    static String parseHost(String address) {
      // Handle IPv6 bracket notation: [::1]:5555
      if (address.startsWith("[")) {
        int closeBracket = address.indexOf(']');
        if (closeBracket < 0) {
          return address;
        }
        return address.substring(1, closeBracket);
      }
      int colonIdx = address.lastIndexOf(':');
      if (colonIdx < 0) {
        return address;
      }
      return address.substring(0, colonIdx);
    }

    static int parsePort(String address) {
      // Handle IPv6 bracket notation: [::1]:5555
      if (address.startsWith("[")) {
        int closeBracket = address.indexOf(']');
        if (closeBracket < 0 || closeBracket + 2 > address.length()) {
          return 5555;
        }
        return Integer.parseInt(address.substring(closeBracket + 2));
      }
      int colonIdx = address.lastIndexOf(':');
      if (colonIdx < 0) {
        return 5555;
      }
      return Integer.parseInt(address.substring(colonIdx + 1));
    }
  }
}
