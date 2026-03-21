package dev.faisca.fila;

import fila.v1.FilaServiceGrpc;
import fila.v1.Messages;
import fila.v1.Service;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    private boolean tlsEnabled;
    private byte[] caCertPem;
    private byte[] clientCertPem;
    private byte[] clientKeyPem;
    private String apiKey;

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

    /** Build and connect the client. */
    public FilaClient build() {
      if (clientCertPem != null && !tlsEnabled) {
        throw new IllegalStateException(
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

      return new FilaClient(channel);
    }

    private static String parseHost(String address) {
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

    private static int parsePort(String address) {
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
