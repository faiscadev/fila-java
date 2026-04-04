package dev.faisca.fila;

import dev.faisca.fila.fibp.Codec;
import dev.faisca.fila.fibp.Connection;
import dev.faisca.fila.fibp.Opcodes;
import dev.faisca.fila.fibp.Primitives;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Client for the Fila message broker using the FIBP binary protocol.
 *
 * <p>Wraps the hot-path operations: enqueue, consume, ack, nack. Also exposes admin and auth
 * operations.
 *
 * <p>By default, {@code enqueue()} routes through an opportunistic batcher that coalesces messages
 * at high load without adding latency at low load.
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
  private final Connection connection;
  private final byte[] caCertPem;
  private final byte[] clientCertPem;
  private final byte[] clientKeyPem;
  private final String apiKey;
  private final Batcher batcher;

  private FilaClient(
      Connection connection,
      byte[] caCertPem,
      byte[] clientCertPem,
      byte[] clientKeyPem,
      String apiKey,
      Batcher batcher) {
    this.connection = connection;
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
   * @param queue target queue name
   * @param headers message headers (may be empty)
   * @param payload message payload bytes
   * @return the broker-assigned message ID (UUIDv7)
   * @throws QueueNotFoundException if the queue does not exist
   * @throws RpcException for unexpected protocol failures
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
        if (cause instanceof FilaException fe) {
          throw fe;
        }
        throw new RpcException(Opcodes.ERR_INTERNAL, cause.getMessage());
      }
    }

    return enqueueDirect(queue, headers, payload);
  }

  /**
   * Enqueue multiple messages in a single RPC call.
   *
   * @param messages the messages to enqueue
   * @return a list of results, one per input message
   * @throws RpcException for transport-level failures affecting the entire batch
   */
  @SuppressWarnings("unchecked")
  public List<EnqueueResult> enqueueMany(List<EnqueueMessage> messages) {
    int count = messages.size();
    String[] queues = new String[count];
    Map<String, String>[] headers = new Map[count];
    byte[][] payloads = new byte[count][];
    for (int i = 0; i < count; i++) {
      EnqueueMessage msg = messages.get(i);
      queues[i] = msg.getQueue();
      headers[i] = msg.getHeaders();
      payloads[i] = msg.getPayload();
    }

    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeEnqueue(requestId, queues, headers, payloads);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.ENQUEUE_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      long resultCount = r.readU32();
      List<EnqueueResult> results = new ArrayList<>((int) resultCount);
      for (int i = 0; i < resultCount; i++) {
        int errorCode = r.readU8();
        String messageId = r.readString();
        if (errorCode == Opcodes.ERR_OK) {
          results.add(EnqueueResult.success(messageId));
        } else {
          results.add(EnqueueResult.error(messageId.isEmpty() ? errorName(errorCode) : messageId));
        }
      }
      return results;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("enqueueMany failed", e);
    } catch (IOException e) {
      throw new FilaException("enqueueMany failed", e);
    }
  }

  /**
   * Open a streaming consumer on the specified queue.
   *
   * @param queue queue to consume from
   * @param handler callback invoked for each message
   * @return a handle to cancel the consumer
   */
  public ConsumerHandle consume(String queue, Consumer<ConsumeMessage> handler) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeConsume(requestId, queue);

    LinkedBlockingQueue<Connection.Frame> deliveryChan =
        connection.registerDeliveryChannel(requestId);

    AtomicBoolean cancelled = new AtomicBoolean(false);

    Thread thread =
        new Thread(
            () -> {
              try {
                // Send subscribe request
                Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);

                byte opcode = response.header().opcode();
                if (opcode == Opcodes.ERROR) {
                  Primitives.Reader er = new Primitives.Reader(response.body());
                  int errorCode = er.readU8();
                  String message = er.readString();
                  Map<String, String> metadata = er.readStringMap();

                  if (errorCode == Opcodes.ERR_NOT_LEADER) {
                    String leaderAddr = metadata.get("leader_addr");
                    if (leaderAddr != null) {
                      retryConsumeOnLeader(leaderAddr, queue, handler);
                      return;
                    }
                  }
                  throw mapErrorCode(errorCode, message);
                }

                if (opcode != Opcodes.CONSUME_OK) {
                  throw new RpcException(
                      Opcodes.ERR_INTERNAL, "unexpected consume response opcode");
                }

                // ConsumeOk received, now read deliveries from the channel
                // The delivery frames use the same requestId
                while (!cancelled.get() && !connection.isClosed()) {
                  Connection.Frame delivery = deliveryChan.poll(1, TimeUnit.SECONDS);
                  if (delivery == null) {
                    continue;
                  }
                  if (delivery.header().opcode() == Opcodes.ERROR) {
                    break;
                  }
                  if (delivery.header().opcode() != Opcodes.DELIVERY) {
                    continue;
                  }

                  Primitives.Reader dr = new Primitives.Reader(delivery.body());
                  long msgCount = dr.readU32();
                  for (long m = 0; m < msgCount; m++) {
                    String msgId = dr.readString();
                    String msgQueue = dr.readString();
                    Map<String, String> msgHeaders = dr.readStringMap();
                    byte[] msgPayload = dr.readBytes();
                    String fairnessKey = dr.readString();
                    long weight = dr.readU32();
                    String[] throttleKeys = dr.readStringList();
                    long attemptCount = dr.readU32();
                    long enqueuedAt = dr.readU64();
                    long leasedAt = dr.readU64();

                    if (msgId.isEmpty()) {
                      continue;
                    }

                    handler.accept(
                        new ConsumeMessage(
                            msgId,
                            msgHeaders,
                            msgPayload,
                            fairnessKey,
                            (int) attemptCount,
                            msgQueue));
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!cancelled.get()) {
                  throw new FilaException("consume stream failed", e);
                }
              } catch (IOException e) {
                if (!cancelled.get()) {
                  throw new FilaException("consume stream failed", e);
                }
              } finally {
                connection.unregisterDeliveryChannel(requestId);
              }
            },
            "fila-consumer-" + queue);
    thread.setDaemon(true);
    thread.start();

    Runnable cancelAction =
        () -> {
          cancelled.set(true);
          try {
            connection.send(Codec.encodeCancelConsume(requestId));
          } catch (IOException ignored) {
            // best effort
          }
          connection.unregisterDeliveryChannel(requestId);
        };

    return new ConsumerHandle(cancelAction, thread);
  }

  /**
   * Acknowledge a successfully processed message.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to acknowledge
   */
  public void ack(String queue, String msgId) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeAck(requestId, new String[] {queue}, new String[] {msgId});

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.ACK_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected ack response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      long resultCount = r.readU32();
      if (resultCount < 1) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "no result from server");
      }
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "ack failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("ack failed", e);
    } catch (IOException e) {
      throw new FilaException("ack failed", e);
    }
  }

  /**
   * Negatively acknowledge a message that failed processing.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to nack
   * @param error description of the failure
   */
  public void nack(String queue, String msgId, String error) {
    int requestId = connection.nextRequestId();
    byte[] frame =
        Codec.encodeNack(
            requestId, new String[] {queue}, new String[] {msgId}, new String[] {error});

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.NACK_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected nack response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      long resultCount = r.readU32();
      if (resultCount < 1) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "no result from server");
      }
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "nack failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("nack failed", e);
    } catch (IOException e) {
      throw new FilaException("nack failed", e);
    }
  }

  // --- Admin operations ---

  /**
   * Create a queue on the server.
   *
   * @param name queue name
   */
  public void createQueue(String name) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeCreateQueue(requestId, name, null, null, 0);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.CREATE_QUEUE_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected createQueue response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        String queueId = r.readString();
        throw mapErrorCode(errorCode, "createQueue: " + queueId);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("createQueue failed", e);
    } catch (IOException e) {
      throw new FilaException("createQueue failed", e);
    }
  }

  /**
   * Delete a queue on the server.
   *
   * @param queue queue name
   */
  public void deleteQueue(String queue) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeDeleteQueue(requestId, queue);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.DELETE_QUEUE_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected deleteQueue response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "deleteQueue failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("deleteQueue failed", e);
    } catch (IOException e) {
      throw new FilaException("deleteQueue failed", e);
    }
  }

  /** Set a runtime configuration key. */
  public void setConfig(String key, String value) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeSetConfig(requestId, key, value);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.SET_CONFIG_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected setConfig response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "setConfig failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("setConfig failed", e);
    } catch (IOException e) {
      throw new FilaException("setConfig failed", e);
    }
  }

  /** Get a runtime configuration value. */
  public String getConfig(String key) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeGetConfig(requestId, key);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.GET_CONFIG_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected getConfig response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "getConfig failed");
      }
      return r.readString();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("getConfig failed", e);
    } catch (IOException e) {
      throw new FilaException("getConfig failed", e);
    }
  }

  /** Redrive messages from a dead-letter queue back to its parent. */
  public long redrive(String dlqQueue, long count) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeRedrive(requestId, dlqQueue, count);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.REDRIVE_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected redrive response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "redrive failed");
      }
      return r.readU64();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("redrive failed", e);
    } catch (IOException e) {
      throw new FilaException("redrive failed", e);
    }
  }

  // --- Auth operations ---

  /**
   * Create an API key.
   *
   * @return array of [keyId, key, isSuperadmin]
   */
  public String[] createApiKey(String name, long expiresAtMs, boolean isSuperadmin) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeCreateApiKey(requestId, name, expiresAtMs, isSuperadmin);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.CREATE_API_KEY_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected createApiKey response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "createApiKey failed");
      }
      String keyId = r.readString();
      String key = r.readString();
      boolean superadmin = r.readBool();
      return new String[] {keyId, key, String.valueOf(superadmin)};
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("createApiKey failed", e);
    } catch (IOException e) {
      throw new FilaException("createApiKey failed", e);
    }
  }

  /** Revoke an API key. */
  public void revokeApiKey(String keyId) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeRevokeApiKey(requestId, keyId);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.REVOKE_API_KEY_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected revokeApiKey response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "revokeApiKey failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("revokeApiKey failed", e);
    } catch (IOException e) {
      throw new FilaException("revokeApiKey failed", e);
    }
  }

  /** Set ACL permissions for an API key. */
  public void setAcl(String keyId, String[] kinds, String[] patterns) {
    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeSetAcl(requestId, keyId, kinds, patterns);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.SET_ACL_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected setAcl response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      if (errorCode != Opcodes.ERR_OK) {
        throw mapErrorCode(errorCode, "setAcl failed");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("setAcl failed", e);
    } catch (IOException e) {
      throw new FilaException("setAcl failed", e);
    }
  }

  @Override
  public void close() {
    if (batcher != null) {
      batcher.shutdown();
    }
    connection.close();
  }

  // --- Internal helpers ---

  @SuppressWarnings("unchecked")
  private String enqueueDirect(String queue, Map<String, String> headers, byte[] payload) {
    int requestId = connection.nextRequestId();
    byte[] frame =
        Codec.encodeEnqueue(
            requestId, new String[] {queue}, new Map[] {headers}, new byte[][] {payload});

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      checkForError(response);

      if (response.header().opcode() != Opcodes.ENQUEUE_RESULT) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "unexpected response opcode");
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      long resultCount = r.readU32();
      if (resultCount < 1) {
        throw new RpcException(Opcodes.ERR_INTERNAL, "no result from server");
      }
      int errorCode = r.readU8();
      String messageId = r.readString();
      if (errorCode == Opcodes.ERR_OK) {
        return messageId;
      }
      throw mapErrorCode(errorCode, "enqueue: " + messageId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("enqueue failed", e);
    } catch (IOException e) {
      throw new FilaException("enqueue failed", e);
    }
  }

  private void retryConsumeOnLeader(
      String leaderAddr, String queue, Consumer<ConsumeMessage> handler) {
    validateLeaderAddr(leaderAddr);
    String host = Builder.parseHost(leaderAddr);
    int port = Builder.parsePort(leaderAddr);

    try {
      SSLContext sslContext = null;
      if (caCertPem != null) {
        sslContext = Builder.buildSSLContext(caCertPem, clientCertPem, clientKeyPem);
      }
      Connection leaderConn = Connection.connect(host, port, apiKey, sslContext);
      try {
        FilaClient leaderClient =
            new FilaClient(leaderConn, caCertPem, clientCertPem, clientKeyPem, apiKey, null);
        ConsumerHandle handle = leaderClient.consume(queue, handler);
        // Block until the consumer thread finishes. This method is called from
        // within the original consumer thread, so blocking here is expected.
        handle.awaitDone();
      } finally {
        leaderConn.close();
      }
    } catch (Exception e) {
      throw new FilaException("failed to connect to leader at " + leaderAddr, e);
    }
  }

  private void checkForError(Connection.Frame response) {
    if (response.header().opcode() == Opcodes.ERROR) {
      throw mapErrorFrame(response.body());
    }
  }

  static FilaException mapErrorFrame(byte[] body) {
    if (body.length == 0) {
      return new RpcException(Opcodes.ERR_INTERNAL, "empty error frame");
    }
    Primitives.Reader r = new Primitives.Reader(body);
    int errorCode = r.readU8();
    String message = r.readString();
    // Read metadata map but don't use it for now (preserved for forward compat)
    if (r.remaining() > 0) {
      r.readStringMap();
    }
    return mapErrorCode(errorCode, message);
  }

  static FilaException mapErrorCode(int errorCode, String message) {
    return switch (errorCode) {
      case Opcodes.ERR_QUEUE_NOT_FOUND -> new QueueNotFoundException("queue not found: " + message);
      case Opcodes.ERR_MESSAGE_NOT_FOUND ->
          new MessageNotFoundException("message not found: " + message);
      case Opcodes.ERR_UNAUTHORIZED -> new RpcException(errorCode, "unauthorized: " + message);
      case Opcodes.ERR_FORBIDDEN -> new RpcException(errorCode, "forbidden: " + message);
      case Opcodes.ERR_NOT_LEADER -> new RpcException(errorCode, "not leader: " + message);
      case Opcodes.ERR_QUEUE_ALREADY_EXISTS ->
          new RpcException(errorCode, "queue already exists: " + message);
      case Opcodes.ERR_CHANNEL_FULL -> new RpcException(errorCode, "channel full: " + message);
      case Opcodes.ERR_API_KEY_NOT_FOUND ->
          new RpcException(errorCode, "api key not found: " + message);
      default -> new RpcException(errorCode, message);
    };
  }

  private static String errorName(int code) {
    return switch (code) {
      case Opcodes.ERR_QUEUE_NOT_FOUND -> "queue not found";
      case Opcodes.ERR_MESSAGE_NOT_FOUND -> "message not found";
      case Opcodes.ERR_QUEUE_ALREADY_EXISTS -> "queue already exists";
      case Opcodes.ERR_UNAUTHORIZED -> "unauthorized";
      case Opcodes.ERR_FORBIDDEN -> "forbidden";
      case Opcodes.ERR_NOT_LEADER -> "not leader";
      default -> "error code 0x" + Integer.toHexString(code);
    };
  }

  private static void validateLeaderAddr(String addr) {
    if (addr == null || addr.isEmpty()) {
      throw new FilaException("invalid leader address: empty");
    }
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
     * @return this builder
     */
    public Builder withTls() {
      this.tlsEnabled = true;
      return this;
    }

    /**
     * Set the CA certificate for TLS server verification.
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

      String host = parseHost(address);
      int port = parsePort(address);

      try {
        SSLContext sslContext = null;
        if (tlsEnabled) {
          sslContext = buildSSLContext(caCertPem, clientCertPem, clientKeyPem);
        }

        Connection connection = Connection.connect(host, port, apiKey, sslContext);

        Batcher batcherInstance = null;
        if (batchMode.getKind() != BatchMode.Kind.DISABLED) {
          batcherInstance = new Batcher(connection, batchMode);
        }

        return new FilaClient(
            connection, caCertPem, clientCertPem, clientKeyPem, apiKey, batcherInstance);
      } catch (FilaException e) {
        throw e;
      } catch (Exception e) {
        throw new FilaException("failed to connect to " + address, e);
      }
    }

    static String parseHost(String address) {
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

    static SSLContext buildSSLContext(byte[] caCertPem, byte[] clientCertPem, byte[] clientKeyPem) {
      try {
        SSLContext sslContext = SSLContext.getInstance("TLS");

        TrustManagerFactory tmf = null;
        if (caCertPem != null) {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          X509Certificate caCert =
              (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(caCertPem));
          KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
          ts.load(null, null);
          ts.setCertificateEntry("ca", caCert);
          tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ts);
        }

        KeyManagerFactory kmf = null;
        if (clientCertPem != null && clientKeyPem != null) {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          X509Certificate clientCert =
              (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(clientCertPem));

          // Parse PEM private key
          String keyPemStr = new String(clientKeyPem);
          keyPemStr =
              keyPemStr
                  .replace("-----BEGIN PRIVATE KEY-----", "")
                  .replace("-----END PRIVATE KEY-----", "")
                  .replace("-----BEGIN EC PRIVATE KEY-----", "")
                  .replace("-----END EC PRIVATE KEY-----", "")
                  .replaceAll("\\s", "");
          byte[] keyDer = Base64.getDecoder().decode(keyPemStr);

          java.security.spec.PKCS8EncodedKeySpec keySpec =
              new java.security.spec.PKCS8EncodedKeySpec(keyDer);

          // Try EC first, then RSA
          java.security.PrivateKey privateKey;
          try {
            privateKey = KeyFactory.getInstance("EC").generatePrivate(keySpec);
          } catch (Exception e) {
            privateKey = KeyFactory.getInstance("RSA").generatePrivate(keySpec);
          }

          KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
          ks.load(null, null);
          ks.setKeyEntry(
              "client", privateKey, new char[0], new java.security.cert.Certificate[] {clientCert});
          kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(ks, new char[0]);
        }

        sslContext.init(
            kmf != null ? kmf.getKeyManagers() : null,
            tmf != null ? tmf.getTrustManagers() : null,
            null);
        return sslContext;
      } catch (Exception e) {
        throw new FilaException("failed to configure TLS: invalid certificate", e);
      }
    }
  }
}
