package dev.faisca.fila;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/**
 * Client for the Fila message broker.
 *
 * <p>Uses the FIBP (Fila Binary Protocol) transport over raw TCP or TLS.
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

  private final FibpConnection conn;
  private final Batcher batcher;

  private FilaClient(FibpConnection conn, Batcher batcher) {
    this.conn = conn;
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
   * @throws RpcException for unexpected transport failures
   */
  public String enqueue(String queue, Map<String, String> headers, byte[] payload) {
    if (batcher != null) {
      CompletableFuture<String> future =
          batcher.submit(new EnqueueMessage(queue, headers, payload));
      try {
        return future.get(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FilaException("enqueue interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof FilaException fe) {
          throw fe;
        }
        throw new RpcException(RpcException.Code.INTERNAL, cause.getMessage());
      } catch (TimeoutException e) {
        throw new RpcException(RpcException.Code.UNAVAILABLE, "enqueue timed out");
      }
    }

    return enqueueDirect(queue, headers, payload);
  }

  /**
   * Enqueue multiple messages in a single FIBP frame.
   *
   * <p>Each message is independently validated and processed. A failed message does not affect the
   * others in the batch. Returns a list of results with one entry per input message, in the same
   * order.
   *
   * <p>This bypasses the batcher and always sends a FIBP ENQUEUE frame directly. All messages must
   * target the same queue (the first message's queue name is used).
   *
   * @param messages the messages to enqueue
   * @return a list of results, one per input message
   * @throws RpcException for transport-level failures affecting the entire batch
   */
  public List<EnqueueResult> enqueueMany(List<EnqueueMessage> messages) {
    if (messages.isEmpty()) {
      return new ArrayList<>();
    }
    // FIBP enqueue frames encode a single queue name at the request level.
    // All messages in one enqueueMany call must target the same queue.
    String queue = messages.get(0).getQueue();
    for (int i = 1; i < messages.size(); i++) {
      if (!messages.get(i).getQueue().equals(queue)) {
        throw new IllegalArgumentException(
            "enqueueMany: all messages must target the same queue — got \""
                + queue
                + "\" and \""
                + messages.get(i).getQueue()
                + "\"");
      }
    }
    byte[] reqPayload = FibpCodec.encodeEnqueue(messages);
    byte[] respPayload = sendSync(conn.sendRequest(FibpConnection.OP_ENQUEUE, reqPayload));
    return FibpCodec.decodeEnqueueResponse(respPayload);
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
   * @throws RpcException for unexpected transport failures
   */
  public ConsumerHandle consume(String queue, Consumer<ConsumeMessage> handler) {
    AtomicBoolean cancelled = new AtomicBoolean(false);

    byte[] reqPayload = FibpCodec.encodeConsume(queue, 64);

    // sendConsumeRequest registers the push handler atomically before writing the frame,
    // waits for the initial server ACK (verifies queue exists), and returns the corr_id.
    int corrId;
    try {
      corrId =
          conn.sendConsumeRequest(
              reqPayload,
              queue,
              pushPayload -> {
                if (cancelled.get()) {
                  return;
                }
                try {
                  List<ConsumeMessage> msgs = FibpCodec.decodePushBatch(pushPayload, queue);
                  for (ConsumeMessage msg : msgs) {
                    if (!msg.getId().isEmpty()) {
                      handler.accept(msg);
                    }
                  }
                } catch (Exception e) {
                  // Decode error — skip this batch and continue consuming.
                }
              });
    } catch (IOException e) {
      throw new FilaException("consume: failed to send request", e);
    } catch (RuntimeException e) {
      // Unwrap FilaException from sendConsumeRequest's RuntimeException wrapper
      if (e.getCause() instanceof FilaException fe) {
        throw fe;
      }
      throw e;
    }

    int consumeCorrId = corrId;
    Thread thread =
        new Thread(
            () -> {
              // This thread keeps the consumer alive until cancelled or connection closes.
              // All message delivery happens on the FIBP reader thread via the push handler.
              while (!cancelled.get() && !conn.isClosed()) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
              }
            },
            "fila-consumer-" + queue);
    thread.setDaemon(true);
    thread.start();

    return new ConsumerHandle(
        cancelled,
        thread,
        () -> {
          conn.unregisterPushHandler(consumeCorrId);
          thread.interrupt();
        });
  }

  /**
   * Acknowledge a successfully processed message.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to acknowledge
   * @throws MessageNotFoundException if the message does not exist
   * @throws RpcException for unexpected transport failures
   */
  public void ack(String queue, String msgId) {
    byte[] reqPayload = FibpCodec.encodeAck(queue, msgId);
    byte[] respPayload = sendSync(conn.sendRequest(FibpConnection.OP_ACK, reqPayload));
    FibpCodec.decodeAckNackResponse(respPayload, true);
  }

  /**
   * Negatively acknowledge a message that failed processing.
   *
   * @param queue queue the message belongs to
   * @param msgId ID of the message to nack
   * @param error description of the failure
   * @throws MessageNotFoundException if the message does not exist
   * @throws RpcException for unexpected transport failures
   */
  public void nack(String queue, String msgId, String error) {
    byte[] reqPayload = FibpCodec.encodeNack(queue, msgId, error);
    byte[] respPayload = sendSync(conn.sendRequest(FibpConnection.OP_NACK, reqPayload));
    FibpCodec.decodeAckNackResponse(respPayload, false);
  }

  /**
   * Shut down the client, draining any pending batched messages before disconnecting.
   *
   * <p>If a batcher is running, pending messages are flushed before the FIBP connection is closed.
   */
  @Override
  public void close() {
    if (batcher != null) {
      batcher.shutdown();
    }
    conn.close();
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  private String enqueueDirect(String queue, Map<String, String> headers, byte[] payload) {
    List<EnqueueMessage> messages = List.of(new EnqueueMessage(queue, headers, payload));
    byte[] reqPayload = FibpCodec.encodeEnqueue(messages);
    byte[] respPayload = sendSync(conn.sendRequest(FibpConnection.OP_ENQUEUE, reqPayload));
    List<EnqueueResult> results = FibpCodec.decodeEnqueueResponse(respPayload);
    if (results.isEmpty()) {
      throw new RpcException(RpcException.Code.INTERNAL, "no result from server");
    }
    EnqueueResult first = results.get(0);
    if (first.isSuccess()) {
      return first.getMessageId();
    }
    throw new QueueNotFoundException(first.getError());
  }

  /** Block on a response future with a 30s timeout, unwrapping exceptions. */
  private static byte[] sendSync(CompletableFuture<byte[]> future) {
    try {
      return future.get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FilaException("request interrupted", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof FilaException fe) {
        throw fe;
      }
      throw new RpcException(RpcException.Code.INTERNAL, cause.getMessage());
    } catch (TimeoutException e) {
      throw new RpcException(RpcException.Code.UNAVAILABLE, "request timed out");
    }
  }

  // ── Builder ───────────────────────────────────────────────────────────────

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
     * <p>When set, the key is sent in an AUTH frame during the FIBP handshake.
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

      String host = parseHost(address);
      int port = parsePort(address);

      FibpConnection conn;
      try {
        if (tlsEnabled) {
          SSLSocket sslSocket = buildSslSocket(host, port);
          conn = FibpConnection.connectTls(sslSocket, apiKey);
        } else {
          conn = FibpConnection.connect(host, port, apiKey);
        }
      } catch (FilaException e) {
        throw e;
      } catch (IOException e) {
        throw new FilaException("failed to connect to " + address, e);
      } catch (Exception e) {
        throw new FilaException("failed to configure connection", e);
      }

      Batcher batcherInstance = null;
      if (batchMode.getKind() != BatchMode.Kind.DISABLED) {
        batcherInstance = new Batcher(conn, batchMode);
      }

      return new FilaClient(conn, batcherInstance);
    }

    private SSLSocket buildSslSocket(String host, int port) throws Exception {
      SSLContext sslContext;

      if (caCertPem != null) {
        // Custom CA certificate
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate caCert;
        try {
          caCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(caCertPem));
        } catch (Exception e) {
          throw new FilaException("failed to configure TLS: invalid certificate", e);
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("fila-ca", caCert);

        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        sslContext = SSLContext.getInstance("TLS");

        if (clientCertPem != null && clientKeyPem != null) {
          // mTLS: load client cert + key via PKCS12 round-trip
          javax.net.ssl.KeyManagerFactory kmf = buildKeyManagerFactory(clientCertPem, clientKeyPem);
          sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } else {
          sslContext.init(null, tmf.getTrustManagers(), null);
        }
      } else {
        // System trust store
        sslContext = SSLContext.getInstance("TLS");
        if (clientCertPem != null && clientKeyPem != null) {
          javax.net.ssl.KeyManagerFactory kmf = buildKeyManagerFactory(clientCertPem, clientKeyPem);
          sslContext.init(kmf.getKeyManagers(), null, null);
        } else {
          sslContext.init(null, null, null);
        }
      }

      SSLSocket sock = (SSLSocket) sslContext.getSocketFactory().createSocket(host, port);
      sock.setUseClientMode(true);
      return sock;
    }

    /**
     * Build a {@link javax.net.ssl.KeyManagerFactory} from PEM-encoded certificate and PKCS#8 key
     * bytes by constructing an in-memory PKCS#12 keystore.
     */
    private static javax.net.ssl.KeyManagerFactory buildKeyManagerFactory(
        byte[] certPem, byte[] keyPem) throws Exception {
      // Parse the certificate
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      java.security.cert.Certificate cert =
          cf.generateCertificate(new ByteArrayInputStream(certPem));

      // Parse the private key — strip PEM headers and decode Base64
      String keyStr = new String(keyPem, java.nio.charset.StandardCharsets.UTF_8);
      String keyBase64 =
          keyStr
              .replaceAll("-----BEGIN.*?-----", "")
              .replaceAll("-----END.*?-----", "")
              .replaceAll("\\s", "");
      byte[] keyBytes = java.util.Base64.getDecoder().decode(keyBase64);

      java.security.PrivateKey privateKey;
      // Try EC first, then RSA
      java.security.KeyFactory kf;
      try {
        kf = java.security.KeyFactory.getInstance("EC");
        privateKey = kf.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));
      } catch (Exception e) {
        kf = java.security.KeyFactory.getInstance("RSA");
        privateKey = kf.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));
      }

      // Build an in-memory PKCS12 keystore
      KeyStore ks = KeyStore.getInstance("PKCS12");
      ks.load(null, null);
      char[] emptyPassword = new char[0];
      ks.setKeyEntry(
          "client", privateKey, emptyPassword, new java.security.cert.Certificate[] {cert});

      javax.net.ssl.KeyManagerFactory kmf =
          javax.net.ssl.KeyManagerFactory.getInstance(
              javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, emptyPassword);
      return kmf;
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
