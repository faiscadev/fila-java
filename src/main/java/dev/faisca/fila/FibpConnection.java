package dev.faisca.fila;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SSLSocket;

/**
 * Low-level FIBP (Fila Binary Protocol) connection over a single TCP socket.
 *
 * <p>Handles framing, handshake, correlation-ID multiplexing, heartbeats, and AUTH. A dedicated
 * reader thread dispatches incoming frames to pending {@link CompletableFuture}s or registered push
 * handlers.
 *
 * <p>Thread-safety: all public methods are safe to call from multiple threads.
 */
final class FibpConnection implements AutoCloseable {

  // ── Op codes ──────────────────────────────────────────────────────────────
  static final byte OP_ENQUEUE = 0x01;
  static final byte OP_CONSUME = 0x02;
  static final byte OP_ACK = 0x03;
  static final byte OP_NACK = 0x04;

  static final byte OP_CREATE_QUEUE = 0x10;

  static final byte OP_FLOW = 0x20;
  static final byte OP_HEARTBEAT = 0x21;
  static final byte OP_AUTH = 0x30;
  static final byte OP_ERROR = (byte) 0xFE;
  static final byte OP_GOAWAY = (byte) 0xFF;

  // Flag bit: stream push (bit 2) — set by server on consume push frames
  static final byte FLAG_STREAM = 0x04;

  // ── Frame layout ──────────────────────────────────────────────────────────
  // [4-byte length][flags:u8 | op:u8 | corr_id:u32 | payload]
  // The 4-byte length covers flags + op + corr_id + payload.
  private static final int FRAME_HEADER_BYTES = 6; // flags(1) + op(1) + corr_id(4)

  // ── Handshake ─────────────────────────────────────────────────────────────
  private static final byte[] HANDSHAKE_MAGIC = {'F', 'I', 'B', 'P', 0x01, 0x00};

  // ── Heartbeat ─────────────────────────────────────────────────────────────
  private static final long HEARTBEAT_INTERVAL_MS = 30_000;

  private final Socket socket;
  private final DataInputStream in;
  private final DataOutputStream out;

  // Pending request futures: corr_id → future carrying raw payload bytes
  private final ConcurrentHashMap<Integer, CompletableFuture<byte[]>> pending =
      new ConcurrentHashMap<>();

  // Push handlers: corr_id → handler receiving raw push payload bytes
  private final ConcurrentHashMap<Integer, PushHandler> pushHandlers = new ConcurrentHashMap<>();

  private final AtomicInteger corrIdGen = new AtomicInteger(1);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Thread readerThread;
  private final ScheduledExecutorService heartbeatScheduler;

  // Written lock protects concurrent write calls from interleaving frames.
  private final Object writeLock = new Object();

  /** A push handler with the queue name it was registered for. */
  static final class PushHandler {
    final String queue;
    final Consumer<byte[]> handler;

    PushHandler(String queue, Consumer<byte[]> handler) {
      this.queue = queue;
      this.handler = handler;
    }
  }

  private FibpConnection(Socket socket, DataInputStream in, DataOutputStream out) {
    this.socket = socket;
    this.in = in;
    this.out = out;

    this.readerThread =
        new Thread(this::readerLoop, "fibp-reader-" + socket.getRemoteSocketAddress());
    this.readerThread.setDaemon(true);

    this.heartbeatScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "fibp-heartbeat");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Establish a plaintext FIBP connection to {@code host:port}, perform the handshake, and
   * optionally authenticate with an API key.
   */
  static FibpConnection connect(String host, int port, String apiKey) throws IOException {
    Socket sock = new Socket(host, port);
    return initConnection(sock, apiKey);
  }

  /**
   * Establish a TLS FIBP connection, perform the handshake, and optionally authenticate with an API
   * key.
   */
  static FibpConnection connectTls(SSLSocket sock, String apiKey) throws IOException {
    sock.startHandshake();
    return initConnection(sock, apiKey);
  }

  private static FibpConnection initConnection(Socket sock, String apiKey) throws IOException {
    sock.setTcpNoDelay(true);
    DataInputStream in = new DataInputStream(sock.getInputStream());
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());

    // FIBP handshake: send magic, expect echo
    out.write(HANDSHAKE_MAGIC);
    out.flush();
    byte[] echo = new byte[HANDSHAKE_MAGIC.length];
    in.readFully(echo);
    if (!Arrays.equals(echo, HANDSHAKE_MAGIC)) {
      sock.close();
      throw new IOException("FIBP handshake failed: unexpected echo " + Arrays.toString(echo));
    }

    FibpConnection conn = new FibpConnection(sock, in, out);
    conn.readerThread.start();
    conn.heartbeatScheduler.scheduleAtFixedRate(
        conn::sendHeartbeat, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

    if (apiKey != null && !apiKey.isEmpty()) {
      conn.authenticate(apiKey);
    }

    return conn;
  }

  // ── Public send API ───────────────────────────────────────────────────────

  /**
   * Send a request frame and return a future that completes with the raw response payload bytes.
   *
   * @param op op code
   * @param payload encoded request payload
   */
  CompletableFuture<byte[]> sendRequest(byte op, byte[] payload) {
    if (closed.get()) {
      CompletableFuture<byte[]> f = new CompletableFuture<>();
      f.completeExceptionally(new FilaException("connection is closed"));
      return f;
    }

    int corrId = corrIdGen.getAndIncrement();
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    pending.put(corrId, future);

    try {
      writeFrame((byte) 0, op, corrId, payload);
    } catch (IOException e) {
      pending.remove(corrId);
      future.completeExceptionally(new FilaException("write failed", e));
    }

    return future;
  }

  /**
   * Register a push handler for server-push frames on {@code corrId}. The handler is called from
   * the reader thread whenever a push frame arrives with a matching corr_id.
   */
  void registerPushHandler(int corrId, String queue, Consumer<byte[]> handler) {
    pushHandlers.put(corrId, new PushHandler(queue, handler));
  }

  /** Remove a push handler. */
  void unregisterPushHandler(int corrId) {
    pushHandlers.remove(corrId);
  }

  /**
   * Send the initial CONSUME frame, register a pending future for the initial ACK response, and
   * return the corr_id used for subsequent push frames.
   *
   * <p>Callers must register a push handler for this corr_id before calling this method to avoid
   * missing early push frames.
   */
  int sendConsumeRequest(byte[] payload, String queue, Consumer<byte[]> pushHandler)
      throws IOException {
    if (closed.get()) {
      throw new FilaException("connection is closed");
    }
    int corrId = corrIdGen.getAndIncrement();
    // Register the push handler before writing the frame to avoid a race with incoming pushes.
    pushHandlers.put(corrId, new PushHandler(queue, pushHandler));
    // Also register a pending future so we get the initial "consume started" response.
    CompletableFuture<byte[]> ackFuture = new CompletableFuture<>();
    pending.put(corrId, ackFuture);
    try {
      writeFrame((byte) 0, OP_CONSUME, corrId, payload);
    } catch (IOException e) {
      pending.remove(corrId);
      pushHandlers.remove(corrId);
      throw e;
    }
    // Wait for the server's initial ACK (queue exists response).
    try {
      ackFuture.get(10, TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException e) {
      pushHandlers.remove(corrId);
      Throwable cause = e.getCause();
      if (cause instanceof FilaException fe) {
        throw new RuntimeException(fe);
      }
      throw new FilaException("consume setup failed", cause);
    } catch (Exception e) {
      pushHandlers.remove(corrId);
      throw new FilaException("consume setup failed", e);
    }
    return corrId;
  }

  // ── Auth ──────────────────────────────────────────────────────────────────

  private void authenticate(String apiKey) {
    // AUTH payload: raw API key bytes (no length prefix)
    byte[] keyBytes = apiKey.getBytes(StandardCharsets.UTF_8);
    CompletableFuture<byte[]> f = sendRequest(OP_AUTH, keyBytes);
    try {
      f.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new FilaException("FIBP AUTH failed: " + e.getMessage(), e);
    }
  }

  // ── Heartbeat ─────────────────────────────────────────────────────────────

  private void sendHeartbeat() {
    if (closed.get()) {
      return;
    }
    try {
      writeFrame((byte) 0, OP_HEARTBEAT, 0, new byte[0]);
    } catch (IOException ignored) {
      // Reader thread will detect the broken socket and close.
    }
  }

  // ── Frame I/O ─────────────────────────────────────────────────────────────

  private void writeFrame(byte flags, byte op, int corrId, byte[] payload) throws IOException {
    int bodyLen = FRAME_HEADER_BYTES + payload.length;
    synchronized (writeLock) {
      out.writeInt(bodyLen);
      out.writeByte(flags);
      out.writeByte(op);
      out.writeInt(corrId);
      out.write(payload);
      out.flush();
    }
  }

  // ── Reader loop ───────────────────────────────────────────────────────────

  private void readerLoop() {
    try {
      while (!closed.get()) {
        int bodyLen;
        try {
          bodyLen = in.readInt();
        } catch (IOException e) {
          if (!closed.get()) {
            failAll(new FilaException("connection lost: " + e.getMessage(), e));
          }
          return;
        }

        if (bodyLen < FRAME_HEADER_BYTES) {
          failAll(new FilaException("malformed frame: bodyLen=" + bodyLen));
          return;
        }

        byte flags = in.readByte();
        byte op = in.readByte();
        int corrId = in.readInt();
        int payloadLen = bodyLen - FRAME_HEADER_BYTES;
        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) {
          in.readFully(payload);
        }

        dispatch(flags, op, corrId, payload);
      }
    } catch (IOException e) {
      if (!closed.get()) {
        failAll(new FilaException("reader error: " + e.getMessage(), e));
      }
    }
  }

  private void dispatch(byte flags, byte op, int corrId, byte[] payload) {
    if (op == OP_GOAWAY) {
      failAll(new FilaException("server sent GOAWAY"));
      return;
    }

    if (op == OP_HEARTBEAT) {
      return;
    }

    // Stream push frame: route to registered push handler
    if ((flags & FLAG_STREAM) != 0) {
      PushHandler ph = pushHandlers.get(corrId);
      if (ph != null) {
        ph.handler.accept(payload);
      }
      return;
    }

    // Response frame: complete the pending future
    CompletableFuture<byte[]> future = pending.remove(corrId);
    if (future == null) {
      return;
    }

    if (op == OP_ERROR) {
      future.completeExceptionally(FibpCodec.decodeError(payload));
    } else {
      future.complete(payload);
    }
  }

  private void failAll(FilaException cause) {
    closed.set(true);
    for (CompletableFuture<byte[]> f : pending.values()) {
      f.completeExceptionally(cause);
    }
    pending.clear();
    closeQuietly();
  }

  // ── AutoCloseable ─────────────────────────────────────────────────────────

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      heartbeatScheduler.shutdownNow();
      failAll(new FilaException("connection closed"));
    }
  }

  private void closeQuietly() {
    try {
      socket.close();
    } catch (IOException ignored) {
    }
  }

  boolean isClosed() {
    return closed.get();
  }
}
