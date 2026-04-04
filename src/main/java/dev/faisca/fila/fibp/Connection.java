package dev.faisca.fila.fibp;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Manages a TCP connection to a Fila server using the FIBP binary protocol.
 *
 * <p>Handles handshake, request/response multiplexing, server-push delivery routing, and keepalive.
 */
public final class Connection implements AutoCloseable {

  /** A received frame (header + body after header). */
  public record Frame(FrameHeader header, byte[] body) {}

  private final Socket socket;
  private final DataInputStream input;
  private final OutputStream output;
  private final Object writeLock = new Object();

  private final AtomicInteger nextRequestId = new AtomicInteger(1);
  private final ConcurrentHashMap<Integer, LinkedBlockingQueue<Frame>> waiters =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, LinkedBlockingQueue<Frame>> deliveryChannels =
      new ConcurrentHashMap<>();

  // Continuation frame reassembly buffers, keyed by request ID.
  private final ConcurrentHashMap<Integer, ByteArrayOutputStream> continuationBuffers =
      new ConcurrentHashMap<>();
  // Track the opcode for each continuation sequence.
  private final ConcurrentHashMap<Integer, Byte> continuationOpcodes = new ConcurrentHashMap<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Thread readLoop;

  private long nodeId;
  private int maxFrameSize;

  private Connection(Socket socket) throws IOException {
    this.socket = socket;
    this.input = new DataInputStream(socket.getInputStream());
    this.output = socket.getOutputStream();
    this.readLoop = new Thread(this::readLoopRun, "fibp-read-loop");
    this.readLoop.setDaemon(true);
  }

  /**
   * Connect to a Fila server and perform the FIBP handshake.
   *
   * @param host server host
   * @param port server port
   * @param apiKey optional API key (null if auth disabled)
   * @param sslContext optional SSLContext for TLS (null for plaintext)
   * @return a connected Connection
   */
  public static Connection connect(String host, int port, String apiKey, SSLContext sslContext)
      throws IOException {
    Socket socket;
    if (sslContext != null) {
      SSLSocketFactory factory = sslContext.getSocketFactory();
      SSLSocket sslSocket = (SSLSocket) factory.createSocket(host, port);
      sslSocket.startHandshake();
      socket = sslSocket;
    } else {
      socket = new Socket(host, port);
    }
    socket.setTcpNoDelay(true);

    Connection conn = new Connection(socket);
    conn.performHandshake(apiKey);
    conn.readLoop.start();
    return conn;
  }

  /** Returns the next request ID. */
  public int nextRequestId() {
    return nextRequestId.getAndIncrement();
  }

  /** Returns the server's node ID from the handshake. */
  public long getNodeId() {
    return nodeId;
  }

  /** Returns the server's max frame size from the handshake. */
  public int getMaxFrameSize() {
    return maxFrameSize;
  }

  /** Send raw bytes (a complete encoded frame) to the server. */
  public void send(byte[] frameBytes) throws IOException {
    synchronized (writeLock) {
      output.write(frameBytes);
      output.flush();
    }
  }

  /**
   * Send a request and wait for the response.
   *
   * @param frameBytes the complete encoded frame
   * @param requestId the request ID used in the frame
   * @param timeoutMs timeout in milliseconds
   * @return the response frame
   */
  public Frame sendAndReceive(byte[] frameBytes, int requestId, long timeoutMs)
      throws IOException, InterruptedException {
    LinkedBlockingQueue<Frame> queue = new LinkedBlockingQueue<>(1);
    waiters.put(requestId, queue);
    try {
      send(frameBytes);
      Frame frame = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (frame == null) {
        throw new IOException("request timed out (requestId=" + requestId + ")");
      }
      return frame;
    } finally {
      waiters.remove(requestId);
    }
  }

  /**
   * Register a delivery channel for a consume subscription.
   *
   * @param requestId the consume request ID
   * @return a queue that will receive delivery frames
   */
  public LinkedBlockingQueue<Frame> registerDeliveryChannel(int requestId) {
    LinkedBlockingQueue<Frame> queue = new LinkedBlockingQueue<>();
    deliveryChannels.put(requestId, queue);
    return queue;
  }

  /** Unregister a delivery channel. */
  public void unregisterDeliveryChannel(int requestId) {
    deliveryChannels.remove(requestId);
  }

  /** Returns true if the connection is closed. */
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // Try to send disconnect
      try {
        send(Codec.encodeDisconnect(0));
      } catch (IOException ignored) {
        // best effort
      }

      try {
        socket.close();
      } catch (IOException ignored) {
        // best effort
      }

      readLoop.interrupt();

      // Fail all pending waiters
      for (Map.Entry<Integer, LinkedBlockingQueue<Frame>> entry : waiters.entrySet()) {
        // Signal by offering a sentinel (or the waiter will timeout)
        entry
            .getValue()
            .offer(
                new Frame(new FrameHeader(Opcodes.ERROR, (byte) 0, entry.getKey()), new byte[0]));
      }
    }
  }

  private void performHandshake(String apiKey) throws IOException {
    byte[] handshakeFrame = Codec.encodeHandshake(0, Opcodes.PROTOCOL_VERSION, apiKey);
    send(handshakeFrame);

    // Read the response synchronously (before read loop starts)
    Frame response = readFrame();
    if (response.header().opcode() == Opcodes.HANDSHAKE_OK) {
      Primitives.Reader r = new Primitives.Reader(response.body());
      int negotiatedVersion = r.readU16();
      this.nodeId = r.readU64();
      int maxFrame = r.readU32AsInt();
      this.maxFrameSize = maxFrame == 0 ? Opcodes.DEFAULT_MAX_FRAME_SIZE : maxFrame;
    } else if (response.header().opcode() == Opcodes.ERROR) {
      Primitives.Reader r = new Primitives.Reader(response.body());
      int errorCode = r.readU8();
      String message = r.readString();
      throw new IOException(
          "handshake rejected: code=0x" + Integer.toHexString(errorCode) + " message=" + message);
    } else {
      throw new IOException(
          "unexpected handshake response opcode: 0x"
              + Integer.toHexString(response.header().opcode() & 0xFF));
    }
  }

  private Frame readFrame() throws IOException {
    // Read 4-byte length prefix
    byte[] lenBytes = new byte[4];
    input.readFully(lenBytes);
    int bodyLen = ByteBuffer.wrap(lenBytes).order(ByteOrder.BIG_ENDIAN).getInt();

    // Read body
    byte[] body = new byte[bodyLen];
    input.readFully(body);

    // Parse header (first 6 bytes of body)
    byte opcode = body[0];
    byte flags = body[1];
    int requestId = ByteBuffer.wrap(body, 2, 4).order(ByteOrder.BIG_ENDIAN).getInt();

    // Payload is everything after the 6-byte header
    byte[] payload = new byte[bodyLen - FrameHeader.SIZE];
    System.arraycopy(body, FrameHeader.SIZE, payload, 0, payload.length);

    FrameHeader header = new FrameHeader(opcode, flags, requestId);

    // Handle continuation frames
    if (header.isContinuation()) {
      ByteArrayOutputStream buf =
          continuationBuffers.computeIfAbsent(requestId, k -> new ByteArrayOutputStream());
      continuationOpcodes.putIfAbsent(requestId, opcode);
      buf.write(payload);
      // Return null to signal the caller to read another frame
      return null;
    } else {
      // Check if we have buffered continuation data
      ByteArrayOutputStream contBuf = continuationBuffers.remove(requestId);
      Byte contOpcode = continuationOpcodes.remove(requestId);
      if (contBuf != null) {
        contBuf.write(payload);
        byte resolvedOpcode = contOpcode != null ? contOpcode : opcode;
        return new Frame(
            new FrameHeader(resolvedOpcode, (byte) 0, requestId), contBuf.toByteArray());
      }
      return new Frame(header, payload);
    }
  }

  private void readLoopRun() {
    try {
      while (!closed.get() && !Thread.currentThread().isInterrupted()) {
        Frame frame = readFrame();
        if (frame == null) {
          // Continuation frame buffered, read next
          continue;
        }

        byte opcode = frame.header().opcode();
        int reqId = frame.header().requestId();

        if (opcode == Opcodes.PING) {
          // Respond with Pong
          try {
            send(Codec.encodePong(reqId));
          } catch (IOException ignored) {
            // connection closing
          }
          continue;
        }

        if (opcode == Opcodes.DELIVERY) {
          // Route to delivery channel
          LinkedBlockingQueue<Frame> ch = deliveryChannels.get(reqId);
          if (ch != null) {
            ch.offer(frame);
          }
          continue;
        }

        // Route to request waiter
        LinkedBlockingQueue<Frame> waiter = waiters.get(reqId);
        if (waiter != null) {
          waiter.offer(frame);
        }
      }
    } catch (IOException e) {
      if (!closed.get()) {
        close();
      }
    }
  }
}
