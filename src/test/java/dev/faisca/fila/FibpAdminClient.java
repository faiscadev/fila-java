package dev.faisca.fila;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/**
 * Minimal FIBP admin client for test infrastructure.
 *
 * <p>Supports CreateQueue only. Admin operation payloads are protobuf-encoded (matching the
 * server's fila-core admin dispatch). We hand-roll the minimal protobuf needed to avoid a test
 * dependency on a protobuf runtime.
 *
 * <p>Supports both plaintext and TLS/mTLS connections.
 */
final class FibpAdminClient implements AutoCloseable {

  private static final byte[] HANDSHAKE_MAGIC = {'F', 'I', 'B', 'P', 0x01, 0x00};
  private static final int FRAME_HEADER_BYTES = 6;

  private final Socket socket;
  private final DataInputStream in;
  private final DataOutputStream out;
  private int nextCorrId = 1;
  private final Object writeLock = new Object();

  private FibpAdminClient(Socket socket, DataInputStream in, DataOutputStream out) {
    this.socket = socket;
    this.in = in;
    this.out = out;
  }

  /** Connect plaintext (no TLS). */
  static FibpAdminClient connect(String host, int port, String apiKey) throws IOException {
    Socket sock = new Socket(host, port);
    return init(sock, apiKey);
  }

  /**
   * Connect with TLS using a custom CA certificate and optional client cert/key for mTLS.
   *
   * @param caCertPem PEM-encoded CA certificate (required)
   * @param clientCertPem PEM-encoded client certificate (optional, for mTLS)
   * @param clientKeyPem PEM-encoded client private key (optional, for mTLS)
   */
  static FibpAdminClient connectTls(
      String host,
      int port,
      String apiKey,
      byte[] caCertPem,
      byte[] clientCertPem,
      byte[] clientKeyPem)
      throws IOException {
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509Certificate caCert =
          (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(caCertPem));

      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null);
      trustStore.setCertificateEntry("fila-ca", caCert);

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);

      SSLContext sslContext = SSLContext.getInstance("TLS");

      if (clientCertPem != null && clientKeyPem != null) {
        KeyManagerFactory kmf = buildKeyManagerFactory(clientCertPem, clientKeyPem);
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
      } else {
        sslContext.init(null, tmf.getTrustManagers(), null);
      }

      SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(host, port);
      sslSocket.setUseClientMode(true);
      sslSocket.startHandshake();
      return init(sslSocket, apiKey);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("TLS admin connection failed", e);
    }
  }

  private static FibpAdminClient init(Socket sock, String apiKey) throws IOException {
    sock.setTcpNoDelay(true);
    DataInputStream in = new DataInputStream(sock.getInputStream());
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());

    // Handshake
    out.write(HANDSHAKE_MAGIC);
    out.flush();
    byte[] echo = new byte[HANDSHAKE_MAGIC.length];
    in.readFully(echo);
    if (!Arrays.equals(echo, HANDSHAKE_MAGIC)) {
      sock.close();
      throw new IOException("FIBP admin handshake failed");
    }

    FibpAdminClient client = new FibpAdminClient(sock, in, out);

    if (apiKey != null && !apiKey.isEmpty()) {
      client.authenticate(apiKey);
    }

    return client;
  }

  void createQueue(String name) throws IOException {
    byte[] payload = encodeCreateQueueRequest(name);
    // If we get a response without IOException, the queue was created.
    // Error frames cause sendRequest to throw.
    sendRequest(FibpConnection.OP_CREATE_QUEUE, payload);
  }

  private void authenticate(String apiKey) throws IOException {
    byte[] keyBytes = apiKey.getBytes(StandardCharsets.UTF_8);
    sendRequest(FibpConnection.OP_AUTH, keyBytes);
  }

  /**
   * Encode a CreateQueueRequest protobuf message with just the name field.
   *
   * <p>Protobuf encoding: field 1 (name, string) = tag 0x0A (field=1, wire_type=2) + varint(len) +
   * utf8 bytes.
   */
  private static byte[] encodeCreateQueueRequest(String name) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    buf.write(0x0A); // field 1, wire type 2 (length-delimited)
    writeVarint(buf, nameBytes.length);
    buf.write(nameBytes, 0, nameBytes.length);
    return buf.toByteArray();
  }

  private static void writeVarint(ByteArrayOutputStream buf, int value) {
    while ((value & ~0x7F) != 0) {
      buf.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buf.write(value);
  }

  private static KeyManagerFactory buildKeyManagerFactory(byte[] certPem, byte[] keyPem)
      throws Exception {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    java.security.cert.Certificate cert = cf.generateCertificate(new ByteArrayInputStream(certPem));

    String keyStr = new String(keyPem, StandardCharsets.UTF_8);
    String keyBase64 =
        keyStr
            .replaceAll("-----BEGIN.*?-----", "")
            .replaceAll("-----END.*?-----", "")
            .replaceAll("\\s", "");
    byte[] keyBytes = java.util.Base64.getDecoder().decode(keyBase64);

    java.security.PrivateKey privateKey;
    java.security.KeyFactory kf;
    try {
      kf = java.security.KeyFactory.getInstance("EC");
      privateKey = kf.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));
    } catch (Exception e) {
      kf = java.security.KeyFactory.getInstance("RSA");
      privateKey = kf.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));
    }

    KeyStore ks = KeyStore.getInstance("PKCS12");
    ks.load(null, null);
    char[] emptyPassword = new char[0];
    ks.setKeyEntry(
        "client", privateKey, emptyPassword, new java.security.cert.Certificate[] {cert});

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, emptyPassword);
    return kmf;
  }

  private byte[] sendRequest(byte op, byte[] payload) throws IOException {
    int corrId;
    synchronized (writeLock) {
      corrId = nextCorrId++;
      int bodyLen = FRAME_HEADER_BYTES + payload.length;
      out.writeInt(bodyLen);
      out.writeByte(0); // flags
      out.writeByte(op);
      out.writeInt(corrId);
      out.write(payload);
      out.flush();
    }

    // Read the response (simple blocking read — single-threaded admin client)
    int bodyLen = in.readInt();
    if (bodyLen < FRAME_HEADER_BYTES) {
      throw new IOException("malformed response frame: bodyLen=" + bodyLen);
    }
    in.readByte(); // flags (not used in single-threaded client)
    byte respOp = in.readByte();
    in.readInt(); // corrId (not used in single-threaded client)
    int respPayloadLen = bodyLen - FRAME_HEADER_BYTES;
    byte[] respPayload = new byte[respPayloadLen];
    if (respPayloadLen > 0) {
      in.readFully(respPayload);
    }

    if (respOp == FibpConnection.OP_ERROR) {
      String msg = new String(respPayload, StandardCharsets.UTF_8);
      throw new IOException("server error: " + msg);
    }

    return respPayload;
  }

  @Override
  public void close() {
    try {
      socket.close();
    } catch (IOException ignored) {
    }
  }
}
