package dev.faisca.fila;

import dev.faisca.fila.fibp.Codec;
import dev.faisca.fila.fibp.Connection;
import dev.faisca.fila.fibp.Opcodes;
import dev.faisca.fila.fibp.Primitives;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

/** Manages a fila-server subprocess for integration tests. */
final class TestServer {
  private final Process process;
  private final Path dataDir;
  private final String address;
  private final Connection adminConn;
  private final boolean tlsEnabled;
  private final byte[] caCertPem;
  private final byte[] clientCertPem;
  private final byte[] clientKeyPem;
  private final String apiKey;

  private TestServer(
      Process process,
      Path dataDir,
      String address,
      Connection adminConn,
      boolean tlsEnabled,
      byte[] caCertPem,
      byte[] clientCertPem,
      byte[] clientKeyPem,
      String apiKey) {
    this.process = process;
    this.dataDir = dataDir;
    this.address = address;
    this.adminConn = adminConn;
    this.tlsEnabled = tlsEnabled;
    this.caCertPem = caCertPem;
    this.clientCertPem = clientCertPem;
    this.clientKeyPem = clientKeyPem;
    this.apiKey = apiKey;
  }

  String address() {
    return address;
  }

  boolean isTlsEnabled() {
    return tlsEnabled;
  }

  byte[] caCertPem() {
    return caCertPem;
  }

  byte[] clientCertPem() {
    return clientCertPem;
  }

  byte[] clientKeyPem() {
    return clientKeyPem;
  }

  String apiKey() {
    return apiKey;
  }

  /** Creates a queue on the test server via FIBP. */
  void createQueue(String name) {
    int requestId = adminConn.nextRequestId();
    byte[] frame = Codec.encodeCreateQueue(requestId, name, null, null, 0);
    try {
      Connection.Frame response = adminConn.sendAndReceive(frame, requestId, 10_000);
      if (response.header().opcode() == Opcodes.ERROR) {
        Primitives.Reader r = new Primitives.Reader(response.body());
        int code = r.readU8();
        String msg = r.readString();
        throw new RuntimeException("createQueue failed: code=" + code + " msg=" + msg);
      }
      if (response.header().opcode() == Opcodes.CREATE_QUEUE_RESULT) {
        Primitives.Reader r = new Primitives.Reader(response.body());
        int code = r.readU8();
        if (code != Opcodes.ERR_OK && code != Opcodes.ERR_QUEUE_ALREADY_EXISTS) {
          throw new RuntimeException("createQueue failed: code=" + code);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("createQueue failed", e);
    } catch (IOException e) {
      throw new RuntimeException("createQueue failed", e);
    }
  }

  /** Creates a queue using an authenticated admin connection (TLS + API key mode). */
  void createQueueWithApiKey(String name) {
    createQueue(name);
  }

  void stop() {
    adminConn.close();
    process.destroyForcibly();
    try {
      process.waitFor(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    deleteDirectory(dataDir);
  }

  static boolean isBinaryAvailable() {
    try {
      String path = findBinary();
      return path != null && Files.isExecutable(Path.of(path));
    } catch (Exception e) {
      return false;
    }
  }

  /** Starts a fila-server on a random port (plaintext, no auth). */
  static TestServer start() throws IOException, InterruptedException {
    int port = findFreePort();
    String address = "127.0.0.1:" + port;

    Path dataDir = Files.createTempDirectory("fila-test-");
    Path configFile = dataDir.resolve("fila.toml");
    Files.writeString(configFile, "[server]\nlisten_addr = \"" + address + "\"\n");

    String binaryPath = findBinary();
    ProcessBuilder pb = new ProcessBuilder(binaryPath).redirectErrorStream(true);
    pb.directory(dataDir.toFile());
    pb.environment().put("FILA_DATA_DIR", dataDir.resolve("db").toString());
    Process process = pb.start();

    Connection adminConn = waitForHandshake("127.0.0.1", port, null, null, 10_000);
    if (adminConn == null) {
      process.destroyForcibly();
      deleteDirectory(dataDir);
      throw new IOException("fila-server failed to start within 10s on " + address);
    }
    return new TestServer(process, dataDir, address, adminConn, false, null, null, null, null);
  }

  /** Starts a fila-server with TLS and API key auth on a random port. */
  static TestServer startWithTls() throws IOException, InterruptedException {
    int port = findFreePort();
    String address = "127.0.0.1:" + port;

    Path dataDir = Files.createTempDirectory("fila-test-tls-");

    generateCerts(dataDir);

    byte[] caCert = Files.readAllBytes(dataDir.resolve("ca.pem"));
    byte[] clientCert = Files.readAllBytes(dataDir.resolve("client.pem"));
    byte[] clientKey = Files.readAllBytes(dataDir.resolve("client-key.pem"));

    String bootstrapKey = "test-bootstrap-key-" + System.currentTimeMillis();

    Path configFile = dataDir.resolve("fila.toml");
    String config =
        "[server]\n"
            + "listen_addr = \""
            + address
            + "\"\n"
            + "\n"
            + "[tls]\n"
            + "cert_file = \""
            + dataDir.resolve("server.pem")
            + "\"\n"
            + "key_file = \""
            + dataDir.resolve("server-key.pem")
            + "\"\n"
            + "ca_file = \""
            + dataDir.resolve("ca.pem")
            + "\"\n"
            + "\n"
            + "[auth]\n"
            + "bootstrap_apikey = \""
            + bootstrapKey
            + "\"\n";
    Files.writeString(configFile, config);

    String binaryPath = findBinary();
    ProcessBuilder pb = new ProcessBuilder(binaryPath).redirectErrorStream(true);
    pb.directory(dataDir.toFile());
    pb.environment().put("FILA_DATA_DIR", dataDir.resolve("db").toString());
    Process process = pb.start();

    SSLContext sslContext = FilaClient.Builder.buildSSLContext(caCert, clientCert, clientKey);
    Connection adminConn = waitForHandshake("127.0.0.1", port, bootstrapKey, sslContext, 10_000);
    if (adminConn == null) {
      process.destroyForcibly();
      deleteDirectory(dataDir);
      throw new IOException("fila-server failed to start within 10s on " + address);
    }

    return new TestServer(
        process, dataDir, address, adminConn, true, caCert, clientCert, clientKey, bootstrapKey);
  }

  private static void generateCerts(Path dir) throws IOException, InterruptedException {
    exec(
        dir,
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-keyout",
        "ca-key.pem",
        "-out",
        "ca.pem",
        "-days",
        "1",
        "-nodes",
        "-subj",
        "/CN=fila-test-ca");

    exec(
        dir,
        "openssl",
        "req",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-keyout",
        "server-key.pem",
        "-out",
        "server.csr",
        "-nodes",
        "-subj",
        "/CN=127.0.0.1");

    Files.writeString(
        dir.resolve("server-ext.cnf"), "subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n");

    exec(
        dir,
        "openssl",
        "x509",
        "-req",
        "-in",
        "server.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca-key.pem",
        "-CAcreateserial",
        "-out",
        "server.pem",
        "-days",
        "1",
        "-extfile",
        "server-ext.cnf");

    exec(
        dir,
        "openssl",
        "req",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-keyout",
        "client-key-ec.pem",
        "-out",
        "client.csr",
        "-nodes",
        "-subj",
        "/CN=fila-test-client");

    // Convert EC key to PKCS#8 format for Java compatibility
    exec(
        dir,
        "openssl",
        "pkcs8",
        "-topk8",
        "-nocrypt",
        "-in",
        "client-key-ec.pem",
        "-out",
        "client-key.pem");

    exec(
        dir,
        "openssl",
        "x509",
        "-req",
        "-in",
        "client.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca-key.pem",
        "-CAcreateserial",
        "-out",
        "client.pem",
        "-days",
        "1");
  }

  private static void exec(Path workDir, String... cmd) throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(cmd).directory(workDir.toFile()).redirectErrorStream(true);
    Process p = pb.start();
    byte[] output = p.getInputStream().readAllBytes();
    int exitCode = p.waitFor();
    if (exitCode != 0) {
      throw new IOException(
          "Command failed: "
              + String.join(" ", cmd)
              + "\nExit code: "
              + exitCode
              + "\nOutput: "
              + new String(output));
    }
  }

  private static String findBinary() {
    Path devPath =
        Path.of(System.getProperty("user.dir")).resolve("../fila/target/release/fila-server");
    if (Files.isExecutable(devPath)) {
      return devPath.toAbsolutePath().normalize().toString();
    }
    Path debugPath =
        Path.of(System.getProperty("user.dir")).resolve("../fila/target/debug/fila-server");
    if (Files.isExecutable(debugPath)) {
      return debugPath.toAbsolutePath().normalize().toString();
    }
    return "fila-server";
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * Wait for the server to accept a FIBP handshake, retrying up to timeoutMs.
   *
   * @return a connected Connection, or null if timed out
   */
  private static Connection waitForHandshake(
      String host, int port, String apiKey, SSLContext sslContext, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      try {
        return Connection.connect(host, port, apiKey, sslContext);
      } catch (IOException e) {
        Thread.sleep(200);
      }
    }
    return null;
  }

  private static void deleteDirectory(Path dir) {
    try (var walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException ignored) {
                  // best effort cleanup
                }
              });
    } catch (IOException ignored) {
      // best effort cleanup
    }
  }
}
