package dev.faisca.fila;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/** Manages a fila-server subprocess for integration tests. */
final class TestServer {
  private final Process process;
  private final Path dataDir;
  private final String address;
  private final boolean tlsEnabled;
  private final byte[] caCertPem;
  private final byte[] clientCertPem;
  private final byte[] clientKeyPem;
  private final String apiKey;

  private TestServer(
      Process process,
      Path dataDir,
      String address,
      boolean tlsEnabled,
      byte[] caCertPem,
      byte[] clientCertPem,
      byte[] clientKeyPem,
      String apiKey) {
    this.process = process;
    this.dataDir = dataDir;
    this.address = address;
    this.tlsEnabled = tlsEnabled;
    this.caCertPem = caCertPem;
    this.clientCertPem = clientCertPem;
    this.clientKeyPem = clientKeyPem;
    this.apiKey = apiKey;
  }

  /** Returns the address of the running server. */
  String address() {
    return address;
  }

  /** Returns true if TLS is enabled on this server. */
  boolean isTlsEnabled() {
    return tlsEnabled;
  }

  /** Returns the CA certificate PEM bytes. Only valid when TLS is enabled. */
  byte[] caCertPem() {
    return caCertPem;
  }

  /** Returns the client certificate PEM bytes. Only valid when TLS is enabled. */
  byte[] clientCertPem() {
    return clientCertPem;
  }

  /** Returns the client private key PEM bytes. Only valid when TLS is enabled. */
  byte[] clientKeyPem() {
    return clientKeyPem;
  }

  /** Returns the bootstrap API key. Only valid when auth is enabled. */
  String apiKey() {
    return apiKey;
  }

  /**
   * Creates a queue on the test server using the fila CLI binary.
   *
   * <p>Falls back to using the FIBP admin RPC directly if the CLI is not available.
   */
  void createQueue(String name) {
    createQueueImpl(name, null);
  }

  /** Creates a queue using an authenticated admin connection (TLS + API key mode). */
  void createQueueWithApiKey(String name) {
    createQueueImpl(name, apiKey);
  }

  private void createQueueImpl(String name, String key) {
    // Use the FIBP admin protocol to create queues.
    String host = address.split(":")[0];
    int port = Integer.parseInt(address.split(":")[1]);
    try (FibpAdminClient admin = FibpAdminClient.connect(host, port, key)) {
      admin.createQueue(name);
    } catch (IOException e) {
      throw new RuntimeException("failed to create queue '" + name + "'", e);
    }
  }

  /** Stops the server and cleans up temporary files. */
  void stop() {
    process.destroyForcibly();
    try {
      process.waitFor(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    deleteDirectory(dataDir);
  }

  /**
   * Returns true if the fila-server binary is available at a known local path.
   *
   * <p>Note: This intentionally does NOT check PATH. The TLS integration tests require a local dev
   * build to ensure cert generation and server config are compatible. In CI, the plaintext
   * integration tests run via {@link FilaClientTest} using the downloaded binary; the TLS tests are
   * skipped until the CI pipeline is configured to provision TLS test infrastructure.
   */
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

    if (!waitForPort(port, 10_000)) {
      process.destroyForcibly();
      deleteDirectory(dataDir);
      throw new IOException("fila-server failed to start within 10s on " + address);
    }

    return new TestServer(process, dataDir, address, false, null, null, null, null);
  }

  /** Starts a fila-server with TLS and API key auth on a random port. */
  static TestServer startWithTls() throws IOException, InterruptedException {
    int port = findFreePort();
    String address = "127.0.0.1:" + port;

    Path dataDir = Files.createTempDirectory("fila-test-tls-");

    // Generate self-signed CA, server cert, and client cert using openssl
    generateCerts(dataDir);

    byte[] caCert = Files.readAllBytes(dataDir.resolve("ca.pem"));
    byte[] clientCert = Files.readAllBytes(dataDir.resolve("client.pem"));
    byte[] clientKey = Files.readAllBytes(dataDir.resolve("client-key.pem"));

    // Bootstrap API key for auth
    String bootstrapKey = "test-bootstrap-key-" + System.currentTimeMillis();

    Path configFile = dataDir.resolve("fila.toml");
    String config =
        "[server]\n"
            + "listen_addr = \""
            + address
            + "\"\n"
            + "\n"
            + "[tls]\n"
            + "ca_cert = \""
            + dataDir.resolve("ca.pem")
            + "\"\n"
            + "server_cert = \""
            + dataDir.resolve("server.pem")
            + "\"\n"
            + "server_key = \""
            + dataDir.resolve("server-key.pem")
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

    if (!waitForPort(port, 10_000)) {
      process.destroyForcibly();
      deleteDirectory(dataDir);
      throw new IOException("fila-server failed to start within 10s on " + address);
    }

    return new TestServer(
        process, dataDir, address, true, caCert, clientCert, clientKey, bootstrapKey);
  }

  private static void generateCerts(Path dir) throws IOException, InterruptedException {
    // Generate CA key and cert
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

    // Generate server key and CSR
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

    // Write SAN extension file
    Files.writeString(
        dir.resolve("server-ext.cnf"), "subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n");

    // Sign server cert with CA
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

    // Generate client key and CSR
    exec(
        dir,
        "openssl",
        "req",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-keyout",
        "client-key.pem",
        "-out",
        "client.csr",
        "-nodes",
        "-subj",
        "/CN=fila-test-client");

    // Sign client cert with CA
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

  static String findBinary() {
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

  private static boolean waitForPort(int port, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      try (var sock = new java.net.Socket("127.0.0.1", port)) {
        return true;
      } catch (IOException e) {
        Thread.sleep(100);
      }
    }
    return false;
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
