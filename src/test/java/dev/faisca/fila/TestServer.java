package dev.faisca.fila;

import fila.v1.Admin;
import fila.v1.FilaAdminGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
  private final ManagedChannel adminChannel;
  private final FilaAdminGrpc.FilaAdminBlockingStub adminStub;

  private TestServer(Process process, Path dataDir, String address, ManagedChannel adminChannel) {
    this.process = process;
    this.dataDir = dataDir;
    this.address = address;
    this.adminChannel = adminChannel;
    this.adminStub = FilaAdminGrpc.newBlockingStub(adminChannel);
  }

  /** Returns the address of the running server. */
  String address() {
    return address;
  }

  /** Creates a queue on the test server. */
  void createQueue(String name) {
    adminStub.createQueue(Admin.CreateQueueRequest.newBuilder().setName(name).build());
  }

  /** Stops the server and cleans up temporary files. */
  void stop() {
    adminChannel.shutdown();
    try {
      adminChannel.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    process.destroyForcibly();
    try {
      process.waitFor(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    deleteDirectory(dataDir);
  }

  /** Starts a fila-server on a random port. */
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

    ManagedChannel adminChannel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    return new TestServer(process, dataDir, address, adminChannel);
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
