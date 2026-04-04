package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Integration tests for TLS and API key authentication.
 *
 * <p>These tests require a fila-server running with TLS and auth enabled. They are skipped if the
 * server binary is not available.
 */
@EnabledIf("serverAvailable")
class TlsAuthClientTest {
  private static TestServer server;

  @BeforeAll
  static void setUp() throws Exception {
    server = TestServer.startWithTls();
    server.createQueueWithApiKey("test-tls-auth");
  }

  @AfterAll
  static void tearDown() {
    if (server != null) server.stop();
  }

  static boolean serverAvailable() {
    return TestServer.isBinaryAvailable();
  }

  @Test
  void connectWithTlsAndApiKey() throws Exception {
    try (FilaClient client =
        FilaClient.builder(server.address())
            .withTlsCaCert(server.caCertPem())
            .withTlsClientCert(server.clientCertPem(), server.clientKeyPem())
            .withApiKey(server.apiKey())
            .build()) {
      String msgId =
          client.enqueue("test-tls-auth", Map.of("secure", "true"), "tls payload".getBytes());
      assertNotNull(msgId);
      assertFalse(msgId.isEmpty());

      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<ConsumeMessage> received = new AtomicReference<>();

      ConsumerHandle handle =
          client.consume(
              "test-tls-auth",
              msg -> {
                received.set(msg);
                client.ack("test-tls-auth", msg.getId());
                latch.countDown();
              });

      assertTrue(latch.await(10, TimeUnit.SECONDS), "Should receive message within 10s");
      handle.cancel();

      ConsumeMessage msg = received.get();
      assertNotNull(msg);
      assertEquals(msgId, msg.getId());
      assertArrayEquals("tls payload".getBytes(), msg.getPayload());
    }
  }

  @Test
  void rejectWithoutApiKey() {
    // Without an API key on an auth-enabled server, the FIBP handshake is rejected.
    // The client.build() should throw because the connection is refused during handshake.
    FilaException ex =
        assertThrows(
            FilaException.class,
            () ->
                FilaClient.builder(server.address())
                    .withTlsCaCert(server.caCertPem())
                    .withTlsClientCert(server.clientCertPem(), server.clientKeyPem())
                    .build());
    assertNotNull(ex.getMessage());
  }
}
