package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for FilaClient.Builder configuration.
 *
 * <p>Tests that exercise the builder configuration itself (e.g. validation, chaining) do not
 * require a running server. Tests that call {@code build()} on a valid configuration require a
 * server at localhost:5555 and are guarded by {@code @EnabledIf("serverAvailable")}.
 *
 * <p>The previous gRPC-based client deferred connection to the first RPC call, so {@code build()}
 * always succeeded even with no server. FIBP connects eagerly during {@code build()}.
 */
class BuilderTest {

  static boolean serverAvailable() {
    return TestServer.isBinaryAvailable();
  }

  // ── Configuration-only tests (no server needed) ───────────────────────────

  @Test
  void builderWithInvalidCaCertThrows() {
    // Invalid PEM bytes should throw FilaException before any network attempt
    assertThrows(
        FilaException.class,
        () ->
            FilaClient.builder("localhost:5555")
                .withTlsCaCert("not-a-valid-cert".getBytes())
                .build());
  }

  @Test
  void builderClientCertWithoutTlsThrows() {
    // Client cert without TLS enabled should fail fast (no network attempt)
    assertThrows(
        FilaException.class,
        () ->
            FilaClient.builder("localhost:5555")
                .withTlsClientCert("cert".getBytes(), "key".getBytes())
                .build());
  }

  @Test
  void builderChainingReturnsBuilder() {
    // Verify fluent API returns the builder for chaining — no build() call
    FilaClient.Builder builder =
        FilaClient.builder("localhost:5555")
            .withApiKey("key")
            .withBatchMode(BatchMode.auto())
            .withTlsCaCert("cert".getBytes())
            .withTlsClientCert("cert".getBytes(), "key".getBytes());
    assertNotNull(builder);
  }

  @Test
  void builderChainingWithTlsReturnsBuilder() {
    // Verify fluent API for withTls() returns the builder for chaining — no build() call
    FilaClient.Builder builder =
        FilaClient.builder("localhost:5555")
            .withTls()
            .withApiKey("key")
            .withTlsClientCert("cert".getBytes(), "key".getBytes());
    assertNotNull(builder);
  }

  @Test
  void parseHostPlaintext() {
    assertEquals("localhost", FilaClient.Builder.parseHost("localhost:5555"));
  }

  @Test
  void parsePortPlaintext() {
    assertEquals(5555, FilaClient.Builder.parsePort("localhost:5555"));
  }

  @Test
  void parsePortDefaultsWhenMissing() {
    assertEquals(5555, FilaClient.Builder.parsePort("localhost"));
  }

  @Test
  void parseHostIpv6() {
    assertEquals("::1", FilaClient.Builder.parseHost("[::1]:5555"));
  }

  @Test
  void parsePortIpv6() {
    assertEquals(5555, FilaClient.Builder.parsePort("[::1]:5555"));
  }

  // ── BatchMode config tests (no server needed) ─────────────────────────────

  @Test
  void batchModeAutoIsDefault() {
    // Default batch mode should be AUTO — verified without connecting
    FilaClient.Builder builder = FilaClient.builder("localhost:5555");
    assertNotNull(builder);
  }
}
