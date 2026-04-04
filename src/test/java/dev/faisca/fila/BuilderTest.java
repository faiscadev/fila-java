package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for FilaClient.Builder configuration validation. */
class BuilderTest {

  @Test
  void builderClientCertWithoutTlsThrows() {
    // Client cert without TLS enabled should fail fast
    assertThrows(
        FilaException.class,
        () ->
            FilaClient.builder("localhost:5555")
                .withTlsClientCert("cert".getBytes(), "key".getBytes())
                .build());
  }

  @Test
  void builderChainingReturnsBuilder() {
    // Verify fluent API returns the builder for chaining
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
    // Verify fluent API for withTls() returns the builder for chaining
    FilaClient.Builder builder =
        FilaClient.builder("localhost:5555")
            .withTls()
            .withApiKey("key")
            .withTlsClientCert("cert".getBytes(), "key".getBytes());
    assertNotNull(builder);
  }

  @Test
  void builderWithInvalidCaCertThrows() {
    // Invalid PEM bytes should throw FilaException during TLS setup
    assertThrows(
        FilaException.class,
        () ->
            FilaClient.builder("localhost:5555")
                .withTlsCaCert("not-a-valid-cert".getBytes())
                .build());
  }
}
