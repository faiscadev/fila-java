package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for FilaClient.Builder configuration. */
class BuilderTest {

  @Test
  void builderPlaintextDoesNotThrow() {
    // Plaintext builder should create a client without error
    FilaClient client = FilaClient.builder("localhost:5555").build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithApiKeyDoesNotThrow() {
    // API key without TLS should work (for backward compat / dev mode)
    FilaClient client = FilaClient.builder("localhost:5555").withApiKey("test-key").build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithInvalidCaCertThrows() {
    // Invalid PEM bytes should throw FilaException
    assertThrows(
        FilaException.class,
        () ->
            FilaClient.builder("localhost:5555")
                .withTlsCaCert("not-a-valid-cert".getBytes())
                .build());
  }

  @Test
  void builderChainingReturnsBuilder() {
    // Verify fluent API returns the builder for chaining
    FilaClient.Builder builder =
        FilaClient.builder("localhost:5555")
            .withApiKey("key")
            .withTlsCaCert("cert".getBytes())
            .withTlsClientCert("cert".getBytes(), "key".getBytes());
    assertNotNull(builder);
  }
}
