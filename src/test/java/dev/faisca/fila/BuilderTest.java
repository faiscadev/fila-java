package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for FilaClient.Builder configuration. */
class BuilderTest {

  @Test
  void builderPlaintextDoesNotThrow() {
    // Plaintext builder should create a client without error (default AUTO batching)
    FilaClient client = FilaClient.builder("localhost:5555").build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithBatchDisabledDoesNotThrow() {
    // Plaintext builder with batching disabled
    FilaClient client =
        FilaClient.builder("localhost:5555").withBatchMode(BatchMode.disabled()).build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithBatchAutoDoesNotThrow() {
    // Explicit AUTO batch mode
    FilaClient client =
        FilaClient.builder("localhost:5555").withBatchMode(BatchMode.auto(50)).build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithBatchLingerDoesNotThrow() {
    // LINGER batch mode
    FilaClient client =
        FilaClient.builder("localhost:5555").withBatchMode(BatchMode.linger(10, 50)).build();
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
            .withBatchMode(BatchMode.auto())
            .withTlsCaCert("cert".getBytes())
            .withTlsClientCert("cert".getBytes(), "key".getBytes());
    assertNotNull(builder);
  }

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
  void builderWithTlsSystemTrustDoesNotThrow() {
    // withTls() using system trust store should create a client without error
    FilaClient client = FilaClient.builder("localhost:5555").withTls().build();
    assertNotNull(client);
    client.close();
  }

  @Test
  void builderWithTlsAndApiKeyDoesNotThrow() {
    // withTls() combined with API key should work
    FilaClient client =
        FilaClient.builder("localhost:5555").withTls().withApiKey("test-key").build();
    assertNotNull(client);
    client.close();
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
}
