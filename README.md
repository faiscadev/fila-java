# fila-client (Java)

Java client SDK for the [Fila](https://github.com/faiscadev/fila) message broker.

## Installation

### Gradle

```groovy
implementation 'dev.faisca:fila-client:0.1.0'
```

### Maven

```xml
<dependency>
    <groupId>dev.faisca</groupId>
    <artifactId>fila-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

```java
import dev.faisca.fila.*;
import java.util.Map;

try (FilaClient client = FilaClient.builder("localhost:5555").build()) {
    // Enqueue a message
    String msgId = client.enqueue("my-queue", Map.of("tenant", "acme"), "hello".getBytes());
    System.out.println("Enqueued: " + msgId);

    // Consume messages
    ConsumerHandle handle = client.consume("my-queue", msg -> {
        System.out.println("Received: " + new String(msg.getPayload()));
        client.ack("my-queue", msg.getId());
    });

    // ... do other work ...

    // Stop consuming
    handle.cancel();
}
```

## TLS

Connect over TLS by providing a CA certificate. The CA cert is used to verify the server's identity:

```java
byte[] caCert = Files.readAllBytes(Path.of("ca.pem"));

FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .build();
```

For mutual TLS (mTLS), also provide the client certificate and key:

```java
byte[] caCert = Files.readAllBytes(Path.of("ca.pem"));
byte[] clientCert = Files.readAllBytes(Path.of("client.pem"));
byte[] clientKey = Files.readAllBytes(Path.of("client-key.pem"));

FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .withTlsClientCert(clientCert, clientKey)
    .build();
```

## API Key Authentication

When the server has auth enabled, provide an API key:

```java
FilaClient client = FilaClient.builder("localhost:5555")
    .withApiKey("your-api-key")
    .build();
```

The key is sent as a `Bearer` token in the `authorization` metadata header on every RPC.

TLS and API key auth can be combined:

```java
FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .withTlsClientCert(clientCert, clientKey)
    .withApiKey("your-api-key")
    .build();
```

## API Reference

### `FilaClient`

Create a client with the builder:

```java
FilaClient client = FilaClient.builder("localhost:5555").build();
```

`FilaClient` implements `AutoCloseable` for use with try-with-resources.

#### Builder Methods

| Method | Description |
|--------|-------------|
| `withTlsCaCert(byte[] caCertPem)` | CA certificate for TLS server verification |
| `withTlsClientCert(byte[] certPem, byte[] keyPem)` | Client cert + key for mTLS |
| `withApiKey(String apiKey)` | API key sent as `Bearer` token on every RPC |

All builder methods are optional. When none are set, the client connects over plaintext without authentication (backward compatible).

#### `enqueue(String queue, Map<String, String> headers, byte[] payload) -> String`

Enqueue a message. Returns the broker-assigned message ID (UUIDv7).

Throws `QueueNotFoundException` if the queue does not exist.

#### `consume(String queue, Consumer<ConsumeMessage> handler) -> ConsumerHandle`

Start consuming messages from a queue. Messages are delivered to the handler on a background thread. Nacked messages are redelivered on the same stream.

Call `handle.cancel()` to stop consuming.

Throws `QueueNotFoundException` if the queue does not exist.

#### `ack(String queue, String msgId)`

Acknowledge a successfully processed message.

Throws `MessageNotFoundException` if the message does not exist.

#### `nack(String queue, String msgId, String error)`

Negatively acknowledge a message. The message is requeued based on the queue's configuration.

Throws `MessageNotFoundException` if the message does not exist.

### `ConsumeMessage`

| Method             | Type                  | Description                          |
|--------------------|-----------------------|--------------------------------------|
| `getId()`          | `String`              | Broker-assigned message ID (UUIDv7)  |
| `getHeaders()`     | `Map<String, String>` | Message headers                      |
| `getPayload()`     | `byte[]`              | Message payload                      |
| `getFairnessKey()` | `String`              | Fairness key assigned by the broker  |
| `getAttemptCount()` | `int`                | Number of delivery attempts          |
| `getQueue()`       | `String`              | Queue this message belongs to        |

### Error Handling

All exceptions extend `FilaException` (unchecked):

- `QueueNotFoundException` — queue does not exist
- `MessageNotFoundException` — message does not exist (or already acked)
- `RpcException` — unexpected gRPC failure (includes status code via `getCode()`)

```java
try {
    client.enqueue("missing-queue", Map.of(), "data".getBytes());
} catch (QueueNotFoundException e) {
    System.err.println("Queue not found: " + e.getMessage());
}
```

## License

AGPLv3 — see [LICENSE](LICENSE).
