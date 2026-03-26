# fila-client (Java)

Java client SDK for the [Fila](https://github.com/faiscadev/fila) message broker.

Uses the **FIBP** (Fila Binary Protocol) — a length-prefixed binary protocol over raw TCP or TLS.
No gRPC or protobuf dependencies.

## Installation

### Gradle

```groovy
implementation 'dev.faisca:fila-client:0.3.0'
```

### Maven

```xml
<dependency>
    <groupId>dev.faisca</groupId>
    <artifactId>fila-client</artifactId>
    <version>0.3.0</version>
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

### System trust store (public CAs)

If the Fila server uses a certificate issued by a public CA (e.g., Let's Encrypt), enable TLS with the JVM's default trust store:

```java
try (FilaClient client = FilaClient.builder("localhost:5555")
    .withTls()
    .build()) {
    // use client...
}
```

### Custom CA certificate

For servers using self-signed or private CA certificates, provide the CA cert explicitly:

```java
byte[] caCert = Files.readAllBytes(Path.of("ca.pem"));

try (FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .build()) {
    // use client...
}
```

### Mutual TLS (mTLS)

For mutual TLS, also provide the client certificate and key. This works with both trust modes:

```java
byte[] caCert = Files.readAllBytes(Path.of("ca.pem"));
byte[] clientCert = Files.readAllBytes(Path.of("client.pem"));
byte[] clientKey = Files.readAllBytes(Path.of("client-key.pem"));

try (FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .withTlsClientCert(clientCert, clientKey)
    .build()) {
    // use client...
}
```

## API Key Authentication

When the server has auth enabled, provide an API key:

```java
try (FilaClient client = FilaClient.builder("localhost:5555")
    .withApiKey("your-api-key")
    .build()) {
    // use client...
}
```

The key is sent in an AUTH frame during the FIBP handshake before any other requests.

TLS and API key auth can be combined:

```java
try (FilaClient client = FilaClient.builder("localhost:5555")
    .withTlsCaCert(caCert)
    .withTlsClientCert(clientCert, clientKey)
    .withApiKey("your-api-key")
    .build()) {
    // use client...
}
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
| `withTls()` | Enable TLS using JVM's default trust store (cacerts) |
| `withTlsCaCert(byte[] caCertPem)` | CA certificate for TLS server verification (implies `withTls()`) |
| `withTlsClientCert(byte[] certPem, byte[] keyPem)` | Client cert + key for mTLS |
| `withApiKey(String apiKey)` | API key sent in AUTH frame during FIBP handshake |
| `withBatchMode(BatchMode batchMode)` | Configure enqueue batching (default: `BatchMode.auto()`) |

All builder methods are optional. When none are set, the client connects over plaintext without authentication (backward compatible).

#### `enqueue(String queue, Map<String, String> headers, byte[] payload) -> String`

Enqueue a message. Returns the broker-assigned message ID (UUIDv7).

Throws `QueueNotFoundException` if the queue does not exist.

#### `enqueueMany(List<EnqueueMessage> messages) -> List<EnqueueResult>`

Enqueue multiple messages in a single FIBP frame. Each message is independently processed. All messages must target the same queue.

#### `consume(String queue, Consumer<ConsumeMessage> handler) -> ConsumerHandle`

Start consuming messages from a queue. Messages are delivered to the handler on the FIBP reader thread. Nacked messages are redelivered on the same stream.

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
- `RpcException` — transport-level failure (includes status code via `getCode()`)

`RpcException.Code` values: `INTERNAL`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `UNAVAILABLE`, `CANCELLED`, `UNKNOWN`.

```java
try {
    client.enqueue("missing-queue", Map.of(), "data".getBytes());
} catch (QueueNotFoundException e) {
    System.err.println("Queue not found: " + e.getMessage());
}
```

## Transport: FIBP

This SDK uses FIBP (Fila Binary Protocol) — a lightweight binary framing protocol over TCP. Each
frame is:

```
[4-byte big-endian length][flags:u8 | op:u8 | corr_id:u32 | payload]
```

All requests are multiplexed over a single TCP connection using correlation IDs. A background
reader thread dispatches responses. Authentication uses an AUTH op frame sent during connection
setup. Heartbeat frames keep the connection alive.

## License

AGPLv3 — see [LICENSE](LICENSE).
