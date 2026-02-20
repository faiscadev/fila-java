package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FilaClientTest {
  private static TestServer server;
  private static FilaClient client;

  @BeforeAll
  static void setUp() throws Exception {
    server = TestServer.start();
    client = FilaClient.builder(server.address()).build();
    server.createQueue("test-lifecycle");
    server.createQueue("test-nack-redeliver");
    server.createQueue("test-nonexistent-error");
  }

  @AfterAll
  static void tearDown() {
    if (client != null) client.close();
    if (server != null) server.stop();
  }

  @Test
  void enqueueConsumeAck() throws Exception {
    String msgId =
        client.enqueue("test-lifecycle", Map.of("tenant", "acme"), "hello from java".getBytes());
    assertNotNull(msgId);
    assertFalse(msgId.isEmpty());

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<ConsumeMessage> received = new AtomicReference<>();

    ConsumerHandle handle =
        client.consume(
            "test-lifecycle",
            msg -> {
              received.set(msg);
              client.ack("test-lifecycle", msg.getId());
              latch.countDown();
            });

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Should receive message within 10s");
    handle.cancel();

    ConsumeMessage msg = received.get();
    assertNotNull(msg);
    assertEquals(msgId, msg.getId());
    assertEquals("acme", msg.getHeaders().get("tenant"));
    assertArrayEquals("hello from java".getBytes(), msg.getPayload());
    assertEquals("test-lifecycle", msg.getQueue());
    assertEquals(0, msg.getAttemptCount());
  }

  @Test
  void nackRedeliversSameStream() throws Exception {
    client.enqueue("test-nack-redeliver", Map.of(), "nack-me".getBytes());

    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger deliveryCount = new AtomicInteger(0);
    AtomicReference<ConsumeMessage> redelivered = new AtomicReference<>();

    ConsumerHandle handle =
        client.consume(
            "test-nack-redeliver",
            msg -> {
              int count = deliveryCount.incrementAndGet();
              if (count == 1) {
                assertEquals(0, msg.getAttemptCount());
                client.nack("test-nack-redeliver", msg.getId(), "transient error");
              } else {
                redelivered.set(msg);
                client.ack("test-nack-redeliver", msg.getId());
                latch.countDown();
              }
            });

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Should receive redelivered message within 10s");
    handle.cancel();

    assertTrue(deliveryCount.get() >= 2, "Message should be delivered at least twice");
    ConsumeMessage msg = redelivered.get();
    assertNotNull(msg);
    assertEquals(1, msg.getAttemptCount());
  }

  @Test
  void enqueueNonexistentQueueThrows() {
    QueueNotFoundException ex =
        assertThrows(
            QueueNotFoundException.class,
            () -> client.enqueue("no-such-queue", Map.of(), "data".getBytes()));
    assertNotNull(ex.getMessage());
  }
}
