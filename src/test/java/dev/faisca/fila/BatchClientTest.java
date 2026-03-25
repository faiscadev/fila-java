package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Integration tests for enqueueMany and smart batching.
 *
 * <p>Requires a fila-server binary. Skipped if not available.
 */
@EnabledIf("serverAvailable")
class BatchClientTest {
  private static TestServer server;

  @BeforeAll
  static void setUp() throws Exception {
    server = TestServer.start();
    server.createQueue("test-batch-explicit");
    server.createQueue("test-batch-auto");
    server.createQueue("test-batch-linger");
    server.createQueue("test-batch-disabled");
    server.createQueue("test-batch-consume");
    server.createQueue("test-batch-mixed");
  }

  @AfterAll
  static void tearDown() {
    if (server != null) server.stop();
  }

  static boolean serverAvailable() {
    return TestServer.isBinaryAvailable();
  }

  @Test
  void explicitEnqueueMany() {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.disabled()).build()) {
      List<EnqueueMessage> messages = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        messages.add(
            new EnqueueMessage(
                "test-batch-explicit",
                Map.of("idx", String.valueOf(i)),
                ("batch-msg-" + i).getBytes()));
      }

      List<EnqueueResult> results = client.enqueueMany(messages);
      assertEquals(5, results.size());

      Set<String> ids = new HashSet<>();
      for (EnqueueResult result : results) {
        assertTrue(result.isSuccess(), "each message should succeed");
        assertFalse(result.getMessageId().isEmpty());
        ids.add(result.getMessageId());
      }
      assertEquals(5, ids.size(), "all message IDs should be unique");
    }
  }

  @Test
  void explicitEnqueueManyWithNonexistentQueue() {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.disabled()).build()) {
      List<EnqueueMessage> messages = new ArrayList<>();
      messages.add(new EnqueueMessage("test-batch-explicit", Map.of(), "good-msg".getBytes()));
      messages.add(new EnqueueMessage("no-such-queue", Map.of(), "bad-msg".getBytes()));
      messages.add(new EnqueueMessage("test-batch-explicit", Map.of(), "another-good".getBytes()));

      List<EnqueueResult> results = client.enqueueMany(messages);
      assertEquals(3, results.size());

      // First and third should succeed, second should fail
      assertTrue(results.get(0).isSuccess());
      assertFalse(results.get(1).isSuccess());
      assertTrue(results.get(2).isSuccess());
    }
  }

  @Test
  void autoBatchingEnqueue() throws Exception {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.auto()).build()) {
      // Enqueue messages through the auto batcher
      String msgId =
          client.enqueue("test-batch-auto", Map.of("mode", "auto"), "auto-msg".getBytes());
      assertNotNull(msgId);
      assertFalse(msgId.isEmpty());

      // Verify the message can be consumed
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<ConsumeMessage> received = new AtomicReference<>();

      ConsumerHandle handle =
          client.consume(
              "test-batch-auto",
              msg -> {
                received.set(msg);
                client.ack("test-batch-auto", msg.getId());
                latch.countDown();
              });

      assertTrue(latch.await(10, TimeUnit.SECONDS), "should receive message within 10s");
      handle.cancel();

      ConsumeMessage msg = received.get();
      assertNotNull(msg);
      assertEquals(msgId, msg.getId());
      assertEquals("auto", msg.getHeaders().get("mode"));
      assertArrayEquals("auto-msg".getBytes(), msg.getPayload());
    }
  }

  @Test
  void autoBatchingMultipleMessages() throws Exception {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.auto(50)).build()) {
      // Send multiple messages quickly to exercise batching under load
      int count = 10;
      Set<String> sentIds = new HashSet<>();
      for (int i = 0; i < count; i++) {
        String msgId =
            client.enqueue(
                "test-batch-consume", Map.of("idx", String.valueOf(i)), ("msg-" + i).getBytes());
        assertNotNull(msgId);
        sentIds.add(msgId);
      }
      assertEquals(count, sentIds.size(), "all message IDs should be unique");

      // Consume all messages
      CountDownLatch latch = new CountDownLatch(count);
      Set<String> receivedIds = java.util.Collections.synchronizedSet(new HashSet<>());

      ConsumerHandle handle =
          client.consume(
              "test-batch-consume",
              msg -> {
                receivedIds.add(msg.getId());
                client.ack("test-batch-consume", msg.getId());
                latch.countDown();
              });

      assertTrue(latch.await(15, TimeUnit.SECONDS), "should receive all messages within 15s");
      handle.cancel();

      assertEquals(sentIds, receivedIds, "should receive all sent messages");
    }
  }

  @Test
  void lingerBatchingEnqueue() throws Exception {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.linger(50, 10)).build()) {
      String msgId =
          client.enqueue("test-batch-linger", Map.of("mode", "linger"), "linger-msg".getBytes());
      assertNotNull(msgId);
      assertFalse(msgId.isEmpty());

      // Verify consumption
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<ConsumeMessage> received = new AtomicReference<>();

      ConsumerHandle handle =
          client.consume(
              "test-batch-linger",
              msg -> {
                received.set(msg);
                client.ack("test-batch-linger", msg.getId());
                latch.countDown();
              });

      assertTrue(latch.await(10, TimeUnit.SECONDS), "should receive message within 10s");
      handle.cancel();

      assertEquals(msgId, received.get().getId());
    }
  }

  @Test
  void disabledBatchingEnqueue() throws Exception {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.disabled()).build()) {
      String msgId =
          client.enqueue(
              "test-batch-disabled", Map.of("mode", "disabled"), "direct-msg".getBytes());
      assertNotNull(msgId);
      assertFalse(msgId.isEmpty());

      // Verify consumption
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<ConsumeMessage> received = new AtomicReference<>();

      ConsumerHandle handle =
          client.consume(
              "test-batch-disabled",
              msg -> {
                received.set(msg);
                client.ack("test-batch-disabled", msg.getId());
                latch.countDown();
              });

      assertTrue(latch.await(10, TimeUnit.SECONDS), "should receive message within 10s");
      handle.cancel();

      assertEquals(msgId, received.get().getId());
    }
  }

  @Test
  void enqueueNonexistentQueueThroughBatcher() {
    try (FilaClient client =
        FilaClient.builder(server.address()).withBatchMode(BatchMode.auto()).build()) {
      assertThrows(
          QueueNotFoundException.class,
          () -> client.enqueue("no-such-queue-batch", Map.of(), "data".getBytes()));
    }
  }

  @Test
  void defaultBatchModeIsAuto() throws Exception {
    // Default builder should use AUTO batching
    try (FilaClient client = FilaClient.builder(server.address()).build()) {
      String msgId =
          client.enqueue("test-batch-mixed", Map.of("default", "true"), "default-batch".getBytes());
      assertNotNull(msgId);
      assertFalse(msgId.isEmpty());
    }
  }
}
