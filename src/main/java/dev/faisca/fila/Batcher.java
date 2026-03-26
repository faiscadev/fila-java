package dev.faisca.fila;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background batcher that coalesces individual enqueue calls into multi-message FIBP frames.
 *
 * <p>Supports two modes: AUTO (opportunistic, Nagle-style) and LINGER (timer-based). The batcher
 * runs on a dedicated daemon thread and flushes frames on an executor pool.
 *
 * <p>FIBP enqueue frames carry one queue name per frame, so messages targeting different queues are
 * split into separate frames.
 */
final class Batcher {
  private final LinkedBlockingQueue<BatchItem> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final FibpConnection conn;
  private final BatchMode mode;
  private final Thread batcherThread;
  private final ExecutorService flushExecutor;
  private final ScheduledExecutorService scheduler;

  static final class BatchItem {
    final EnqueueMessage message;
    final CompletableFuture<String> future;

    BatchItem(EnqueueMessage message, CompletableFuture<String> future) {
      this.message = message;
      this.future = future;
    }
  }

  Batcher(FibpConnection conn, BatchMode mode) {
    this.conn = conn;
    this.mode = mode;
    this.flushExecutor = Executors.newCachedThreadPool(r -> newDaemon(r, "fila-batch-flush"));
    this.scheduler =
        mode.getKind() == BatchMode.Kind.LINGER
            ? Executors.newSingleThreadScheduledExecutor(r -> newDaemon(r, "fila-batch-scheduler"))
            : null;

    this.batcherThread =
        new Thread(
            mode.getKind() == BatchMode.Kind.AUTO ? this::runAuto : this::runLinger,
            "fila-batcher");
    this.batcherThread.setDaemon(true);
    this.batcherThread.start();
  }

  /**
   * Submit a message for batched enqueuing.
   *
   * @return a future that completes with the message ID or fails with a FilaException
   */
  CompletableFuture<String> submit(EnqueueMessage message) {
    CompletableFuture<String> future = new CompletableFuture<>();
    if (!running.get()) {
      future.completeExceptionally(new FilaException("batcher is shut down"));
      return future;
    }
    queue.add(new BatchItem(message, future));
    return future;
  }

  /** Drain pending messages and shut down. Blocks until all pending flushes complete. */
  void shutdown() {
    running.set(false);
    batcherThread.interrupt();
    try {
      batcherThread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Drain any remaining items in the queue.
    List<BatchItem> remaining = new ArrayList<>();
    queue.drainTo(remaining);
    if (!remaining.isEmpty()) {
      flushBatch(remaining);
    }

    flushExecutor.shutdown();
    try {
      if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        flushExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      flushExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /** AUTO mode: block for first message, drain any additional, flush concurrently. */
  private void runAuto() {
    int maxBatchSize = mode.getMaxBatchSize();
    while (running.get()) {
      try {
        BatchItem first = queue.take();
        List<BatchItem> batch = new ArrayList<>();
        batch.add(first);
        queue.drainTo(batch, maxBatchSize - 1);

        List<BatchItem> toFlush = List.copyOf(batch);
        flushExecutor.submit(() -> flushBatch(toFlush));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** LINGER mode: buffer messages and flush when batch is full or linger timer fires. */
  private void runLinger() {
    int batchSize = mode.getMaxBatchSize();
    long lingerMs = mode.getLingerMs();
    List<BatchItem> buffer = new ArrayList<>();
    ScheduledFuture<?> lingerTimer = null;

    while (running.get()) {
      try {
        if (buffer.isEmpty()) {
          BatchItem item = queue.take();
          buffer.add(item);

          if (buffer.size() >= batchSize) {
            List<BatchItem> toFlush = List.copyOf(buffer);
            buffer.clear();
            flushExecutor.submit(() -> flushBatch(toFlush));
          } else {
            lingerTimer =
                scheduler.schedule(
                    () -> batcherThread.interrupt(), lingerMs, TimeUnit.MILLISECONDS);
          }
        } else {
          BatchItem item = queue.poll(lingerMs, TimeUnit.MILLISECONDS);
          if (item != null) {
            buffer.add(item);
            queue.drainTo(buffer, batchSize - buffer.size());
          }

          if (buffer.size() >= batchSize || item == null) {
            if (lingerTimer != null) {
              lingerTimer.cancel(false);
              lingerTimer = null;
            }
            List<BatchItem> toFlush = List.copyOf(buffer);
            buffer.clear();
            flushExecutor.submit(() -> flushBatch(toFlush));
          }
        }
      } catch (InterruptedException e) {
        if (!buffer.isEmpty()) {
          if (lingerTimer != null) {
            lingerTimer.cancel(false);
            lingerTimer = null;
          }
          List<BatchItem> toFlush = List.copyOf(buffer);
          buffer.clear();
          flushExecutor.submit(() -> flushBatch(toFlush));
        }
        if (!running.get()) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /**
   * Flush a batch of messages via FIBP enqueue frames. Messages targeting different queues are sent
   * in separate frames (one FIBP frame per queue).
   */
  private void flushBatch(List<BatchItem> items) {
    if (items.isEmpty()) {
      return;
    }

    // Group by queue — LinkedHashMap preserves insertion order for deterministic frame ordering.
    Map<String, List<BatchItem>> byQueue = new LinkedHashMap<>();
    for (BatchItem item : items) {
      byQueue.computeIfAbsent(item.message.getQueue(), k -> new ArrayList<>()).add(item);
    }

    for (Map.Entry<String, List<BatchItem>> entry : byQueue.entrySet()) {
      flushQueueBatch(entry.getValue());
    }
  }

  private void flushQueueBatch(List<BatchItem> items) {
    List<EnqueueMessage> messages = new ArrayList<>(items.size());
    for (BatchItem item : items) {
      messages.add(item.message);
    }

    byte[] payload = FibpCodec.encodeEnqueue(messages);
    CompletableFuture<byte[]> respFuture = conn.sendRequest(FibpConnection.OP_ENQUEUE, payload);

    try {
      byte[] respPayload = respFuture.get(30, TimeUnit.SECONDS);
      List<EnqueueResult> results = FibpCodec.decodeEnqueueResponse(respPayload);

      for (int i = 0; i < items.size(); i++) {
        BatchItem item = items.get(i);
        if (i < results.size()) {
          EnqueueResult result = results.get(i);
          if (result.isSuccess()) {
            item.future.complete(result.getMessageId());
          } else {
            item.future.completeExceptionally(new QueueNotFoundException(result.getError()));
          }
        } else {
          item.future.completeExceptionally(
              new RpcException(RpcException.Code.INTERNAL, "server returned fewer results"));
        }
      }
    } catch (java.util.concurrent.ExecutionException e) {
      FilaException mapped = mapException(e.getCause());
      for (BatchItem item : items) {
        item.future.completeExceptionally(mapped);
      }
    } catch (java.util.concurrent.TimeoutException e) {
      FilaException mapped = new RpcException(RpcException.Code.UNAVAILABLE, "enqueue timed out");
      for (BatchItem item : items) {
        item.future.completeExceptionally(mapped);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      FilaException mapped = new RpcException(RpcException.Code.UNAVAILABLE, "enqueue interrupted");
      for (BatchItem item : items) {
        item.future.completeExceptionally(mapped);
      }
    } catch (RuntimeException e) {
      // Guard against unexpected failures (e.g. decodeEnqueueResponse errors) so all per-item
      // futures are resolved and callers are not left blocked indefinitely.
      FilaException mapped = mapException(e);
      for (BatchItem item : items) {
        item.future.completeExceptionally(mapped);
      }
    }
  }

  private static FilaException mapException(Throwable t) {
    if (t instanceof FilaException fe) {
      return fe;
    }
    return new RpcException(RpcException.Code.INTERNAL, t != null ? t.getMessage() : "unknown");
  }

  private static Thread newDaemon(Runnable r, String name) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    return t;
  }
}
