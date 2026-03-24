package dev.faisca.fila;

import fila.v1.FilaServiceGrpc;
import fila.v1.Service;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background batcher that coalesces individual enqueue calls into batch RPCs.
 *
 * <p>Supports two modes: AUTO (opportunistic, Nagle-style) and LINGER (timer-based). The batcher
 * runs on a dedicated daemon thread and flushes RPCs on an executor pool.
 */
final class Batcher {
  private final LinkedBlockingQueue<BatchItem> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final FilaServiceGrpc.FilaServiceBlockingStub stub;
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

  Batcher(FilaServiceGrpc.FilaServiceBlockingStub stub, BatchMode mode) {
    this.stub = stub;
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
          // Block for first message.
          BatchItem item = queue.take();
          buffer.add(item);

          if (buffer.size() >= batchSize) {
            List<BatchItem> toFlush = List.copyOf(buffer);
            buffer.clear();
            flushExecutor.submit(() -> flushBatch(toFlush));
          } else {
            // Start linger timer.
            final List<BatchItem> timerBuffer = buffer;
            lingerTimer =
                scheduler.schedule(
                    () -> {
                      // Signal the batcher thread to flush by adding a poison pill.
                      // The actual flush happens in the main loop.
                      batcherThread.interrupt();
                    },
                    lingerMs,
                    TimeUnit.MILLISECONDS);
          }
        } else {
          // Buffer has items -- wait for more or timer expiry.
          BatchItem item = queue.poll(lingerMs, TimeUnit.MILLISECONDS);
          if (item != null) {
            buffer.add(item);
            // Drain any additional available items.
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
        // Timer or shutdown interrupt -- flush what we have.
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
   * Flush a batch of messages. Uses single-message Enqueue RPC for 1 message (preserves error
   * types), BatchEnqueue for 2+ messages.
   */
  private void flushBatch(List<BatchItem> items) {
    if (items.isEmpty()) {
      return;
    }

    if (items.size() == 1) {
      flushSingle(items.get(0));
      return;
    }

    flushMultiple(items);
  }

  /** Single-item optimization: use regular Enqueue RPC for exact error semantics. */
  private void flushSingle(BatchItem item) {
    Service.EnqueueRequest req =
        Service.EnqueueRequest.newBuilder()
            .setQueue(item.message.getQueue())
            .putAllHeaders(item.message.getHeaders())
            .setPayload(com.google.protobuf.ByteString.copyFrom(item.message.getPayload()))
            .build();
    try {
      Service.EnqueueResponse resp = stub.enqueue(req);
      item.future.complete(resp.getMessageId());
    } catch (StatusRuntimeException e) {
      item.future.completeExceptionally(FilaClient.mapEnqueueError(e));
    }
  }

  /** Multi-item flush: use BatchEnqueue RPC for amortized overhead. */
  private void flushMultiple(List<BatchItem> items) {
    Service.BatchEnqueueRequest.Builder reqBuilder = Service.BatchEnqueueRequest.newBuilder();
    for (BatchItem item : items) {
      reqBuilder.addMessages(
          Service.EnqueueRequest.newBuilder()
              .setQueue(item.message.getQueue())
              .putAllHeaders(item.message.getHeaders())
              .setPayload(com.google.protobuf.ByteString.copyFrom(item.message.getPayload()))
              .build());
    }

    try {
      Service.BatchEnqueueResponse resp = stub.batchEnqueue(reqBuilder.build());
      List<Service.BatchEnqueueResult> results = resp.getResultsList();

      for (int i = 0; i < items.size(); i++) {
        BatchItem item = items.get(i);
        if (i < results.size()) {
          Service.BatchEnqueueResult result = results.get(i);
          switch (result.getResultCase()) {
            case SUCCESS:
              item.future.complete(result.getSuccess().getMessageId());
              break;
            case ERROR:
              item.future.completeExceptionally(
                  new RpcException(io.grpc.Status.Code.INTERNAL, result.getError()));
              break;
            default:
              item.future.completeExceptionally(
                  new RpcException(io.grpc.Status.Code.INTERNAL, "no result from server"));
              break;
          }
        } else {
          item.future.completeExceptionally(
              new RpcException(
                  io.grpc.Status.Code.INTERNAL,
                  "server returned fewer results than messages sent"));
        }
      }
    } catch (StatusRuntimeException e) {
      FilaException mapped = FilaClient.mapBatchEnqueueError(e);
      for (BatchItem item : items) {
        item.future.completeExceptionally(mapped);
      }
    }
  }

  private static Thread newDaemon(Runnable r, String name) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    return t;
  }
}
