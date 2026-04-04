package dev.faisca.fila;

import dev.faisca.fila.fibp.Codec;
import dev.faisca.fila.fibp.Connection;
import dev.faisca.fila.fibp.Opcodes;
import dev.faisca.fila.fibp.Primitives;
import java.io.IOException;
import java.util.ArrayList;
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
 * Background batcher that coalesces individual enqueue calls into multi-message RPCs.
 *
 * <p>Supports two modes: AUTO (opportunistic, Nagle-style) and LINGER (timer-based). The batcher
 * runs on a dedicated daemon thread and flushes RPCs on an executor pool.
 */
final class Batcher {
  private final LinkedBlockingQueue<BatchItem> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Connection connection;
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

  Batcher(Connection connection, BatchMode mode) {
    this.connection = connection;
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

  CompletableFuture<String> submit(EnqueueMessage message) {
    CompletableFuture<String> future = new CompletableFuture<>();
    if (!running.get()) {
      future.completeExceptionally(new FilaException("batcher is shut down"));
      return future;
    }
    queue.add(new BatchItem(message, future));
    return future;
  }

  void shutdown() {
    running.set(false);
    batcherThread.interrupt();
    try {
      batcherThread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

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

  @SuppressWarnings("unchecked")
  private void flushBatch(List<BatchItem> items) {
    if (items.isEmpty()) {
      return;
    }

    int count = items.size();
    String[] queues = new String[count];
    Map<String, String>[] headers = new Map[count];
    byte[][] payloads = new byte[count][];

    for (int i = 0; i < count; i++) {
      EnqueueMessage msg = items.get(i).message;
      queues[i] = msg.getQueue();
      headers[i] = msg.getHeaders();
      payloads[i] = msg.getPayload();
    }

    int requestId = connection.nextRequestId();
    byte[] frame = Codec.encodeEnqueue(requestId, queues, headers, payloads);

    try {
      Connection.Frame response = connection.sendAndReceive(frame, requestId, 30_000);
      byte opcode = response.header().opcode();

      if (opcode == Opcodes.ERROR) {
        FilaException ex = FilaClient.mapErrorFrame(response.body());
        for (BatchItem item : items) {
          item.future.completeExceptionally(ex);
        }
        return;
      }

      if (opcode != Opcodes.ENQUEUE_RESULT) {
        FilaException ex = new RpcException(Opcodes.ERR_INTERNAL, "unexpected response opcode");
        for (BatchItem item : items) {
          item.future.completeExceptionally(ex);
        }
        return;
      }

      Primitives.Reader r = new Primitives.Reader(response.body());
      long resultCount = r.readU32();

      for (int i = 0; i < items.size(); i++) {
        BatchItem item = items.get(i);
        if (i < resultCount) {
          int errorCode = r.readU8();
          String messageId = r.readString();
          if (errorCode == Opcodes.ERR_OK) {
            item.future.complete(messageId);
          } else {
            item.future.completeExceptionally(FilaClient.mapErrorCode(errorCode, messageId));
          }
        } else {
          item.future.completeExceptionally(
              new RpcException(Opcodes.ERR_INTERNAL, "server returned fewer results than sent"));
        }
      }
    } catch (IOException | InterruptedException e) {
      FilaException ex = new FilaException("batch enqueue failed", e);
      for (BatchItem item : items) {
        item.future.completeExceptionally(ex);
      }
    }
  }

  private static Thread newDaemon(Runnable r, String name) {
    Thread t = new Thread(r, name);
    t.setDaemon(true);
    return t;
  }
}
