package dev.faisca.fila;

import java.util.concurrent.atomic.AtomicBoolean;

/** Handle for a running consume stream. Call {@link #cancel()} to stop consuming. */
public final class ConsumerHandle {
  private final AtomicBoolean cancelled;
  private final Thread thread;
  private final Runnable onCancel;

  ConsumerHandle(AtomicBoolean cancelled, Thread thread, Runnable onCancel) {
    this.cancelled = cancelled;
    this.thread = thread;
    this.onCancel = onCancel;
  }

  /** Cancel the consume stream and wait for the consumer thread to finish. */
  public void cancel() {
    cancelled.set(true);
    onCancel.run();
    try {
      thread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
