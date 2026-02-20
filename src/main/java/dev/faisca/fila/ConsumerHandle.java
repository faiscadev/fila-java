package dev.faisca.fila;

import io.grpc.Context;

/** Handle for a running consume stream. Call {@link #cancel()} to stop consuming. */
public final class ConsumerHandle {
  private final Context.CancellableContext context;
  private final Thread thread;

  ConsumerHandle(Context.CancellableContext context, Thread thread) {
    this.context = context;
    this.thread = thread;
  }

  /** Cancel the consume stream and wait for the consumer thread to finish. */
  public void cancel() {
    context.cancel(null);
    try {
      thread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
