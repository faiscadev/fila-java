package dev.faisca.fila;

/** Handle for a running consume stream. Call {@link #cancel()} to stop consuming. */
public final class ConsumerHandle {
  private final Runnable cancelAction;
  private final Thread thread;

  ConsumerHandle(Runnable cancelAction, Thread thread) {
    this.cancelAction = cancelAction;
    this.thread = thread;
  }

  /** Cancel the consume stream and wait for the consumer thread to finish. */
  public void cancel() {
    cancelAction.run();
    try {
      thread.join(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
