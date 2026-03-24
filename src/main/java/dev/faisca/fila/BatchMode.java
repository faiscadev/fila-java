package dev.faisca.fila;

/**
 * Controls how the SDK batches {@link FilaClient#enqueue} calls.
 *
 * <p>The default is {@link #auto()} -- opportunistic batching that requires zero configuration. At
 * low load each message is sent individually (zero added latency). At high load messages accumulate
 * naturally and are flushed together.
 */
public final class BatchMode {
  enum Kind {
    AUTO,
    LINGER,
    DISABLED
  }

  private final Kind kind;
  private final int maxBatchSize;
  private final long lingerMs;

  private BatchMode(Kind kind, int maxBatchSize, long lingerMs) {
    this.kind = kind;
    this.maxBatchSize = maxBatchSize;
    this.lingerMs = lingerMs;
  }

  /**
   * Opportunistic batching (default).
   *
   * <p>A background thread blocks for the first message, then drains any additional messages that
   * arrived concurrently. At low load each message is sent individually. At high load messages
   * accumulate naturally into batches. Zero config, zero latency penalty.
   *
   * @return a new AUTO batch mode with default max batch size (100)
   */
  public static BatchMode auto() {
    return new BatchMode(Kind.AUTO, 100, 0);
  }

  /**
   * Opportunistic batching with a custom max batch size.
   *
   * @param maxBatchSize safety cap on batch size
   * @return a new AUTO batch mode
   */
  public static BatchMode auto(int maxBatchSize) {
    if (maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be >= 1");
    }
    return new BatchMode(Kind.AUTO, maxBatchSize, 0);
  }

  /**
   * Timer-based forced batching.
   *
   * <p>Buffers messages and flushes when either {@code batchSize} messages accumulate or {@code
   * lingerMs} milliseconds elapse since the first message in the batch -- whichever comes first.
   *
   * @param lingerMs time threshold in milliseconds before a partial batch is flushed
   * @param batchSize maximum messages per batch
   * @return a new LINGER batch mode
   */
  public static BatchMode linger(long lingerMs, int batchSize) {
    if (lingerMs < 1) {
      throw new IllegalArgumentException("lingerMs must be >= 1");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be >= 1");
    }
    return new BatchMode(Kind.LINGER, batchSize, lingerMs);
  }

  /**
   * No batching. Each {@link FilaClient#enqueue} call is a direct single-message RPC.
   *
   * @return a DISABLED batch mode
   */
  public static BatchMode disabled() {
    return new BatchMode(Kind.DISABLED, 0, 0);
  }

  Kind getKind() {
    return kind;
  }

  int getMaxBatchSize() {
    return maxBatchSize;
  }

  long getLingerMs() {
    return lingerMs;
  }
}
