package dev.faisca.fila;

/**
 * The result of a single message within an enqueue call.
 *
 * <p>Each message in a multi-message enqueue is independently validated and processed. A failed
 * message does not affect the others. Use {@link #isSuccess()} to check the outcome, then either
 * {@link #getMessageId()} or {@link #getError()}.
 */
public final class EnqueueResult {
  private final String messageId;
  private final String error;

  private EnqueueResult(String messageId, String error) {
    this.messageId = messageId;
    this.error = error;
  }

  /** Create a successful result with the broker-assigned message ID. */
  static EnqueueResult success(String messageId) {
    return new EnqueueResult(messageId, null);
  }

  /** Create a failed result with an error description. */
  static EnqueueResult error(String error) {
    return new EnqueueResult(null, error);
  }

  /** Returns true if the message was successfully enqueued. */
  public boolean isSuccess() {
    return messageId != null;
  }

  /**
   * Returns the broker-assigned message ID.
   *
   * @throws IllegalStateException if this result is an error
   */
  public String getMessageId() {
    if (messageId == null) {
      throw new IllegalStateException("result is an error: " + error);
    }
    return messageId;
  }

  /**
   * Returns the error description.
   *
   * @throws IllegalStateException if this result is a success
   */
  public String getError() {
    if (error == null) {
      throw new IllegalStateException("result is a success");
    }
    return error;
  }
}
