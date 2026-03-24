package dev.faisca.fila;

import java.util.Map;

/**
 * A message to be enqueued via {@link FilaClient#batchEnqueue(java.util.List)}.
 *
 * <p>Each message specifies its target queue, headers, and payload independently, allowing a single
 * batch to target multiple queues.
 */
public final class EnqueueMessage {
  private final String queue;
  private final Map<String, String> headers;
  private final byte[] payload;

  /**
   * Create a new enqueue message.
   *
   * @param queue target queue name
   * @param headers message headers (may be empty)
   * @param payload message payload bytes
   */
  public EnqueueMessage(String queue, Map<String, String> headers, byte[] payload) {
    this.queue = queue;
    this.headers = Map.copyOf(headers);
    this.payload = payload.clone();
  }

  /** Returns the target queue name. */
  public String getQueue() {
    return queue;
  }

  /** Returns the message headers. */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /** Returns the message payload bytes. */
  public byte[] getPayload() {
    return payload.clone();
  }
}
