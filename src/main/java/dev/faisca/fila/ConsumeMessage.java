package dev.faisca.fila;

import java.util.Map;

/** A message received from a Fila consume stream. */
public final class ConsumeMessage {
  private final String id;
  private final Map<String, String> headers;
  private final byte[] payload;
  private final String fairnessKey;
  private final int attemptCount;
  private final String queue;

  ConsumeMessage(
      String id,
      Map<String, String> headers,
      byte[] payload,
      String fairnessKey,
      int attemptCount,
      String queue) {
    this.id = id;
    this.headers = Map.copyOf(headers);
    this.payload = payload.clone();
    this.fairnessKey = fairnessKey;
    this.attemptCount = attemptCount;
    this.queue = queue;
  }

  /** Returns the broker-assigned message ID (UUIDv7). */
  public String getId() {
    return id;
  }

  /** Returns the message headers. */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /** Returns the message payload bytes. */
  public byte[] getPayload() {
    return payload.clone();
  }

  /** Returns the fairness key assigned by the broker. */
  public String getFairnessKey() {
    return fairnessKey;
  }

  /** Returns the number of delivery attempts for this message. */
  public int getAttemptCount() {
    return attemptCount;
  }

  /** Returns the queue this message belongs to. */
  public String getQueue() {
    return queue;
  }
}
