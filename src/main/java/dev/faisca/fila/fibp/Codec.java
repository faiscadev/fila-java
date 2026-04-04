package dev.faisca.fila.fibp;

import java.util.Map;

/**
 * Encodes and decodes FIBP frames.
 *
 * <p>Encoding methods produce complete frame bodies (including the 6-byte header). Decoding methods
 * consume the body bytes after the header has been parsed.
 */
public final class Codec {
  private Codec() {}

  // --- Frame body encoding (header + payload) ---

  /** Encode a complete frame: [u32 length][body]. */
  public static byte[] encodeFrame(byte opcode, byte flags, int requestId, byte[] bodyPayload) {
    int bodyLen = FrameHeader.SIZE + bodyPayload.length;
    Primitives.Writer w = new Primitives.Writer(4 + bodyLen);
    w.writeU32(bodyLen);
    w.writeU8(opcode & 0xFF);
    w.writeU8(flags & 0xFF);
    w.writeU32(requestId);
    if (bodyPayload.length > 0) {
      // Write raw bytes directly
      byte[] frameBytes = w.toByteArray();
      byte[] result = new byte[frameBytes.length + bodyPayload.length];
      System.arraycopy(frameBytes, 0, result, 0, frameBytes.length);
      System.arraycopy(bodyPayload, 0, result, frameBytes.length, bodyPayload.length);
      return result;
    }
    return w.toByteArray();
  }

  /** Encode a Handshake frame (0x01). */
  public static byte[] encodeHandshake(int requestId, int version, String apiKey) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeU16(version);
    w.writeOptionalString(apiKey);
    return encodeFrame(Opcodes.HANDSHAKE, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a Ping frame (0x03). */
  public static byte[] encodePing(int requestId) {
    return encodeFrame(Opcodes.PING, (byte) 0, requestId, new byte[0]);
  }

  /** Encode a Pong frame (0x04). */
  public static byte[] encodePong(int requestId) {
    return encodeFrame(Opcodes.PONG, (byte) 0, requestId, new byte[0]);
  }

  /** Encode a Disconnect frame (0x05). */
  public static byte[] encodeDisconnect(int requestId) {
    return encodeFrame(Opcodes.DISCONNECT, (byte) 0, requestId, new byte[0]);
  }

  /** Encode an Enqueue frame (0x10) for a batch of messages. */
  public static byte[] encodeEnqueue(
      int requestId, String[] queues, Map<String, String>[] headers, byte[][] payloads) {
    Primitives.Writer w = new Primitives.Writer(256);
    w.writeU32(queues.length);
    for (int i = 0; i < queues.length; i++) {
      w.writeString(queues[i]);
      w.writeStringMap(headers[i]);
      w.writeBytes(payloads[i]);
    }
    return encodeFrame(Opcodes.ENQUEUE, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a Consume frame (0x12). */
  public static byte[] encodeConsume(int requestId, String queue) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(queue);
    return encodeFrame(Opcodes.CONSUME, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a CancelConsume frame (0x14). */
  public static byte[] encodeCancelConsume(int requestId) {
    return encodeFrame(Opcodes.CANCEL_CONSUME, (byte) 0, requestId, new byte[0]);
  }

  /** Encode an Ack frame (0x16). */
  public static byte[] encodeAck(int requestId, String[] queues, String[] messageIds) {
    Primitives.Writer w = new Primitives.Writer(128);
    w.writeU32(queues.length);
    for (int i = 0; i < queues.length; i++) {
      w.writeString(queues[i]);
      w.writeString(messageIds[i]);
    }
    return encodeFrame(Opcodes.ACK, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a Nack frame (0x18). */
  public static byte[] encodeNack(
      int requestId, String[] queues, String[] messageIds, String[] errors) {
    Primitives.Writer w = new Primitives.Writer(128);
    w.writeU32(queues.length);
    for (int i = 0; i < queues.length; i++) {
      w.writeString(queues[i]);
      w.writeString(messageIds[i]);
      w.writeString(errors[i]);
    }
    return encodeFrame(Opcodes.NACK, (byte) 0, requestId, w.toByteArray());
  }

  // --- Admin opcodes ---

  /** Encode a CreateQueue frame (0xFD). */
  public static byte[] encodeCreateQueue(
      int requestId,
      String name,
      String onEnqueueScript,
      String onFailureScript,
      long visibilityTimeoutMs) {
    Primitives.Writer w = new Primitives.Writer(64);
    w.writeString(name);
    w.writeOptionalString(onEnqueueScript);
    w.writeOptionalString(onFailureScript);
    w.writeU64(visibilityTimeoutMs);
    return encodeFrame(Opcodes.CREATE_QUEUE, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a DeleteQueue frame (0xFB). */
  public static byte[] encodeDeleteQueue(int requestId, String queue) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(queue);
    return encodeFrame(Opcodes.DELETE_QUEUE, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a GetStats frame (0xF9). */
  public static byte[] encodeGetStats(int requestId, String queue) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(queue);
    return encodeFrame(Opcodes.GET_STATS, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a ListQueues frame (0xF7). */
  public static byte[] encodeListQueues(int requestId) {
    return encodeFrame(Opcodes.LIST_QUEUES, (byte) 0, requestId, new byte[0]);
  }

  /** Encode a SetConfig frame (0xF5). */
  public static byte[] encodeSetConfig(int requestId, String key, String value) {
    Primitives.Writer w = new Primitives.Writer(64);
    w.writeString(key);
    w.writeString(value);
    return encodeFrame(Opcodes.SET_CONFIG, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a GetConfig frame (0xF3). */
  public static byte[] encodeGetConfig(int requestId, String key) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(key);
    return encodeFrame(Opcodes.GET_CONFIG, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a ListConfig frame (0xF1). */
  public static byte[] encodeListConfig(int requestId, String prefix) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(prefix);
    return encodeFrame(Opcodes.LIST_CONFIG, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a Redrive frame (0xEF). */
  public static byte[] encodeRedrive(int requestId, String dlqQueue, long count) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(dlqQueue);
    w.writeU64(count);
    return encodeFrame(Opcodes.REDRIVE, (byte) 0, requestId, w.toByteArray());
  }

  // --- Auth opcodes ---

  /** Encode a CreateApiKey frame (0xED). */
  public static byte[] encodeCreateApiKey(
      int requestId, String name, long expiresAtMs, boolean isSuperadmin) {
    Primitives.Writer w = new Primitives.Writer(64);
    w.writeString(name);
    w.writeU64(expiresAtMs);
    w.writeBool(isSuperadmin);
    return encodeFrame(Opcodes.CREATE_API_KEY, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a RevokeApiKey frame (0xEB). */
  public static byte[] encodeRevokeApiKey(int requestId, String keyId) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(keyId);
    return encodeFrame(Opcodes.REVOKE_API_KEY, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a ListApiKeys frame (0xE9). */
  public static byte[] encodeListApiKeys(int requestId) {
    return encodeFrame(Opcodes.LIST_API_KEYS, (byte) 0, requestId, new byte[0]);
  }

  /** Encode a SetAcl frame (0xE7). */
  public static byte[] encodeSetAcl(
      int requestId, String keyId, String[] kinds, String[] patterns) {
    Primitives.Writer w = new Primitives.Writer(128);
    w.writeString(keyId);
    w.writeU16(kinds.length);
    for (int i = 0; i < kinds.length; i++) {
      w.writeString(kinds[i]);
      w.writeString(patterns[i]);
    }
    return encodeFrame(Opcodes.SET_ACL, (byte) 0, requestId, w.toByteArray());
  }

  /** Encode a GetAcl frame (0xE5). */
  public static byte[] encodeGetAcl(int requestId, String keyId) {
    Primitives.Writer w = new Primitives.Writer(32);
    w.writeString(keyId);
    return encodeFrame(Opcodes.GET_ACL, (byte) 0, requestId, w.toByteArray());
  }
}
