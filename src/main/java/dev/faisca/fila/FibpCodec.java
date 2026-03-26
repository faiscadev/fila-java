package dev.faisca.fila;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes and decodes FIBP wire messages.
 *
 * <p>All integers are big-endian. String lengths are encoded as u16BE unless noted otherwise.
 *
 * <p>Wire formats (from fila-core/src/fibp/wire.rs):
 *
 * <pre>
 * Enqueue request:
 *   queue_len:u16 | queue:utf8 | msg_count:u16 | messages...
 *   Each message: header_count:u8 | (key:str16 val:str16)* | payload_len:u32 | payload
 *
 * Enqueue response:
 *   count:u16 | results...
 *   ok=1: 0x01 | msg_id:str16
 *   ok=0: 0x00 | err_code:u16 | err_msg:str16
 *
 * Consume request:
 *   queue:str16 | initial_credits:u32
 *
 * Consume push (server → client):
 *   count:u16 | messages...
 *   Each: msg_id:str16 | fairness_key:str16 | attempt_count:u32 |
 *         header_count:u8 | (key:str16 val:str16)* | payload_len:u32 | payload
 *
 * Ack request:
 *   count:u16 | items: (queue:str16 msg_id:str16)*
 *
 * Nack request:
 *   count:u16 | items: (queue:str16 msg_id:str16 error:str16)*
 *
 * Ack/Nack response:
 *   count:u16 | results: (0x01 | 0x00 err_code:u16 err_msg:str16)*
 * </pre>
 */
final class FibpCodec {

  // Enqueue error codes
  static final int ENQUEUE_ERR_QUEUE_NOT_FOUND = 1;

  // Ack/Nack error codes
  static final int ACK_NACK_ERR_MESSAGE_NOT_FOUND = 1;

  private FibpCodec() {}

  // ── Enqueue ───────────────────────────────────────────────────────────────

  /**
   * Encode an enqueue request. All messages must target the same queue (the first message's queue
   * name is used).
   */
  static byte[] encodeEnqueue(List<EnqueueMessage> messages) {
    if (messages.isEmpty()) {
      throw new IllegalArgumentException("messages must not be empty");
    }
    String queue = messages.get(0).getQueue();
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf)) {
      writeStr16(dos, queue);
      dos.writeShort(messages.size());
      for (EnqueueMessage msg : messages) {
        int headerCount = msg.getHeaders().size();
        if (headerCount > 255) {
          throw new FilaException("too many headers: " + headerCount + " exceeds maximum of 255");
        }
        dos.writeByte(headerCount);
        for (Map.Entry<String, String> entry : msg.getHeaders().entrySet()) {
          writeStr16(dos, entry.getKey());
          writeStr16(dos, entry.getValue());
        }
        dos.writeInt(msg.getPayload().length);
        dos.write(msg.getPayload());
      }
      dos.flush();
      return buf.toByteArray();
    } catch (IOException e) {
      throw new FilaException("encode enqueue failed", e);
    }
  }

  /**
   * Decode an enqueue response.
   *
   * <pre>
   * count:u16 | (0x01 msg_id:str16 | 0x00 err_code:u16 err_msg:str16)*
   * </pre>
   */
  static List<EnqueueResult> decodeEnqueueResponse(byte[] payload) {
    int pos = 0;
    int count = readU16(payload, pos);
    pos += 2;
    List<EnqueueResult> results = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int tag = payload[pos++] & 0xFF;
      if (tag == 1) {
        int idLen = readU16(payload, pos);
        pos += 2;
        String msgId = new String(payload, pos, idLen, StandardCharsets.UTF_8);
        pos += idLen;
        results.add(EnqueueResult.success(msgId));
      } else {
        int errCode = readU16(payload, pos);
        pos += 2;
        int errLen = readU16(payload, pos);
        pos += 2;
        String errMsg = new String(payload, pos, errLen, StandardCharsets.UTF_8);
        pos += errLen;
        results.add(EnqueueResult.error(errCode + ":" + errMsg));
      }
    }
    return results;
  }

  // ── Consume ───────────────────────────────────────────────────────────────

  /**
   * Encode a consume request.
   *
   * <pre>
   * queue:str16 | initial_credits:u32
   * </pre>
   */
  static byte[] encodeConsume(String queue, int initialCredits) {
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf)) {
      writeStr16(dos, queue);
      dos.writeInt(initialCredits);
      dos.flush();
      return buf.toByteArray();
    } catch (IOException e) {
      throw new FilaException("encode consume failed", e);
    }
  }

  /**
   * Decode a batch of server-pushed consume messages.
   *
   * <pre>
   * count:u16 | (msg_id:str16 fairness_key:str16 attempt_count:u32
   *              header_count:u8 (key:str16 val:str16)* payload_len:u32 payload)*
   * </pre>
   *
   * @param queue the queue name (not in the wire format; supplied by the caller)
   */
  static List<ConsumeMessage> decodePushBatch(byte[] payload, String queue) {
    int pos = 0;
    int count = readU16(payload, pos);
    pos += 2;
    List<ConsumeMessage> messages = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int idLen = readU16(payload, pos);
      pos += 2;
      String msgId = new String(payload, pos, idLen, StandardCharsets.UTF_8);
      pos += idLen;

      int fkLen = readU16(payload, pos);
      pos += 2;
      String fairnessKey = new String(payload, pos, fkLen, StandardCharsets.UTF_8);
      pos += fkLen;

      int attemptCount = readU32(payload, pos);
      pos += 4;

      int headerCount = payload[pos++] & 0xFF;
      Map<String, String> headers = new HashMap<>(headerCount * 2);
      for (int h = 0; h < headerCount; h++) {
        int kLen = readU16(payload, pos);
        pos += 2;
        String key = new String(payload, pos, kLen, StandardCharsets.UTF_8);
        pos += kLen;
        int vLen = readU16(payload, pos);
        pos += 2;
        String val = new String(payload, pos, vLen, StandardCharsets.UTF_8);
        pos += vLen;
        headers.put(key, val);
      }

      int dataLen = readU32(payload, pos);
      pos += 4;
      byte[] data = new byte[dataLen];
      System.arraycopy(payload, pos, data, 0, dataLen);
      pos += dataLen;

      messages.add(new ConsumeMessage(msgId, headers, data, fairnessKey, attemptCount, queue));
    }
    return messages;
  }

  // ── Ack ───────────────────────────────────────────────────────────────────

  /**
   * Encode an ack request.
   *
   * <pre>
   * count:u16 | (queue:str16 msg_id:str16)*
   * </pre>
   */
  static byte[] encodeAck(String queue, String msgId) {
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf)) {
      dos.writeShort(1);
      writeStr16(dos, queue);
      writeStr16(dos, msgId);
      dos.flush();
      return buf.toByteArray();
    } catch (IOException e) {
      throw new FilaException("encode ack failed", e);
    }
  }

  // ── Nack ──────────────────────────────────────────────────────────────────

  /**
   * Encode a nack request.
   *
   * <pre>
   * count:u16 | (queue:str16 msg_id:str16 error:str16)*
   * </pre>
   */
  static byte[] encodeNack(String queue, String msgId, String error) {
    try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(buf)) {
      dos.writeShort(1);
      writeStr16(dos, queue);
      writeStr16(dos, msgId);
      writeStr16(dos, error);
      dos.flush();
      return buf.toByteArray();
    } catch (IOException e) {
      throw new FilaException("encode nack failed", e);
    }
  }

  // ── Ack/Nack response ─────────────────────────────────────────────────────

  /**
   * Decode an ack or nack response and throw if the single item failed.
   *
   * <pre>
   * count:u16 | (0x01 | 0x00 err_code:u16 err_msg:str16)*
   * </pre>
   */
  static void decodeAckNackResponse(byte[] payload, boolean isAck) {
    int pos = 0;
    int count = readU16(payload, pos);
    pos += 2;
    if (count < 1) {
      throw new RpcException(RpcException.Code.INTERNAL, "no result from server");
    }
    int tag = payload[pos++] & 0xFF;
    if (tag != 1) {
      int errCode = readU16(payload, pos);
      pos += 2;
      int errLen = readU16(payload, pos);
      pos += 2;
      String msg = new String(payload, pos, errLen, StandardCharsets.UTF_8);
      throw decodeAckNackError(errCode, msg, isAck);
    }
  }

  private static FilaException decodeAckNackError(int errCode, String msg, boolean isAck) {
    String prefix = isAck ? "ack: " : "nack: ";
    return switch (errCode) {
      case ACK_NACK_ERR_MESSAGE_NOT_FOUND -> new MessageNotFoundException(prefix + msg);
      default -> new RpcException(RpcException.Code.INTERNAL, msg);
    };
  }

  // ── Error frame ───────────────────────────────────────────────────────────

  /**
   * Decode an ERROR frame payload (raw UTF-8 string) into a FilaException.
   *
   * <p>The server sends error messages as plain text. We map them by keyword matching, matching the
   * server's error message conventions.
   */
  static FilaException decodeError(byte[] payload) {
    String msg = new String(payload, StandardCharsets.UTF_8);
    String lower = msg.toLowerCase();
    if (lower.contains("not found") && lower.contains("queue")) {
      return new QueueNotFoundException(msg);
    }
    if (lower.contains("not found") && lower.contains("message")) {
      return new MessageNotFoundException(msg);
    }
    if (lower.contains("not found")) {
      return new QueueNotFoundException(msg);
    }
    if (lower.contains("permission denied") || lower.contains("forbidden")) {
      return new RpcException(RpcException.Code.PERMISSION_DENIED, msg);
    }
    if (lower.contains("authentication required")
        || lower.contains("unauthenticated")
        || lower.contains("invalid api key")) {
      return new RpcException(RpcException.Code.UNAUTHENTICATED, msg);
    }
    return new RpcException(RpcException.Code.INTERNAL, msg);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static void writeStr16(DataOutputStream dos, String s) throws IOException {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 65535) {
      throw new FilaException(
          "string too long: " + bytes.length + " bytes exceeds u16 maximum of 65535");
    }
    dos.writeShort(bytes.length);
    dos.write(bytes);
  }

  static int readU16(byte[] buf, int pos) {
    return ((buf[pos] & 0xFF) << 8) | (buf[pos + 1] & 0xFF);
  }

  static int readU32(byte[] buf, int pos) {
    return ((buf[pos] & 0xFF) << 24)
        | ((buf[pos + 1] & 0xFF) << 16)
        | ((buf[pos + 2] & 0xFF) << 8)
        | (buf[pos + 3] & 0xFF);
  }
}
