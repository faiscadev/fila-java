package dev.faisca.fila.fibp;

/** FIBP protocol opcodes and error codes. */
public final class Opcodes {
  private Opcodes() {}

  // Protocol version
  public static final int PROTOCOL_VERSION = 1;

  // Default max frame size (16 MiB)
  public static final int DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024;

  // Control opcodes (0x00-0x0F)
  public static final byte HANDSHAKE = 0x01;
  public static final byte HANDSHAKE_OK = 0x02;
  public static final byte PING = 0x03;
  public static final byte PONG = 0x04;
  public static final byte DISCONNECT = 0x05;

  // Hot-path opcodes (0x10-0x1F)
  public static final byte ENQUEUE = 0x10;
  public static final byte ENQUEUE_RESULT = 0x11;
  public static final byte CONSUME = 0x12;
  public static final byte CONSUME_OK = 0x13;
  public static final byte DELIVERY = 0x14;
  public static final byte CANCEL_CONSUME = 0x15;
  public static final byte ACK = 0x16;
  public static final byte ACK_RESULT = 0x17;
  public static final byte NACK = 0x18;
  public static final byte NACK_RESULT = 0x19;

  // Error opcode
  public static final byte ERROR = (byte) 0xFE;

  // Admin opcodes (0xFD downward)
  public static final byte CREATE_QUEUE = (byte) 0xFD;
  public static final byte CREATE_QUEUE_RESULT = (byte) 0xFC;
  public static final byte DELETE_QUEUE = (byte) 0xFB;
  public static final byte DELETE_QUEUE_RESULT = (byte) 0xFA;
  public static final byte GET_STATS = (byte) 0xF9;
  public static final byte GET_STATS_RESULT = (byte) 0xF8;
  public static final byte LIST_QUEUES = (byte) 0xF7;
  public static final byte LIST_QUEUES_RESULT = (byte) 0xF6;
  public static final byte SET_CONFIG = (byte) 0xF5;
  public static final byte SET_CONFIG_RESULT = (byte) 0xF4;
  public static final byte GET_CONFIG = (byte) 0xF3;
  public static final byte GET_CONFIG_RESULT = (byte) 0xF2;
  public static final byte LIST_CONFIG = (byte) 0xF1;
  public static final byte LIST_CONFIG_RESULT = (byte) 0xF0;
  public static final byte REDRIVE = (byte) 0xEF;
  public static final byte REDRIVE_RESULT = (byte) 0xEE;
  public static final byte CREATE_API_KEY = (byte) 0xED;
  public static final byte CREATE_API_KEY_RESULT = (byte) 0xEC;
  public static final byte REVOKE_API_KEY = (byte) 0xEB;
  public static final byte REVOKE_API_KEY_RESULT = (byte) 0xEA;
  public static final byte LIST_API_KEYS = (byte) 0xE9;
  public static final byte LIST_API_KEYS_RESULT = (byte) 0xE8;
  public static final byte SET_ACL = (byte) 0xE7;
  public static final byte SET_ACL_RESULT = (byte) 0xE6;
  public static final byte GET_ACL = (byte) 0xE5;
  public static final byte GET_ACL_RESULT = (byte) 0xE4;

  // Error codes
  public static final byte ERR_OK = 0x00;
  public static final byte ERR_QUEUE_NOT_FOUND = 0x01;
  public static final byte ERR_MESSAGE_NOT_FOUND = 0x02;
  public static final byte ERR_QUEUE_ALREADY_EXISTS = 0x03;
  public static final byte ERR_LUA_COMPILATION = 0x04;
  public static final byte ERR_STORAGE = 0x05;
  public static final byte ERR_NOT_A_DLQ = 0x06;
  public static final byte ERR_PARENT_QUEUE_NOT_FOUND = 0x07;
  public static final byte ERR_INVALID_CONFIG_VALUE = 0x08;
  public static final byte ERR_CHANNEL_FULL = 0x09;
  public static final byte ERR_UNAUTHORIZED = 0x0A;
  public static final byte ERR_FORBIDDEN = 0x0B;
  public static final byte ERR_NOT_LEADER = 0x0C;
  public static final byte ERR_UNSUPPORTED_VERSION = 0x0D;
  public static final byte ERR_INVALID_FRAME = 0x0E;
  public static final byte ERR_API_KEY_NOT_FOUND = 0x0F;
  public static final byte ERR_NODE_NOT_READY = 0x10;
  public static final byte ERR_INTERNAL = (byte) 0xFF;

  // Flags
  public static final byte FLAG_CONTINUATION = 0x01;
}
