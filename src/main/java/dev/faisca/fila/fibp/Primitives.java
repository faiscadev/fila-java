package dev.faisca.fila.fibp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reader and writer for FIBP primitive types using big-endian encoding.
 *
 * <p>The writer accumulates bytes into an expanding buffer. The reader consumes bytes from a
 * provided byte array.
 */
public final class Primitives {
  private Primitives() {}

  /** Writer that accumulates encoded bytes. */
  public static final class Writer {
    private ByteBuffer buf;

    public Writer() {
      this(256);
    }

    public Writer(int initialCapacity) {
      buf = ByteBuffer.allocate(initialCapacity).order(ByteOrder.BIG_ENDIAN);
    }

    private void ensureCapacity(int needed) {
      if (buf.remaining() < needed) {
        int newCap = Math.max(buf.capacity() * 2, buf.position() + needed);
        ByteBuffer newBuf = ByteBuffer.allocate(newCap).order(ByteOrder.BIG_ENDIAN);
        buf.flip();
        newBuf.put(buf);
        buf = newBuf;
      }
    }

    public void writeU8(int value) {
      ensureCapacity(1);
      buf.put((byte) (value & 0xFF));
    }

    public void writeU16(int value) {
      ensureCapacity(2);
      buf.putShort((short) (value & 0xFFFF));
    }

    public void writeU32(int value) {
      ensureCapacity(4);
      buf.putInt(value);
    }

    public void writeU32(long value) {
      ensureCapacity(4);
      buf.putInt((int) (value & 0xFFFFFFFFL));
    }

    public void writeU64(long value) {
      ensureCapacity(8);
      buf.putLong(value);
    }

    public void writeI64(long value) {
      ensureCapacity(8);
      buf.putLong(value);
    }

    public void writeF64(double value) {
      ensureCapacity(8);
      buf.putDouble(value);
    }

    public void writeBool(boolean value) {
      writeU8(value ? 1 : 0);
    }

    public void writeString(String value) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (bytes.length > 65535) {
        throw new IllegalArgumentException(
            "string exceeds u16 max length: " + bytes.length + " bytes");
      }
      writeU16(bytes.length);
      ensureCapacity(bytes.length);
      buf.put(bytes);
    }

    public void writeBytes(byte[] value) {
      writeU32(value.length);
      ensureCapacity(value.length);
      buf.put(value);
    }

    public void writeStringMap(Map<String, String> map) {
      if (map.size() > 65535) {
        throw new IllegalArgumentException("map exceeds u16 max entry count: " + map.size());
      }
      writeU16(map.size());
      for (Map.Entry<String, String> entry : map.entrySet()) {
        writeString(entry.getKey());
        writeString(entry.getValue());
      }
    }

    public void writeStringList(String[] list) {
      writeU16(list.length);
      for (String s : list) {
        writeString(s);
      }
    }

    public void writeOptionalString(String value) {
      if (value == null) {
        writeU8(0);
      } else {
        writeU8(1);
        writeString(value);
      }
    }

    /** Returns the encoded bytes. */
    public byte[] toByteArray() {
      byte[] result = new byte[buf.position()];
      buf.flip();
      buf.get(result);
      buf.flip(); // restore position for potential further use
      buf.position(result.length);
      return result;
    }

    /** Returns current write position (number of bytes written). */
    public int position() {
      return buf.position();
    }
  }

  /** Reader that consumes bytes from a buffer. */
  public static final class Reader {
    private final ByteBuffer buf;

    public Reader(byte[] data) {
      buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    }

    public Reader(byte[] data, int offset, int length) {
      buf = ByteBuffer.wrap(data, offset, length).order(ByteOrder.BIG_ENDIAN);
    }

    public int readU8() {
      return buf.get() & 0xFF;
    }

    public int readU16() {
      return buf.getShort() & 0xFFFF;
    }

    public int readU32AsInt() {
      return buf.getInt();
    }

    public long readU32() {
      return buf.getInt() & 0xFFFFFFFFL;
    }

    public long readU64() {
      return buf.getLong();
    }

    public long readI64() {
      return buf.getLong();
    }

    public double readF64() {
      return buf.getDouble();
    }

    public boolean readBool() {
      return readU8() != 0;
    }

    public String readString() {
      int len = readU16();
      byte[] bytes = new byte[len];
      buf.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }

    public byte[] readBytes() {
      int len = readU32AsInt();
      if (len < 0 || len > buf.remaining()) {
        throw new IllegalArgumentException(
            "invalid byte array length: " + len + " (remaining: " + buf.remaining() + ")");
      }
      byte[] bytes = new byte[len];
      buf.get(bytes);
      return bytes;
    }

    public Map<String, String> readStringMap() {
      int count = readU16();
      Map<String, String> map = new LinkedHashMap<>(count);
      for (int i = 0; i < count; i++) {
        String key = readString();
        String value = readString();
        map.put(key, value);
      }
      return map;
    }

    public String[] readStringList() {
      int count = readU16();
      String[] list = new String[count];
      for (int i = 0; i < count; i++) {
        list[i] = readString();
      }
      return list;
    }

    public String readOptionalString() {
      int present = readU8();
      if (present == 0) {
        return null;
      }
      return readString();
    }

    /** Returns the number of remaining bytes. */
    public int remaining() {
      return buf.remaining();
    }
  }
}
