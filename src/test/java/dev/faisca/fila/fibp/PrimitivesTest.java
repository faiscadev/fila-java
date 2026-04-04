package dev.faisca.fila.fibp;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for FIBP primitives encoding/decoding. */
class PrimitivesTest {

  @Test
  void u8RoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeU8(0);
    w.writeU8(127);
    w.writeU8(255);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals(0, r.readU8());
    assertEquals(127, r.readU8());
    assertEquals(255, r.readU8());
  }

  @Test
  void u16RoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeU16(0);
    w.writeU16(1000);
    w.writeU16(65535);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals(0, r.readU16());
    assertEquals(1000, r.readU16());
    assertEquals(65535, r.readU16());
  }

  @Test
  void u32RoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeU32(0);
    w.writeU32(100_000);
    w.writeU32(0xFFFFFFFFL);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals(0, r.readU32());
    assertEquals(100_000, r.readU32());
    assertEquals(0xFFFFFFFFL, r.readU32());
  }

  @Test
  void u64RoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeU64(0);
    w.writeU64(Long.MAX_VALUE);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals(0, r.readU64());
    assertEquals(Long.MAX_VALUE, r.readU64());
  }

  @Test
  void boolRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeBool(true);
    w.writeBool(false);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertTrue(r.readBool());
    assertFalse(r.readBool());
  }

  @Test
  void stringRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeString("hello");
    w.writeString("");
    w.writeString("unicode: \u00e9\u00e8\u00ea");

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals("hello", r.readString());
    assertEquals("", r.readString());
    assertEquals("unicode: \u00e9\u00e8\u00ea", r.readString());
  }

  @Test
  void bytesRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    byte[] data = {1, 2, 3, 4, 5};
    w.writeBytes(data);
    w.writeBytes(new byte[0]);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertArrayEquals(data, r.readBytes());
    assertArrayEquals(new byte[0], r.readBytes());
  }

  @Test
  void stringMapRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    Map<String, String> map = new LinkedHashMap<>();
    map.put("key1", "val1");
    map.put("key2", "val2");
    w.writeStringMap(map);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    Map<String, String> result = r.readStringMap();
    assertEquals(2, result.size());
    assertEquals("val1", result.get("key1"));
    assertEquals("val2", result.get("key2"));
  }

  @Test
  void optionalStringRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeOptionalString("present");
    w.writeOptionalString(null);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals("present", r.readOptionalString());
    assertNull(r.readOptionalString());
  }

  @Test
  void f64RoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeF64(3.14);
    w.writeF64(0.0);

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    assertEquals(3.14, r.readF64(), 0.001);
    assertEquals(0.0, r.readF64(), 0.001);
  }

  @Test
  void stringListRoundTrip() {
    Primitives.Writer w = new Primitives.Writer();
    w.writeStringList(new String[] {"a", "b", "c"});

    Primitives.Reader r = new Primitives.Reader(w.toByteArray());
    String[] list = r.readStringList();
    assertArrayEquals(new String[] {"a", "b", "c"}, list);
  }
}
