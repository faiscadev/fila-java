package dev.faisca.fila.fibp;

/** Represents the 6-byte frame header in the FIBP protocol. */
public record FrameHeader(byte opcode, byte flags, int requestId) {

  /** Size of the frame header in bytes. */
  public static final int SIZE = 6;

  /** Returns true if the CONTINUATION flag is set. */
  public boolean isContinuation() {
    return (flags & Opcodes.FLAG_CONTINUATION) != 0;
  }
}
