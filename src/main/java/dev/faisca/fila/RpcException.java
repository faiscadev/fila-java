package dev.faisca.fila;

/** Thrown for protocol-level failures not mapped to a specific Fila exception. */
public class RpcException extends FilaException {
  private final int errorCode;

  public RpcException(int errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  /** Returns the FIBP error code of the failed call. */
  public int getErrorCode() {
    return errorCode;
  }
}
