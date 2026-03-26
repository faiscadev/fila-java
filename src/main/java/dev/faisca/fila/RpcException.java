package dev.faisca.fila;

/** Thrown for unexpected transport-level failures not mapped to a specific Fila exception. */
public class RpcException extends FilaException {

  /** Status codes mirroring common error categories. */
  public enum Code {
    INTERNAL,
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    UNAVAILABLE,
    CANCELLED,
    UNKNOWN
  }

  private final Code code;

  public RpcException(Code code, String message) {
    super(message);
    this.code = code;
  }

  /** Returns the status code of the failed call. */
  public Code getCode() {
    return code;
  }
}
