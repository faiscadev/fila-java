package dev.faisca.fila;

import io.grpc.Status;

/** Thrown for unexpected gRPC failures not mapped to a specific Fila exception. */
public class RpcException extends FilaException {
  private final Status.Code code;

  public RpcException(Status.Code code, String message) {
    super(message);
    this.code = code;
  }

  /** Returns the gRPC status code of the failed call. */
  public Status.Code getCode() {
    return code;
  }
}
