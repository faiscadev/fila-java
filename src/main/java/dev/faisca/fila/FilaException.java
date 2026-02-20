package dev.faisca.fila;

/** Base exception for all Fila client errors. */
public class FilaException extends RuntimeException {
  public FilaException(String message) {
    super(message);
  }

  public FilaException(String message, Throwable cause) {
    super(message, cause);
  }
}
