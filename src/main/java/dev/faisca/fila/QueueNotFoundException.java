package dev.faisca.fila;

/** Thrown when an operation references a queue that does not exist. */
public class QueueNotFoundException extends FilaException {
  public QueueNotFoundException(String message) {
    super(message);
  }
}
