package dev.faisca.fila;

/** Thrown when an operation references a message that does not exist. */
public class MessageNotFoundException extends FilaException {
  public MessageNotFoundException(String message) {
    super(message);
  }
}
