package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for EnqueueResult. */
class EnqueueResultTest {

  @Test
  void successResult() {
    EnqueueResult result = EnqueueResult.success("msg-123");
    assertTrue(result.isSuccess());
    assertEquals("msg-123", result.getMessageId());
  }

  @Test
  void successGetErrorThrows() {
    EnqueueResult result = EnqueueResult.success("msg-123");
    assertThrows(IllegalStateException.class, result::getError);
  }

  @Test
  void errorResult() {
    EnqueueResult result = EnqueueResult.error("queue not found");
    assertFalse(result.isSuccess());
    assertEquals("queue not found", result.getError());
  }

  @Test
  void errorGetMessageIdThrows() {
    EnqueueResult result = EnqueueResult.error("queue not found");
    assertThrows(IllegalStateException.class, result::getMessageId);
  }
}
