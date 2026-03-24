package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for BatchEnqueueResult. */
class BatchEnqueueResultTest {

  @Test
  void successResult() {
    BatchEnqueueResult result = BatchEnqueueResult.success("msg-123");
    assertTrue(result.isSuccess());
    assertEquals("msg-123", result.getMessageId());
  }

  @Test
  void successGetErrorThrows() {
    BatchEnqueueResult result = BatchEnqueueResult.success("msg-123");
    assertThrows(IllegalStateException.class, result::getError);
  }

  @Test
  void errorResult() {
    BatchEnqueueResult result = BatchEnqueueResult.error("queue not found");
    assertFalse(result.isSuccess());
    assertEquals("queue not found", result.getError());
  }

  @Test
  void errorGetMessageIdThrows() {
    BatchEnqueueResult result = BatchEnqueueResult.error("queue not found");
    assertThrows(IllegalStateException.class, result::getMessageId);
  }
}
