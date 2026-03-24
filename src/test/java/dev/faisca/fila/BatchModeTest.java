package dev.faisca.fila;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for BatchMode configuration. */
class BatchModeTest {

  @Test
  void autoDefaultMaxBatchSize() {
    BatchMode mode = BatchMode.auto();
    assertEquals(BatchMode.Kind.AUTO, mode.getKind());
    assertEquals(100, mode.getMaxBatchSize());
  }

  @Test
  void autoCustomMaxBatchSize() {
    BatchMode mode = BatchMode.auto(50);
    assertEquals(BatchMode.Kind.AUTO, mode.getKind());
    assertEquals(50, mode.getMaxBatchSize());
  }

  @Test
  void autoRejectsZeroMaxBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> BatchMode.auto(0));
  }

  @Test
  void autoRejectsNegativeMaxBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> BatchMode.auto(-1));
  }

  @Test
  void lingerConfigValues() {
    BatchMode mode = BatchMode.linger(10, 50);
    assertEquals(BatchMode.Kind.LINGER, mode.getKind());
    assertEquals(10, mode.getLingerMs());
    assertEquals(50, mode.getMaxBatchSize());
  }

  @Test
  void lingerRejectsZeroLingerMs() {
    assertThrows(IllegalArgumentException.class, () -> BatchMode.linger(0, 50));
  }

  @Test
  void lingerRejectsZeroBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> BatchMode.linger(10, 0));
  }

  @Test
  void disabledMode() {
    BatchMode mode = BatchMode.disabled();
    assertEquals(BatchMode.Kind.DISABLED, mode.getKind());
  }
}
