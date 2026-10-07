package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

class FlussDirectReceiveTest {
  @Test
  void fragmentedLengthAllocatesTheCompleteFrameAndThenReusesIt() {
    var allocator = UnpooledByteBufAllocator.DEFAULT;
    var first = Unpooled.directBuffer(2).writeShort(0);
    var frame = FlussDirectReceive.CUMULATOR.cumulate(allocator, Unpooled.EMPTY_BUFFER, first);
    try {
      assertEquals(0, first.refCnt());
      var header = Unpooled.directBuffer(2).writeShort(128);
      frame = FlussDirectReceive.CUMULATOR.cumulate(allocator, frame, header);
      assertEquals(132, frame.capacity());
      assertEquals(0, header.refCnt());
      long address = frame.memoryAddress();
      for (int part = 0; part < 4; part++) {
        var bytes = Unpooled.directBuffer(32).writeZero(32);
        frame = FlussDirectReceive.CUMULATOR.cumulate(allocator, frame, bytes);
        assertEquals(address, frame.memoryAddress());
        assertEquals(0, bytes.refCnt());
      }
      assertEquals(132, frame.readableBytes());
      assertEquals(128, frame.readInt());
    } finally {
      frame.release();
    }
  }

  @Test
  void retainedResponsesAreNotOverwrittenByTheNextFrame() {
    var frame = Unpooled.directBuffer(8).writeInt(4).writeInt(42);
    var held = frame.readRetainedSlice(8);
    var incoming = Unpooled.directBuffer(6).writeInt(4).writeShort(7);
    var next =
        FlussDirectReceive.CUMULATOR.cumulate(UnpooledByteBufAllocator.DEFAULT, frame, incoming);
    try {
      assertEquals(42, held.getInt(4));
      assertEquals(1, held.refCnt());
      assertNotEquals(held.memoryAddress(), next.memoryAddress());
      assertEquals(8, next.capacity());
      assertEquals(4, next.getInt(0));
      assertEquals(0, incoming.refCnt());
    } finally {
      held.release();
      next.release();
    }
  }

  @Test
  void invalidLengthReleasesInputAndLeavesThePreviousOwnerIntact() {
    var current = Unpooled.directBuffer(2).writeShort(65535);
    var incoming = Unpooled.directBuffer(2).writeShort(65535);
    try {
      assertThrows(
          TooLongFrameException.class,
          () ->
              FlussDirectReceive.CUMULATOR.cumulate(
                  UnpooledByteBufAllocator.DEFAULT, current, incoming));
      assertEquals(0, incoming.refCnt());
      assertEquals(1, current.refCnt());
    } finally {
      current.release();
    }
  }
}
