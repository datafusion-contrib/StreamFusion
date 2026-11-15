package tech.streamfusion.fluss;

import java.lang.reflect.Field;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.rpc.netty.client.NettyClient;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;

/** Fluss 1.0 transport hook: preserve its initializer and allocate direct frames once. */
final class FlussDirectReceive {
  private FlussDirectReceive() {}

  static boolean supported() {
    try {
      Field rpc = FlussConnection.class.getDeclaredField("rpcClient");
      Field bootstrap = NettyClient.class.getDeclaredField("bootstrap");
      return bootstrap.getType() == Bootstrap.class
          && rpc.trySetAccessible()
          && bootstrap.trySetAccessible();
    } catch (ReflectiveOperationException | SecurityException unavailable) {
      return false;
    }
  }

  static Bootstrap bootstrap(FlussConnection connection) throws ReflectiveOperationException {
    Field rpcField = FlussConnection.class.getDeclaredField("rpcClient");
    rpcField.setAccessible(true);
    Object rpc = rpcField.get(connection);
    Field bootstrapField = NettyClient.class.getDeclaredField("bootstrap");
    bootstrapField.setAccessible(true);
    return (Bootstrap) bootstrapField.get(rpc);
  }

  static void install(FlussConnection connection) throws ReflectiveOperationException {
    Bootstrap bootstrap = bootstrap(connection);
    ChannelHandler original = bootstrap.config().handler();
    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) {
            channel.pipeline().addLast(original);
            channel.pipeline().get(LengthFieldBasedFrameDecoder.class).setCumulator(CUMULATOR);
          }
        });
  }

  static final ByteToMessageDecoder.Cumulator CUMULATOR =
      (allocator, current, incoming) -> {
        boolean releaseIncoming = true;
        try {
          if (current == incoming || !incoming.isReadable()) return current;
          int readable = current.readableBytes();
          int incomingBytes = incoming.readableBytes();
          int total = Math.addExact(readable, incomingBytes);
          long frameBytes = 4;
          if (total >= 4) {
            long length = 0;
            for (int index = 0; index < 4; index++) {
              int value =
                  index < readable
                      ? current.getUnsignedByte(current.readerIndex() + index)
                      : incoming.getUnsignedByte(incoming.readerIndex() + index - readable);
              length = (length << 8) | value;
            }
            frameBytes += length;
            if (frameBytes > Integer.MAX_VALUE)
              throw new TooLongFrameException("Fluss frame exceeds integer range");
          }
          int capacity = Math.max(total, (int) frameBytes);
          if (readable == 0
              && incoming.isDirect()
              && incoming.capacity() - incoming.readerIndex() >= capacity) {
            current.release();
            releaseIncoming = false;
            return incoming;
          }
          if (current.isDirect() && current.refCnt() == 1) {
            if (current.readerIndex() > 0 && current.capacity() >= capacity)
              current.discardReadBytes();
            if (current.capacity() - current.readerIndex() >= capacity
                && current.writableBytes() >= incomingBytes) {
              current.writeBytes(incoming, incoming.readerIndex(), incomingBytes);
              return current;
            }
          }
          ByteBuf replacement = allocator.directBuffer(capacity);
          boolean success = false;
          try {
            replacement.writeBytes(current, current.readerIndex(), readable);
            replacement.writeBytes(incoming, incoming.readerIndex(), incomingBytes);
            success = true;
          } finally {
            if (!success) replacement.release();
          }
          current.release();
          return replacement;
        } finally {
          if (releaseIncoming) incoming.release();
        }
      };
}
