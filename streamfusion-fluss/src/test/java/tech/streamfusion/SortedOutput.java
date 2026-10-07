package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Exact multiset parity with bounded heap, including duplicate rows. */
final class SortedOutput implements AutoCloseable {
  private final Path directory = Files.createTempDirectory("streamfusion-fluss-parity-");
  private final List<Path> chunks = new ArrayList<>();
  private final List<String> buffered = new ArrayList<>();
  private final int chunkRows;
  private long bufferedBytes;
  private long count;

  SortedOutput() throws IOException {
    this(50_000);
  }

  SortedOutput(int chunkRows) throws IOException {
    this.chunkRows = chunkRows;
  }

  void add(String row) throws IOException {
    buffered.add(row);
    bufferedBytes += row.length() * 2L;
    count++;
    if (buffered.size() >= chunkRows || bufferedBytes >= 8 * 1024 * 1024) spill();
  }

  private void spill() throws IOException {
    if (buffered.isEmpty()) return;
    Collections.sort(buffered);
    Path path = directory.resolve("chunk-" + chunks.size());
    chunks.add(path);
    try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
      for (String row : buffered) {
        byte[] bytes = row.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    }
    buffered.clear();
    bufferedBytes = 0;
  }

  void assertSame(SortedOutput other, String query) throws IOException {
    assertEquals(count, other.count, query + " output row count");
    spill();
    other.spill();
    try (Merge expected = new Merge(chunks);
        Merge actual = new Merge(other.chunks)) {
      for (long row = 0; row < count; row++) {
        assertEquals(expected.next(), actual.next(), query + " output row " + row);
      }
      assertEquals(null, expected.next());
      assertEquals(null, actual.next());
    }
  }

  private record Head(String value, int chunk) {}

  private static final class Merge implements AutoCloseable {
    private final List<DataInputStream> streams = new ArrayList<>();
    private final PriorityQueue<Head> heads =
        new PriorityQueue<>(Comparator.comparing(Head::value));

    Merge(List<Path> chunks) throws IOException {
      try {
        for (Path chunk : chunks) {
          DataInputStream stream =
              new DataInputStream(new BufferedInputStream(Files.newInputStream(chunk)));
          streams.add(stream);
          String value = read(stream);
          if (value != null) heads.add(new Head(value, streams.size() - 1));
        }
      } catch (Throwable failure) {
        try {
          close();
        } catch (IOException closing) {
          failure.addSuppressed(closing);
        }
        throw failure;
      }
    }

    String next() throws IOException {
      Head head = heads.poll();
      if (head == null) return null;
      String value = read(streams.get(head.chunk()));
      if (value != null) heads.add(new Head(value, head.chunk()));
      return head.value();
    }

    private static String read(DataInputStream stream) throws IOException {
      int size;
      try {
        size = stream.readInt();
      } catch (EOFException end) {
        return null;
      }
      if (size < 0) throw new IOException("Invalid parity record length");
      byte[] bytes = stream.readNBytes(size);
      if (bytes.length != size) throw new EOFException("Truncated parity record");
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (DataInputStream stream : streams) {
        try {
          stream.close();
        } catch (IOException closing) {
          if (failure == null) failure = closing;
          else failure.addSuppressed(closing);
        }
      }
      if (failure != null) throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    buffered.clear();
    for (Path chunk : chunks) Files.deleteIfExists(chunk);
    Files.deleteIfExists(directory);
  }
}
