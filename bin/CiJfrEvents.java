import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

class CiJfrEvents {
  private static String quote(String value) {
    StringBuilder result = new StringBuilder("\"");
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character == '"' || character == '\\') {
        result.append('\\').append(character);
      } else if (character < 32) {
        result.append(String.format("\\u%04x", (int) character));
      } else {
        result.append(character);
      }
    }
    return result.append('"').toString();
  }

  private static void stack(StringBuilder line, RecordedEvent event) {
    line.append(",[");
    RecordedStackTrace stack = event.getStackTrace();
    if (stack != null) {
      List<RecordedFrame> frames = stack.getFrames();
      for (int index = 0; index < Math.min(32, frames.size()); index++) {
        if (index > 0) line.append(',');
        RecordedFrame frame = frames.get(index);
        line.append(
            quote(frame.getMethod().getType().getName() + "." + frame.getMethod().getName()));
      }
    }
    line.append(']');
  }

  public static void main(String[] arguments) throws Exception {
    try (RecordingFile recording = new RecordingFile(Path.of(arguments[0]));
        BufferedWriter output =
            new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
      while (recording.hasMoreEvents()) {
        RecordedEvent event = recording.readEvent();
        String type = event.getEventType().getName();
        StringBuilder line = new StringBuilder("[").append(quote(type));
        if (type.equals("jdk.ExecutionSample") || type.equals("jdk.NativeMethodSample")) {
          RecordedThread thread = event.getThread("sampledThread");
          line.append(',').append(quote(thread == null ? "<unknown>" : thread.getJavaName()));
          stack(line, event);
        } else if (type.equals("jdk.ThreadPark")
            || type.equals("jdk.ThreadSleep")
            || type.equals("jdk.JavaMonitorWait")) {
          RecordedThread thread = event.getThread();
          line.append(',').append(quote(thread == null ? "<unknown>" : thread.getJavaName()));
          line.append(',').append(event.getDuration().toNanos() / 1_000_000_000.0);
          stack(line, event);
        } else if (type.equals("jdk.GarbageCollection")) {
          line.append(',')
              .append(quote(event.getString("name") + " / " + event.getString("cause")));
          line.append(',').append(event.getDuration().toNanos() / 1_000_000_000.0);
        } else if (type.equals("jdk.GCHeapSummary")) {
          line.append(',').append(quote(String.valueOf((Object) event.getValue("when"))));
          line.append(',').append(event.getLong("heapUsed"));
        } else if (type.equals("jdk.Compilation")) {
          String compiler;
          if (event.getEventType().getField("compiler") != null) {
            compiler = event.getString("compiler");
          } else {
            int level = event.getShort("compileLevel");
            compiler = level == 4 ? "c2" : level >= 1 && level <= 3 ? "c1" : "level-" + level;
          }
          line.append(',').append(quote(compiler));
          line.append(',').append(event.getDuration().toNanos() / 1_000_000_000.0);
        } else if (type.equals("jdk.ObjectAllocationSample")) {
          line.append(',').append(quote(event.getClass("objectClass").getName()));
          line.append(',').append(event.getLong("weight"));
        } else {
          continue;
        }
        output.write(line.append(']').toString());
        output.newLine();
      }
    }
  }
}
