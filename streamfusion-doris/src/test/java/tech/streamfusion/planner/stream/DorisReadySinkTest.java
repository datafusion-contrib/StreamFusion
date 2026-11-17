package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.batch.DorisBatchStreamLoad;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.junit.jupiter.api.Test;

class DorisReadySinkTest {
  @Test
  void anEmptyWriterCanFlushImmediatelyAfterItsRealLoaderStarts() throws Exception {
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });
    server.start();
    try {
      var options = DorisOptions.builder()
          .setFenodes("127.0.0.1:" + server.getAddress().getPort())
          .setBenodes("127.0.0.1:" + server.getAddress().getPort())
          .setTableIdentifier("test.records").setUsername("root").setPassword("").build();
      var execution = DorisExecutionOptions.builder().setStreamLoadProp(new Properties()).build();
      for (int i = 0; i < 20; i++) {
        var loader = new DorisBatchStreamLoad(options, DorisReadOptions.defaults(), execution,
            new LabelGenerator("startup-" + i, false), 0);
        try {
          DorisReadySink.awaitStarted(loader);
          assertTrue(loader.isLoadThreadAlive());
          assertTrue(loader.checkpointFlush());
          assertNull(loader.getException().get());
        } finally {
          loader.close();
        }
      }
    } finally {
      server.stop(0);
    }
  }
}
