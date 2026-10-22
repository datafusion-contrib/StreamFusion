package tech.streamfusion.doris;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;

interface DorisBatchReader extends AutoCloseable {
  VectorSchemaRoot readBatch() throws IOException;

  Schema schema() throws IOException;

  @Override
  void close() throws IOException;

  static boolean flight(DorisReadOptions reads) {
    return !reads.getUseOldApi()
        && reads.getUseFlightSql()
        && reads.getFlightSqlPort() != null
        && reads.getFlightSqlPort() > 0;
  }

  static DorisBatchReader create(
      DorisSourceSplit split, DorisOptions options, DorisReadOptions reads, Schema expected) {
    return flight(reads)
        ? new DorisArrowReader(split, options, reads, expected)
        : new DorisThriftArrowReader((DorisSnapshotSplit) split, options, reads, expected);
  }

  static DorisBatchReader probe(DorisOptions options, DorisReadOptions reads, Schema expected)
      throws Exception {
    if (flight(reads)) {
      reads.setFilterQuery("1 = 0");
      return create(
          new DorisSnapshotSplit(
              "schema",
              org.apache.doris.flink.rest.PartitionDefinition.emptyPartition(
                  options.getTableIdentifier())),
          options,
          reads,
          expected);
    }
    reads.setRowLimit(1L);
    var partitions =
        org.apache.doris.flink.rest.RestService.findPartitions(
            options, reads, org.slf4j.LoggerFactory.getLogger(DorisBatchReader.class));
    for (var partition : partitions) {
      var allocator = new org.apache.arrow.memory.RootAllocator();
      DorisThriftArrowReader reader = null;
      try {
        reader =
            new DorisThriftArrowReader(
                new DorisSnapshotSplit("schema", partition),
                options,
                reads,
                expected,
                allocator,
                true);
        if (reader.schema() != null) return reader;
      } catch (Throwable failure) {
        try {
          if (reader != null) reader.close();
          else allocator.close();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
      reader.close();
    }
    return null;
  }
}
