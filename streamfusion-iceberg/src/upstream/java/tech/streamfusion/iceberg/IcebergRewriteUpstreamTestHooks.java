package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewriteRunner;

/** Test-only access to the production file workers; upstream metadata policies are unchanged. */
public final class IcebergRewriteUpstreamTestHooks {
  private IcebergRewriteUpstreamTestHooks() {}

  public static List<DataFile> rewrite(
      StreamExecutionEnvironment env, Table table, PartitionSpec spec, List<CombinedScanTask> tasks)
      throws Exception {
    var groups = new ArrayList<RewriteFileGroup>();
    for (var task : tasks) {
      var files = List.copyOf(task.files());
      int index = groups.size();
      var info =
          ImmutableRewriteDataFiles.FileGroupInfo.builder()
              .globalIndex(index)
              .partitionIndex(index)
              .partition(files.get(0).file().partition())
              .build();
      var group =
          new RewriteFileGroup(info, files, spec.specId(), Long.MAX_VALUE, Long.MAX_VALUE, 1);
      IcebergRewriteFiles.validate(table, group, false);
      groups.add(group);
    }
    var type = Class.forName(NativeIcebergMaintenance.class.getName() + "$RewriteOperator");
    var constructor = type.getDeclaredConstructor(SerializableTable.class, boolean.class);
    constructor.setAccessible(true);
    @SuppressWarnings("unchecked")
    var worker =
        (OneInputStreamOperator<RewriteFileGroup, RewriteFileGroup>)
            constructor.newInstance(SerializableTable.copyOf(table), false);
    var output = new ArrayList<DataFile>();
    try (var result =
        env.fromData(groups, TypeInformation.of(RewriteFileGroup.class))
            .transform(
                "Upstream native rewrite worker",
                TypeInformation.of(RewriteFileGroup.class),
                worker)
            .setParallelism(1)
            .executeAndCollect()) {
      while (result.hasNext()) output.addAll(result.next().addedFiles());
    }
    assertFalse(output.isEmpty(), "Selected rewrite groups must perform native file work");
    for (var file : output) assertEncoder(table, file, true);
    assertEquals("", NativeIceberg.liveNativeHandles());
    return output;
  }

  public static void install(StreamExecutionEnvironment env) throws Exception {
    var replace =
        NativeIcebergMaintenance.class.getDeclaredMethod(
            "replace",
            OneInputTransformation.class,
            DataFileRewriteRunner.class,
            boolean.class,
            long.class);
    replace.setAccessible(true);
    var visited =
        java.util.Collections.newSetFromMap(
            new java.util.IdentityHashMap<org.apache.flink.api.dag.Transformation<?>, Boolean>());
    int replaced = 0;
    for (var root : env.getTransformations()) {
      for (var transformation : root.getTransitivePredecessors()) {
        if (!visited.add(transformation)
            || !(transformation instanceof OneInputTransformation<?, ?> input)
            || !(input.getOperatorFactory() instanceof SimpleOperatorFactory<?> factory)
            || !(factory.getOperator() instanceof ProcessOperator<?, ?> operator)
            || operator.getUserFunction().getClass() != DataFileRewriteRunner.class) continue;
        replace.invoke(null, input, operator.getUserFunction(), false, 64L << 20);
        replaced++;
      }
    }
    assertEquals(1, replaced, "Expected the released maintenance rewrite worker");
  }

  public static Set<String> paths(Table table) throws Exception {
    var paths = new java.util.HashSet<String>();
    table.refresh();
    for (var ref : table.refs().keySet()) {
      try (var files = table.newScan().useRef(ref).planFiles()) {
        for (var file : files) paths.add(file.file().location());
      }
    }
    return paths;
  }

  public static void assertNewFiles(Table table, Set<String> before, boolean nativeMode)
      throws Exception {
    table.refresh();
    int created = 0;
    var visited = new java.util.HashSet<String>();
    for (var ref : table.refs().keySet()) {
      try (var files = table.newScan().useRef(ref).planFiles()) {
        for (var task : files) {
          if (!before.contains(task.file().location()) && visited.add(task.file().location())) {
            assertEncoder(
                table, task.file(), nativeMode && task.file().format() == FileFormat.PARQUET);
            created++;
          }
        }
      }
    }
    assertTrue(created > 0, "Expected actual rewritten files");
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  private static void assertEncoder(Table table, DataFile file, boolean nativeMode)
      throws Exception {
    if (file.format() != FileFormat.PARQUET) {
      assertFalse(nativeMode);
      return;
    }
    try (var reader =
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.location()),
                new org.apache.hadoop.conf.Configuration()))) {
      assertEquals(
          nativeMode,
          reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"),
          file.location());
    }
  }
}
