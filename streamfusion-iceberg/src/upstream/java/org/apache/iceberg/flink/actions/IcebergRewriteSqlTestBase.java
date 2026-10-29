package org.apache.iceberg.flink.actions;

import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SqlBase;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Hadoop fixtures for the published rewrite SQL assertions, without a Hive/REST server. */
@ExtendWith(ParameterizedTestExtension.class)
abstract class IcebergRewriteSqlTestBase extends SqlBase {
  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @Parameter(index = 0)
  protected boolean nativeMode;

  @Parameter(index = 1)
  protected Namespace baseNamespace;

  @TempDir java.nio.file.Path warehouse;
  protected static final String DATABASE = "db";
  protected String catalogName = "ice";
  protected String flinkDatabase = "ice.db";
  protected Namespace icebergNamespace;
  protected HadoopCatalog validationCatalog;
  private TableEnvironment environment;

  protected static List<Object[]> parameters() {
    return List.of(
        new Object[] {false, Namespace.empty()},
        new Object[] {true, Namespace.empty()},
        new Object[] {false, Namespace.of("l0", "l1")},
        new Object[] {true, Namespace.of("l0", "l1")});
  }

  public void before() {
    validationCatalog =
        new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse.toUri().toString());
    var levels = new java.util.ArrayList<>(List.of(baseNamespace.levels()));
    levels.add(DATABASE);
    icebergNamespace = Namespace.of(levels.toArray(String[]::new));
    String base = baseNamespace.isEmpty() ? "" : ", 'base-namespace'='" + baseNamespace + "'";
    sql(
        "CREATE CATALOG ice WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s'%s)",
        warehouse.toUri(), base);
  }

  public void clean() {
    dropCatalog(catalogName, true);
    try {
      validationCatalog.close();
    } catch (java.io.IOException failure) {
      throw new java.io.UncheckedIOException(failure);
    }
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (environment == null) {
      environment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
      environment
          .getConfig()
          .getConfiguration()
          .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
    }
    return environment;
  }
}
