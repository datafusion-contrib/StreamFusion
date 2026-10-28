package tech.streamfusion.planner;

import java.util.List;

/** Keeps optional connector linkage behind the dependency check. */
public final class StarRocksPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    try {
      Class.forName(
          "com.starrocks.connector.flink.table.sink.StarRocksDynamicTableSink",
          false,
          getClass().getClassLoader());
      StarRocksTables.addSubstitutions(entries);
    } catch (ClassNotFoundException | LinkageError unavailable) {
      // The stock connector and its compatible API are optional.
    }
  }
}
