package tech.streamfusion.planner;

import java.util.List;

/** Keeps optional connector linkage behind the dependency check. */
public final class DorisPlannerExtension implements NativePlannerExtension {
  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    try {
      Class.forName(
          "org.apache.doris.flink.table.DorisDynamicTableSource",
          false,
          getClass().getClassLoader());
      Class.forName(
          "org.apache.doris.flink.table.DorisDynamicTableSink", false, getClass().getClassLoader());
      DorisTables.addSubstitutions(entries);
    } catch (ClassNotFoundException | LinkageError unavailable) {
      // The stock connector and its compatible API are optional.
    }
  }
}
