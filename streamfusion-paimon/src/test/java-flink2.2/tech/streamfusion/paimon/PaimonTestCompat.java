package tech.streamfusion.paimon;

import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CoordinatorCommittingRowDataStoreWriteOperator;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.coordinator.CommittingWriteOperatorCoordinator;
import org.apache.paimon.table.FileStoreTable;

/** Test construction at the released coordinator API boundary. */
final class PaimonTestCompat {
  private PaimonTestCompat() {}

  static boolean paimon21() {
    return true;
  }

  static org.apache.paimon.schema.SchemaManager schemaManager(
      org.apache.paimon.fs.FileIO io, org.apache.paimon.fs.Path path) {
    return new org.apache.paimon.schema.FileSystemSchemaManager(io, path);
  }

  static CoordinatorCommittingRowDataStoreWriteOperator operator(
      StreamOperatorParameters<Committable> parameters,
      FileStoreTable table,
      StoreSinkWrite.Provider provider,
      String user,
      OperatorEventGateway gateway) {
    return new CoordinatorCommittingRowDataStoreWriteOperator(
        parameters, table, provider, user, gateway, false);
  }

  static CommittingWriteOperatorCoordinator coordinator(
      FileStoreTable table, MockOperatorCoordinatorContext context, String user) {
    return new CommittingWriteOperatorCoordinator(
        context,
        commitContext ->
            new StoreCommitter(
                table,
                table.newCommit(commitContext.commitUser()).ignoreEmptyCommit(false),
                commitContext),
        true,
        user,
        null);
  }
}
