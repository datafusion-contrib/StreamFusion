package tech.streamfusion.operator.batch;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;

/** Transfers one owned native output and retires the temporary handoff reservation. */
@org.apache.flink.annotation.Internal
public final class BoundedArrowOutput {
  private BoundedArrowOutput() {}

  public static ArrowBatch importBatch(long handle, ArrowArray array, Schema schema) {
    VectorSchemaRoot root = null;
    try {
      root = VectorSchemaRoot.create(schema, NativeAllocator.SHARED);
      Data.importIntoVectorSchemaRoot(
          NativeAllocator.SHARED, array, root, NativeAllocator.DICTIONARIES);
      return new ArrowBatch(root);
    } catch (Throwable failure) {
      if (root != null) root.close();
      throw failure;
    } finally {
      org.apache.arrow.c.StreamFusionArrayAccess.releaseIfOpen(array);
      Native.releaseBoundedSorterOutput(handle);
    }
  }
}
