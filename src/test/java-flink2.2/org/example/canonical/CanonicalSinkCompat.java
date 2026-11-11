package org.example.canonical;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.example.canonical.CanonicalFixtureFactory.*;

abstract class CanonicalSinkCompat
    implements Sink<RowData>,
        SupportsWriterState<RowData, WriterState>,
        SupportsCommitter<WriterState> {
  protected abstract RowsWriter writer(Collection<WriterState> restored) throws IOException;

  protected abstract RowsCommitter committer();

  @Override
  public RowsWriter createWriter(WriterInitContext context) throws IOException {
    return writer(List.of());
  }

  @Override
  public RowsWriter restoreWriter(WriterInitContext context, Collection<WriterState> restored)
      throws IOException {
    return writer(restored);
  }

  @Override
  public SimpleVersionedSerializer<WriterState> getWriterStateSerializer() {
    return new RowsSerializer();
  }

  @Override
  public SimpleVersionedSerializer<WriterState> getCommittableSerializer() {
    return new RowsSerializer();
  }

  @Override
  public Committer<WriterState> createCommitter(CommitterInitContext context) {
    return committer();
  }
}
