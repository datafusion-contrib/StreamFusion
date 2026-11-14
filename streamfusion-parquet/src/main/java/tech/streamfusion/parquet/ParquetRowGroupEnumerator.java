package tech.streamfusion.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;

/** Stock recursive discovery with row-group splits even on filesystems without block locations. */
public final class ParquetRowGroupEnumerator extends NonSplittingRecursiveEnumerator {
  @Override
  protected void convertToSourceSplits(FileStatus file, FileSystem fs, List<FileSourceSplit> target)
      throws IOException {
    var whole = new ArrayList<FileSourceSplit>(1);
    super.convertToSourceSplits(file, fs, whole);
    long[] offsets;
    try (var input = new ParquetInputFile(file.getPath())) {
      offsets = NativeParquet.parquetRowGroupOffsets(input, file.getLen());
    }
    if (offsets.length == 0) {
      target.addAll(whole);
      return;
    }
    for (int index = 0; index < offsets.length; index++) {
      long start = index == 0 ? 0 : offsets[index];
      long end = index + 1 == offsets.length ? file.getLen() : offsets[index + 1];
      target.add(
          new FileSourceSplit(
              getNextId(),
              file.getPath(),
              start,
              end - start,
              file.getModificationTime(),
              file.getLen(),
              whole.get(0).hostnames()));
    }
  }
}
