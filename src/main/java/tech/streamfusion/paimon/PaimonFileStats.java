package tech.streamfusion.paimon;

import java.io.IOException;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

@FunctionalInterface
interface PaimonFileStats {
  SimpleColStats[] extract(FileIO io, Path path, long size, Object metadata) throws IOException;
}
