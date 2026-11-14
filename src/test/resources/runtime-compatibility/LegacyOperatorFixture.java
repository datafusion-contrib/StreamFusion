import java.io.*;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.RowDataToArrowOperator;

public final class LegacyOperatorFixture {
  public static void main(String[] args) throws Exception {
    Class<?> parent = Class.forName("tech.streamfusion.compat.FlinkStreamOperator");
    if (ObjectStreamClass.lookup(parent).getFields().length != 0)
      throw new AssertionError("not the legacy superclass");
    var operator = new RowDataToArrowOperator(RowType.of(new IntType()), 1024, false, null);
    try (var output = new ObjectOutputStream(new FileOutputStream(args[0]))) {
      output.writeObject(operator);
    }
  }
}
