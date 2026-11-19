package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

class ArrowRowTypeSupportTest {
  @Test
  void mapsAreRejectedAtEveryNestingLevel() {
    var map = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
    assertFalse(ArrowRowTypeSupport.supports(map.getLogicalType()));
    assertFalse(ArrowRowTypeSupport.supports(DataTypes.ARRAY(map).getLogicalType()));
    assertFalse(ArrowRowTypeSupport.supports(DataTypes.ROW(DataTypes.FIELD("m", map)).getLogicalType()));
    assertFalse(ArrowRowTypeSupport.supports(DataTypes.MULTISET(DataTypes.INT()).getLogicalType()));
    assertTrue(ArrowRowTypeSupport.supports(DataTypes.ARRAY(DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("s", DataTypes.STRING()))).getLogicalType()));
  }
}
