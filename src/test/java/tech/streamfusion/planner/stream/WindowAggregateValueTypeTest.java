package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class WindowAggregateValueTypeTest {
  @Test
  void stringTypesAdmitExtremaAndValidityCounts() {
    for (var type : new SqlTypeName[] {SqlTypeName.CHAR, SqlTypeName.VARCHAR}) {
      assertTrue(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MIN, type));
      assertTrue(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MAX, type));
      assertFalse(WindowAggregateMatcher.supportedValueType(type));
      assertTrue(WindowAggregateMatcher.supportedAggregateValueType(
          WindowAggregateMatcher.KIND_COUNT, type));
      for (int kind :
          new int[] {
            WindowAggregateMatcher.KIND_SUM,
            WindowAggregateMatcher.KIND_AVG
          }) {
        assertFalse(WindowAggregateMatcher.supportedAggregateValueType(kind, type));
      }
    }
  }

  @Test
  void timestampTypesAdmitExtremaAndValidityCounts() {
    for (var type :
        new SqlTypeName[] {SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE}) {
      assertTrue(WindowAggregateMatcher.supportedAggregateValueType(
          WindowAggregateMatcher.KIND_MIN, type));
      assertTrue(WindowAggregateMatcher.supportedAggregateValueType(
          WindowAggregateMatcher.KIND_MAX, type));
      assertTrue(WindowAggregateMatcher.supportedAggregateValueType(
          WindowAggregateMatcher.KIND_COUNT, type));
      for (int kind : new int[] {WindowAggregateMatcher.KIND_SUM,
          WindowAggregateMatcher.KIND_SUM_ZERO, WindowAggregateMatcher.KIND_AVG}) {
        assertFalse(WindowAggregateMatcher.supportedAggregateValueType(kind, type));
      }
    }
  }

  @Test
  void extremaDoNotAdmitUnverifiedTypes() {
    for (var type :
        new SqlTypeName[] {
          SqlTypeName.BINARY,
          SqlTypeName.ARRAY,
          SqlTypeName.MAP
        }) {
      assertFalse(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MIN, type));
      assertFalse(
          WindowAggregateMatcher.supportedAggregateValueType(
              WindowAggregateMatcher.KIND_MAX, type));
    }
  }

  @Test
  void dateTimeAndBooleanAdmitExtremaAndCounts() {
    for (var type : new SqlTypeName[] {SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.BOOLEAN}) {
      for (int kind : new int[] {WindowAggregateMatcher.KIND_MIN, WindowAggregateMatcher.KIND_MAX,
          WindowAggregateMatcher.KIND_COUNT}) {
        assertTrue(WindowAggregateMatcher.supportedAggregateValueType(kind, type));
      }
      for (int kind : new int[] {WindowAggregateMatcher.KIND_SUM, WindowAggregateMatcher.KIND_AVG}) {
        assertFalse(WindowAggregateMatcher.supportedAggregateValueType(kind, type));
      }
    }
  }
}
