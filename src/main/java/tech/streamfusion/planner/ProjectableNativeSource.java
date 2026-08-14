package tech.streamfusion.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/** Optional native source capability for pushing a Calc's projected schema into decoding. */
interface ProjectableNativeSource {

  boolean supportsProjection(RelDataType projectedType);

  RelNode withProjection(RelDataType projectedType);
}
