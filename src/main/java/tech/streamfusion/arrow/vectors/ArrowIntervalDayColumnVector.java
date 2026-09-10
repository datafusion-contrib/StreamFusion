/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.columnar.vector.LongColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.IntervalDayVector;

/**
 * Arrow column vector for a day-time INTERVAL carried as Arrow's own {@code Interval(DAY_TIME)}.
 *
 * <p>StreamFusion's canonical form for these is a signed millisecond {@code Int64} — Flink's internal
 * representation — but DataFusion produces a native interval array when an expression's result type
 * is an interval rather than a timestamp. Both encodings therefore reach this boundary, exactly as
 * the four Arrow time encodings do for {@code TIME}.
 */
@Internal
public final class ArrowIntervalDayColumnVector implements LongColumnVector {

  private static final long MILLIS_PER_DAY = 86_400_000L;

  private final IntervalDayVector valueVector;

  public ArrowIntervalDayColumnVector(IntervalDayVector valueVector) {
    this.valueVector = Preconditions.checkNotNull(valueVector);
  }

  @Override
  public long getLong(int i) {
    return IntervalDayVector.getDays(valueVector.getDataBuffer(), i) * MILLIS_PER_DAY
        + IntervalDayVector.getMilliseconds(valueVector.getDataBuffer(), i);
  }

  @Override
  public boolean isNullAt(int i) {
    return valueVector.isNull(i);
  }
}
