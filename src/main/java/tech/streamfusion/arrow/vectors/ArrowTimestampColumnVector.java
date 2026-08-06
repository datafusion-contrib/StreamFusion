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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Arrow column vector for Timestamp. */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    /** Container which is used to store the sequence of timestamp values of a column to read. */
    private final ValueVector valueVector;

    public ArrowTimestampColumnVector(ValueVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        Preconditions.checkState(valueVector instanceof TimeStampVector);
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        TimeStampVector timestamps = (TimeStampVector) valueVector;
        long value = timestamps.get(i);
        TimeUnit unit = ((ArrowType.Timestamp) timestamps.getField().getType()).getUnit();
        switch (unit) {
            case SECOND:
                return TimestampData.fromEpochMillis(value * 1000);
            case MILLISECOND:
                return TimestampData.fromEpochMillis(value);
            case MICROSECOND:
                return TimestampData.fromEpochMillis(
                        Math.floorDiv(value, 1000), (int) Math.floorMod(value, 1000) * 1000);
            case NANOSECOND:
                return TimestampData.fromEpochMillis(
                        Math.floorDiv(value, 1_000_000), (int) Math.floorMod(value, 1_000_000));
            default:
                throw new IllegalStateException("Unsupported Arrow timestamp unit " + unit);
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
