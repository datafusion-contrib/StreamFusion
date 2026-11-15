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

import java.math.BigDecimal;
import java.nio.ByteOrder;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.DecimalColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.DecimalVector;

/** Arrow column vector for DecimalData. */
@Internal
public final class ArrowDecimalColumnVector implements DecimalColumnVector {
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    /** Container which is used to store the sequence of DecimalData values of a column to read. */
    private final DecimalVector decimalVector;

    public ArrowDecimalColumnVector(DecimalVector decimalVector) {
        this.decimalVector = Preconditions.checkNotNull(decimalVector);
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        if (LITTLE_ENDIAN && !decimalVector.isNull(i)) {
            long offset = (long) i * DecimalVector.TYPE_WIDTH;
            long unscaled = decimalVector.getDataBuffer().getLong(offset);
            long high = decimalVector.getDataBuffer().getLong(offset + Long.BYTES);
            // Sign extension proves that reading the low word cannot truncate a wider value.
            if (high == (unscaled >> (Long.SIZE - 1))) {
                return DecimalData.isCompact(precision)
                        ? DecimalData.fromUnscaledLong(unscaled, precision, scale)
                        : DecimalData.fromBigDecimal(
                                BigDecimal.valueOf(unscaled, scale), precision, scale);
            }
        }
        return tech.streamfusion.arrow.DecimalAccessor.fromInternalValue(
                decimalVector.getObject(i), precision, scale);
    }

    @Override
    public boolean isNullAt(int i) {
        return decimalVector.isNull(i);
    }
}
