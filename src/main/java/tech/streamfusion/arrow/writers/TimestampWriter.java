/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.arrow.writers;

import tech.streamfusion.arrow.TimestampAccessor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import org.apache.arrow.vector.ValueVector;

/** {@link ArrowFieldWriter} for Timestamp. */
@Internal
public abstract class TimestampWriter<T> extends ArrowFieldWriter<T> {

    public static TimestampWriter<RowData> forRow(ValueVector valueVector, int precision) {
        return new TimestampWriterForRow(valueVector, precision);
    }

    public static TimestampWriter<ArrayData> forArray(ValueVector valueVector, int precision) {
        return new TimestampWriterForArray(valueVector, precision);
    }

    // ------------------------------------------------------------------------------------------

    protected final int precision;
    private final TimestampAccessor accessor;

    private TimestampWriter(ValueVector valueVector, int precision) {
        super(valueVector);
        accessor = new TimestampAccessor(valueVector);
        this.precision = precision;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract TimestampData readTimestamp(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        accessor.set(getCount(), isNullAt(in, ordinal) ? null : readTimestamp(in, ordinal));
    }

    // ------------------------------------------------------------------------------------------

    /** {@link TimestampWriter} for {@link RowData} input. */
    public static final class TimestampWriterForRow extends TimestampWriter<RowData> {

        private TimestampWriterForRow(ValueVector valueVector, int precision) {
            super(valueVector, precision);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        TimestampData readTimestamp(RowData in, int ordinal) {
            return in.getTimestamp(ordinal, precision);
        }
    }

    /** {@link TimestampWriter} for {@link ArrayData} input. */
    public static final class TimestampWriterForArray extends TimestampWriter<ArrayData> {

        private TimestampWriterForArray(ValueVector valueVector, int precision) {
            super(valueVector, precision);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        TimestampData readTimestamp(ArrayData in, int ordinal) {
            return in.getTimestamp(ordinal, precision);
        }
    }
}
