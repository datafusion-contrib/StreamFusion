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

package io.github.jordepic.streamfusion.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;

/** {@link ArrowFieldWriter} for VarChar. */
@Internal
public abstract class VarCharWriter<T> extends ArrowFieldWriter<T> {

    public static VarCharWriter<RowData> forRow(VarCharVector varCharVector) {
        return new VarCharWriterForRow(varCharVector);
    }

    public static VarCharWriter<ArrayData> forArray(VarCharVector varCharVector) {
        return new VarCharWriterForArray(varCharVector);
    }

    // ------------------------------------------------------------------------------------------

    private VarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract StringData readString(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        VarCharVector vector = (VarCharVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
            return;
        }
        StringData value = readString(in, ordinal);
        // The common case — a single-segment heap BinaryStringData (every string coming out of
        // Flink's row formats) — copies its UTF-8 bytes straight into the Arrow buffer.
        // toBytes() would first copy them into a fresh byte[] only for setSafe to copy again;
        // string columns dominate the entry transpose in the Nexmark profiles, so the doubled
        // copy (and its per-string garbage) was a measurable share of every rowwise-fed query.
        if (value instanceof BinaryStringData) {
            BinaryStringData binary = (BinaryStringData) value;
            // A still-lazy value wraps a java.lang.String (rowwise sources arrive through
            // RowRowConverter). Encoding it with the JDK's intrinsified UTF-8 encoder instead of
            // materializing through Flink's char-at-a-time StringUtf8Utils is byte-identical
            // (Flink's encoder documents JDK-equivalent output and delegates its edge cases to
            // it) and was ~9% of a string-heavy rowwise-fed job in the q20 profile.
            if (binary.getBinarySection() == null) {
                vector.setSafe(
                        getCount(), binary.getJavaObject().getBytes(StandardCharsets.UTF_8));
                return;
            }
            MemorySegment[] segments = binary.getSegments();
            if (segments.length == 1 && !segments[0].isOffHeap()) {
                vector.setSafe(
                        getCount(),
                        segments[0].getArray(),
                        binary.getOffset(),
                        binary.getSizeInBytes());
                return;
            }
        }
        vector.setSafe(getCount(), value.toBytes());
    }

    // ------------------------------------------------------------------------------------------

    /** {@link VarCharWriter} for {@link RowData} input. */
    public static final class VarCharWriterForRow extends VarCharWriter<RowData> {

        private VarCharWriterForRow(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(RowData in, int ordinal) {
            return in.getString(ordinal);
        }
    }

    /** {@link VarCharWriter} for {@link ArrayData} input. */
    public static final class VarCharWriterForArray extends VarCharWriter<ArrayData> {

        private VarCharWriterForArray(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(ArrayData in, int ordinal) {
            return in.getString(ordinal);
        }
    }
}
