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

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.vector.ArrayColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import tech.streamfusion.arrow.NestedRepresentation;
import tech.streamfusion.arrow.RepresentedArrayData;

/** Arrow column vector for Array. */
@Internal
public final class ArrowArrayColumnVector implements ArrayColumnVector {

    /** Container which is used to store the sequence of array values of a column to read. */
    private final ListVector listVector;

    private final ColumnVector elementVector;
    private final ArrayData.ElementGetter elementGetter;
    private final boolean represented;
    private final ArrayDataSerializer serializer;
    private final TinyIntVector copyClass;

    public ArrowArrayColumnVector(ListVector listVector, ColumnVector elementVector) {
        this(listVector, elementVector, null);
    }

    public ArrowArrayColumnVector(
            ListVector listVector, ColumnVector elementVector, LogicalType elementType) {
        this(listVector, elementVector, elementType, null);
    }

    public ArrowArrayColumnVector(
            ListVector listVector,
            ColumnVector elementVector,
            LogicalType elementType,
            TinyIntVector copyClass) {
        this.listVector = Preconditions.checkNotNull(listVector);
        this.elementVector = Preconditions.checkNotNull(elementVector);
        represented =
                copyClass != null
                        || NestedRepresentation.requiresView(elementVector)
                        || "1".equals(listVector.getField().getMetadata().get(NestedRepresentation.ARRAY));
        elementGetter = elementType == null ? null : ArrayData.createElementGetter(elementType);
        serializer = !represented ? null : new ArrayDataSerializer(elementType);
        this.copyClass = copyClass;
    }

    @Override
    public ArrayData getArray(int i) {
        int index = i * ListVector.OFFSET_WIDTH;
        int start = listVector.getOffsetBuffer().getInt(index);
        int end = listVector.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
        if (!represented) return new ColumnarArrayData(elementVector, start, end - start);
        var slice = new RepresentedArrayData(elementVector, start, end - start);
        if (start < end
                && (copyClass != null && !copyClass.isNull(start) && copyClass.get(start) != 0
                        || NestedRepresentation.binaryArray(elementVector, start)))
            return serializer.toBinaryArray(slice).copy();
        // Flink's array serializer preserves caches for GenericArrayData, but normalizes unknown
        // ArrayData implementations to binary during generated local-reference copies.
        Object[] values = new Object[slice.size()];
        for (int pos = 0; pos < values.length; pos++)
            values[pos] = slice.isNullAt(pos) ? null : elementGetter.getElementOrNull(slice, pos);
        return new GenericArrayData(values);
    }

    @Override
    public boolean isNullAt(int i) {
        return listVector.isNull(i);
    }
}
