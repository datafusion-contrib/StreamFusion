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
import org.apache.arrow.vector.ValueVector;
import tech.streamfusion.arrow.TimestampAccessor;

/** Arrow column vector for Timestamp. */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    private final TimestampAccessor timestamps;

    public ArrowTimestampColumnVector(ValueVector valueVector) {
        this.timestamps = new TimestampAccessor(valueVector);
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        return timestamps.getTimestamp(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return timestamps.isNull(i);
    }
}
