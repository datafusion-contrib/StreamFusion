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

/**
 * Arrow ↔ {@code RowData} conversion, vendored from Apache Flink's {@code flink-python} module
 * (package {@code org.apache.flink.table.runtime.arrow}, Apache-2.0). The per-type column vectors and
 * field writers are copied verbatim (only repackaged); {@link
 * tech.streamfusion.arrow.ArrowConversion} is a trimmed extract of {@code ArrowUtils}'s
 * schema/reader/writer factories, with timestamps pinned to nanoseconds to match the native side.
 *
 * <p>Vendored rather than depended on because the upstream classes live in {@code flink-python}, which a
 * standard Java Flink deployment ships under {@code opt/} (not on the runtime classpath). They depend
 * only on {@code flink-table-runtime}'s columnar interfaces and Arrow, both already on the classpath.
 */
package tech.streamfusion.arrow;
