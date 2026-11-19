package org.example.canonical;

import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.table.data.RowData;
import org.example.canonical.CanonicalFixtureFactory.WriterState;

interface CanonicalWriterCompat
    extends StatefulSinkWriter<RowData, WriterState>, CommittingSinkWriter<RowData, WriterState> {}
