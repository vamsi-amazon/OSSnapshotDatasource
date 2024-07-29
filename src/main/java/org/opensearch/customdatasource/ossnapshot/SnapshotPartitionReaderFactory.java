package org.opensearch.customdatasource.ossnapshot;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class SnapshotPartitionReaderFactory implements PartitionReaderFactory {
    private final StructType schema;
    private final SnapshotParams snapshotParams;

    public SnapshotPartitionReaderFactory(StructType schema, SnapshotParams snapshotParams) {
        this.schema = schema;
        this.snapshotParams = snapshotParams;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new SnapshotPartitionReader(snapshotParams, schema, (SnapshotInputPartition) partition);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SnapshotPartitionReader", e);
        }
    }
}
