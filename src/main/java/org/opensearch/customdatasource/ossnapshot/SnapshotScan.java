package org.opensearch.customdatasource.ossnapshot;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

@RequiredArgsConstructor
public class SnapshotScan implements Scan {
    private final StructType schema;
    private final SnapshotParams snapshotParams;
    private final SnapshotTableMetadata snapshotTableMetadata;

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return snapshotParams.getSnapshotName();
    }

    @Override
    public Batch toBatch() {
        return new SnapshotBatch(schema, snapshotParams, snapshotTableMetadata);
    }
}