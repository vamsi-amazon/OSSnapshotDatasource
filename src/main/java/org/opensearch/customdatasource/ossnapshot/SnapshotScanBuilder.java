package org.opensearch.customdatasource.ossnapshot;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

@RequiredArgsConstructor
public class SnapshotScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final SnapshotParams snapshotParams;
    private final SnapshotTableMetadata snapshotTableMetadata;
    @Override
    public Scan build() {
        return new SnapshotScan(schema, snapshotParams, snapshotTableMetadata);
    }
}
