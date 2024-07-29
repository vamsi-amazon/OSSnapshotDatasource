package org.opensearch.customdatasource.ossnapshot;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.connector.read.InputPartition;

@RequiredArgsConstructor
@Data
public class SnapshotInputPartition implements InputPartition {
    private final String snapshotUUID;
    private final String indexId;
    private final String indexName;
    private final String shardId;
}
