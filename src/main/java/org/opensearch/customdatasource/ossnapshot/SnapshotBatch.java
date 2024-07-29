package org.opensearch.customdatasource.ossnapshot;

import java.util.stream.IntStream;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.SnapshotInfo;

public class SnapshotBatch implements Batch {

  private final StructType schema;
  private final SnapshotParams snapshotParams;
  private final SnapshotTableMetadata snapshotTableMetadata;

  public SnapshotBatch(StructType schema, SnapshotParams snapshotParams,
                       SnapshotTableMetadata snapshotTableMetadata) {
    this.schema = schema;
    this.snapshotParams = snapshotParams;
    this.snapshotTableMetadata = snapshotTableMetadata;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    IndexMetadata indexMetadata = snapshotTableMetadata.getIndexMetadata();
    SnapshotInfo snapshotInfo = snapshotTableMetadata.getSnapshotInfo();
    IndexId indexId = snapshotTableMetadata.getIndexId();
    return IntStream.range(0, indexMetadata.getNumberOfShards())
        .mapToObj(
            i -> new SnapshotInputPartition(snapshotInfo.snapshotId().getUUID(), indexId.getId(),
                indexId.getName(), Integer.toString(i)))
        .toArray(InputPartition[]::new);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SnapshotPartitionReaderFactory(schema, snapshotParams);
  }

}