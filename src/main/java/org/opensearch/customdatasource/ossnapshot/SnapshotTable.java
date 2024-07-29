package org.opensearch.customdatasource.ossnapshot;

import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@RequiredArgsConstructor
public class SnapshotTable implements SupportsRead {

  private final StructType schema;
  private final SnapshotParams snapshotParams;
  private final SnapshotTableMetadata snapshotTableMetadata;
  @Override
  public String name() {
    return this.snapshotParams.getTableName();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SnapshotScanBuilder(schema, snapshotParams, snapshotTableMetadata);
  }
}
