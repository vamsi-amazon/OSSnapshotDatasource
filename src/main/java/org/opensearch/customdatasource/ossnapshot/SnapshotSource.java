package org.opensearch.customdatasource.ossnapshot;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.customdatasource.utils.SnapshotUtil;
import org.opensearch.customdatasource.utils.Util;
import org.opensearch.repositories.s3.S3BlobStore;

public class SnapshotSource implements TableProvider {
  private SnapshotParams snapshotParams;
  private StructType schema;
  private SnapshotTableMetadata snapshotTableMetadata;

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    this.snapshotParams = Util.extractSnapshotParams(options);
    try(S3BlobStore s3BlobStore = SnapshotUtil.createS3BlobStore(snapshotParams)) {
      this.snapshotTableMetadata =
          SnapshotUtil.getSnapshotTableMetadata(s3BlobStore, snapshotParams);
      this.schema =
          SnapshotUtil.createSparkSchema(snapshotTableMetadata.getIndexMetadata().mapping());
      return this.schema;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning,
                        Map<String, String> properties) {
    return new SnapshotTable(schema, snapshotParams, snapshotTableMetadata);
  }

}
