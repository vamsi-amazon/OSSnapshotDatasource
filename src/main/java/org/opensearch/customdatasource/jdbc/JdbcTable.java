package org.opensearch.customdatasource.jdbc;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class JdbcTable implements SupportsRead {

  private final StructType schema;
  private final JdbcParams jdbcParams;

  public JdbcTable(StructType schema, JdbcParams jdbcParams) {
    this.schema  = schema;
    this.jdbcParams = jdbcParams;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new JdbcScanBuilder(schema, jdbcParams);
  }

  @Override
  public String name() {
    return jdbcParams.getTableName();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of(TableCapability.BATCH_READ);
  }
}
