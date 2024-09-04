package org.opensearch.customdatasource.jdbc;

import java.sql.SQLException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class JdbcPartitionReaderFactory implements PartitionReaderFactory {
  private final StructType schema;
  private final JdbcParams jdbcParams;

  public JdbcPartitionReaderFactory(StructType schema, JdbcParams jdbcParams) {
    this.schema = schema;
    this.jdbcParams = jdbcParams;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    try {
      return new JdbcPartitionReader((JdbcInputPartition) partition, schema, jdbcParams);
    } catch (SQLException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }
}
