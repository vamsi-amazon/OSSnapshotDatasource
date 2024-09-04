package org.opensearch.customdatasource.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.opensearch.customdatasource.utils.JdbcUtil;
import scala.collection.JavaConverters;

public class JdbcPartitionReader implements PartitionReader<InternalRow> {

  private final JdbcInputPartition jdbcInputPartition;
  private final JdbcParams jdbcParams;
  private ResultSet resultSet;
  private List<BiFunction> valueConverters;

  public JdbcPartitionReader(
      JdbcInputPartition jdbcInputPartition,
      StructType schema,
      JdbcParams jdbcParams) throws SQLException, ClassNotFoundException {
    this.jdbcInputPartition = jdbcInputPartition;
    this.valueConverters = ValueConverters.getConverters(schema);
    this.jdbcParams = jdbcParams;
    this.createJdbcReader();
  }


  private void createJdbcReader() throws SQLException, ClassNotFoundException {
    resultSet = JdbcUtil.readPartitionData(jdbcParams, jdbcInputPartition.getValues());
  }

  public boolean next() {
    try {
      return resultSet.next();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public InternalRow get() {
    Object[] convertedValues = new Object[valueConverters.size()];
    for (int i = 0; i < valueConverters.size(); i++) {
      Object value = valueConverters.get(i).apply(resultSet, i+1);
      convertedValues[i] = value;
    }
    return InternalRow.apply(
        JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());
  }

  @Override
  public void close(){
    try {
      resultSet.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
