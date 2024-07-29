package org.opensearch.customdatasource.jdbc;

import java.sql.SQLException;
import java.util.List;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.opensearch.customdatasource.utils.JdbcUtil;

public class JdbcBatch implements Batch {

  private final StructType schema;

  private final JdbcParams jdbcParams;

  public JdbcBatch(StructType schema, JdbcParams jdbcParams) {
    this.schema = schema;
    this.jdbcParams = jdbcParams;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return createPartitions();
  }

  private InputPartition[] createPartitions() {
    InputPartition[] partitions = new JdbcInputPartition[jdbcParams.getNumPartitions()];
    try {
      List<Integer> partitionColumnValues = JdbcUtil.getPartitionsColumnValues(jdbcParams);
      int bucketSize = (int) Math.ceil(partitionColumnValues.size()*1.0/
          jdbcParams.getNumPartitions());
      int startIndex = 0;
      int endIndex = bucketSize;
      int index = 0;
      String lastHostAddr = null, hostAddr;
      while (startIndex < partitionColumnValues.size() && endIndex <= partitionColumnValues.size()) {
        Integer[] values =
            partitionColumnValues.subList(startIndex, endIndex).toArray(Integer[]::new);
        hostAddr = getLocalityInfo(jdbcParams.getLocalityInfo(), lastHostAddr, index);
        partitions[index++] = new JdbcInputPartition(values, hostAddr);
        lastHostAddr = hostAddr;
        startIndex = endIndex;
        endIndex = Math.min(endIndex+bucketSize, partitionColumnValues.size());
      }
    } catch (SQLException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new JdbcPartitionReaderFactory(schema, jdbcParams);
  }


  private String getLocalityInfo(List<String> localityInfo, String lastHostAddr, int index) {
    if (localityInfo.size() - 1 < index)
      return lastHostAddr;
    else return localityInfo.get(index);
  }
}
