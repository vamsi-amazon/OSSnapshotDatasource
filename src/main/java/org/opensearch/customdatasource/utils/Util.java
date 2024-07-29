package org.opensearch.customdatasource.utils;

import static org.opensearch.customdatasource.ossnapshot.SnapshotParams.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.customdatasource.constants.Constants;
import org.opensearch.customdatasource.jdbc.JdbcParams;
import org.opensearch.customdatasource.ossnapshot.SnapshotParams;

public class Util {
    public static JdbcParams extractOptions(Map<String, String> options) {
        return new JdbcParams.JdbcParamsBuilder()
                .setTableName(options.get(Constants.TABLE_NAME))
                .setJdbcUrl(options.get(Constants.JDBC_URL))
                .setUserName(options.get(Constants.USER))
                .setPassword(options.get(Constants.PASSWORD))
                .setJdbcDriver(options.get(Constants.JDBC_DRIVER))
                .setPartitioningColumn(options.get(Constants.PARTITIONING_COLUMN))
                .setNumPartitions(
                        options.get(Constants.NUM_PARTITIONS) != null
                                ? Integer.parseInt(options.get(Constants.NUM_PARTITIONS))
                                : 1)
                .setLocalityInfo(getLocalityInfo(options))
                .build();
    }

    private static List<String> getLocalityInfo(Map<String, String> options) {
        String localityInfo = options.get(Constants.LOCALITY_INFO);
        if (localityInfo == null)
            return null;
        String[] localityNodes = localityInfo.split(",");
        return Arrays.asList(localityNodes);
    }

    public static SnapshotParams extractSnapshotParams(CaseInsensitiveStringMap options) {
        return SnapshotParams.builder()
            .snapshotName(options.get(Constants.SNAPSHOT_NAME))
            .s3Bucket(options.get(Constants.S3_BUCKET))
            .s3Region(options.get(Constants.S3_REGION))
            .s3AccessKey(options.get(Constants.S3_ACCESS_KEY))
            .s3SecretKey(options.get(Constants.S3_SECRET_KEY))
            .tableName(options.get(Constants.TABLE_NAME))
            .basePath(options.get(Constants.BASE_PATH))
            .build();
    }
}
