package org.opensearch.customdatasource.ossnapshot;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class SnapshotParams implements Serializable {
    private final String snapshotName;
    private final String s3Bucket;
    private final String basePath;
    private final String s3Region;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String tableName;
}
