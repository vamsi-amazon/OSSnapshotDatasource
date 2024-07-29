package org.opensearch.customdatasource.ossnapshot;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.SnapshotInfo;

@Data
@AllArgsConstructor
public class SnapshotTableMetadata {
    private final IndexMetadata indexMetadata;
    private final SnapshotInfo snapshotInfo;
    private final IndexId indexId;
}