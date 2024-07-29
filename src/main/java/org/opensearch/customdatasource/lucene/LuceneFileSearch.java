package org.opensearch.customdatasource.lucene;

import static org.apache.hadoop.shaded.org.apache.kerby.util.HexUtil.bytesToHex;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.INDEX_FILE_PREFIX;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.INDEX_LATEST_BLOB;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.METADATA_NAME_FORMAT;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_NAME_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.Streams;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.util.BytesRefUtils;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.repositories.s3.GenericStatsMetricPublisher;
import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.S3ClientSettings;
import org.opensearch.repositories.s3.S3Service;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.TransferSemaphoresHolder;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

public class LuceneFileSearch {
  private List<Directory> directoryList;

  public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot>
      INDEX_SHARD_SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
      "snapshot",
      SNAPSHOT_NAME_FORMAT,
      BlobStoreIndexShardSnapshot::fromXContent
  );

  public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
      "index-metadata",
      METADATA_NAME_FORMAT,
      IndexMetadata::fromXContent
  );

  public static final ChecksumBlobStoreFormat<SnapshotInfo> SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
      "snapshot",
      SNAPSHOT_NAME_FORMAT,
      SnapshotInfo::fromXContentInternal
  );

  public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
      "metadata",
      METADATA_NAME_FORMAT,
      Metadata::fromXContent
  );

  public LuceneFileSearch()
      throws IOException {
    List<Directory> remoteSnapshotDirectories = new ArrayList<>();
    NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
    String snapshotName = "testindex_snapshot";
    S3BlobStore s3BlobStore = createS3BlobStore();
    final BlobPath snapshotRootPath = new BlobPath()
        .add("snaphsots");
    BlobContainer snapshotRootContainer = s3BlobStore.blobContainer(snapshotRootPath);
    long indexGen = listBlobsToGetLatestIndexId(snapshotRootContainer);
    RepositoryData repositoryData = getRepositoryData(s3BlobStore.blobContainer(snapshotRootPath), indexGen);
    final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds()
        .stream()
        .filter(s -> snapshotName.equals(s.getName()))
        .findFirst();
    final SnapshotId snapshotId = matchingSnapshotId.get();
    SnapshotInfo snapshotInfo = SNAPSHOT_FORMAT.read(snapshotRootContainer, snapshotId.getUUID(), namedXContentRegistry);
    Metadata metadata = GLOBAL_METADATA_FORMAT.read(snapshotRootContainer, snapshotId.getUUID(), namedXContentRegistry);
    System.out.println(snapshotInfo.toString());
    System.out.println(metadata.toString());
    for(String index : snapshotInfo.indices()) {
      IndexId indexId = repositoryData.resolveIndexId(index);
       IndexMetadata indexMetadata = INDEX_METADATA_FORMAT.read(
           s3BlobStore.blobContainer(
               snapshotRootPath
                   .add("indices")
                   .add(indexId.getId())),
          repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId),
          namedXContentRegistry
      );
      System.out.println(indexMetadata.toString());
      for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
        BlobContainer shardContainer = s3BlobStore.blobContainer(
            snapshotRootPath
                .add("indices")
                .add(indexId.getId())
                .add(Integer.toString(i)));
        BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot =
            INDEX_SHARD_SNAPSHOT_FORMAT.read(shardContainer, snapshotId.getUUID(), namedXContentRegistry);
        final FileCache remoteStoreFileCache = FileCacheFactory.createConcurrentLRUFileCache(
            1024 * 1024,
            1,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(PathUtils.get("RemoteLocalStore").resolve(
            String.valueOf(i))));
        TransferManager
            transferManager = new TransferManager(shardContainer, remoteStoreFileCache);
        remoteSnapshotDirectories.add(new RemoteSnapshotDirectory(blobStoreIndexShardSnapshot, localStoreDir, transferManager));
      }
    }
    this.directoryList = remoteSnapshotDirectories;
  }

  private S3BlobStore createS3BlobStore() {
    Path configPath = PathUtils.get("config");
    S3Service s3Service = new S3Service( configPath);
    final Settings.Builder clientSettings = Settings.builder();
    clientSettings.put("s3.client.default.region", "us-east-1");
    final MockSecureSettings secureSettings = new MockSecureSettings();
    secureSettings.setString(
        "s3.client.default.access_key", "accesskey");
    secureSettings.setString(
        "s3.client.default.secret_key", "secretkey");
    clientSettings.setSecureSettings(secureSettings);
    s3Service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build(), configPath));
    SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath.toString()));
    ExecutorService futureCompletionService = Executors.newSingleThreadExecutor();
    ExecutorService streamReaderService = Executors.newSingleThreadExecutor();
    ExecutorService transferQueueConsumerService = Executors.newFixedThreadPool(20);
    AsyncTransferEventLoopGroup transferNIOGroup = new AsyncTransferEventLoopGroup(1);
    GenericStatsMetricPublisher
        genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
    SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
        new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 10L, ByteSizeUnit.GB),
        transferQueueConsumerService,
        10,
        genericStatsMetricPublisher,
        SizeBasedBlockingQ.QueueEventType.NORMAL
    );
    SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
        new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 20L, ByteSizeUnit.GB),
        transferQueueConsumerService,
        5,
        genericStatsMetricPublisher,
        SizeBasedBlockingQ.QueueEventType.NORMAL
    );
    normalPrioritySizeBasedBlockingQ.start();
    lowPrioritySizeBasedBlockingQ.start();
    final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
        "repository",
        "s3",
        Settings.builder().put("client", "default").put("bucket", "reddyvam-os-snapshot").build()
    );
    AsyncExecutorContainer asyncExecutorContainer = new AsyncExecutorContainer(
        futureCompletionService,
        streamReaderService,
        transferNIOGroup
    );
    return new S3BlobStore(
        s3Service,
        null,
        true,
        "reddyvam-os-snapshot",
        false,
        new ByteSizeValue(
            Math.max(
                ByteSizeUnit.MB.toBytes(5), // minimum value
                Math.min(ByteSizeUnit.MB.toBytes(100), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
            ),
            ByteSizeUnit.BYTES
        ),
        "",
        "",
        1000,
        repositoryMetadata,
        new AsyncTransferManager(
            new ByteSizeValue(
            ByteSizeUnit.MB.toBytes(16),
            ByteSizeUnit.BYTES
        ).getBytes(),
            asyncExecutorContainer.getStreamReader(),
            asyncExecutorContainer.getStreamReader(),
            asyncExecutorContainer.getStreamReader(),
            new TransferSemaphoresHolder(
                3,
                Math.max(Runtime.getRuntime().availableProcessors() * 5, 10),
                5,
                TimeUnit.MINUTES,
                genericStatsMetricPublisher
            )
        ),
        asyncExecutorContainer,
        asyncExecutorContainer,
        asyncExecutorContainer,
        normalPrioritySizeBasedBlockingQ,
        lowPrioritySizeBasedBlockingQ,
        genericStatsMetricPublisher
    );
  }

  @NotNull
  private static RepositoryData getRepositoryData(BlobContainer blobContainer, long indexGen)
      throws IOException {
    final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + indexGen;
    // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
    try (
        InputStream blob = blobContainer.readBlob(snapshotsIndexBlobName);
        XContentParser parser = MediaTypeRegistry.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, blob)
    ) {
      return RepositoryData.snapshotsFromXContent(parser, indexGen, true);
    }
  }
  public List<Document> searchFiles() {
    List<Document> documentList = new ArrayList<>();
    for (Directory directory:
         this.directoryList) {
      try {
        Query query = new MatchAllDocsQuery();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        StoredFields storedFields = searcher.storedFields();
        // Create a collector to gather all documents
        List<Document> allDocuments = new ArrayList<>();
        searcher.search(query, new SimpleCollector() {
          private int docBase;

          @Override
          public void collect(int doc) throws IOException {
            allDocuments.add(storedFields.document(doc + docBase));
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            this.docBase = context.docBase;
          }

          @Override
          public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
          }
        });
        for (Document document : allDocuments) {
          // Read _id field
          BytesRef idBytes = document.getBinaryValue("_id");
          String idHex = bytesToHex(idBytes.bytes);
          System.out.println("_id: " + idHex);
          // Read _source field
          BytesRef sourceBytes = document.getBinaryValue("_source");
          String sourceJson = new String(sourceBytes.bytes, StandardCharsets.UTF_8);
          System.out.println(idHex + ":" + sourceJson);
        }
        documentList.addAll(allDocuments);
        indexReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
    return documentList;
  }

  long readSnapshotIndexLatestBlob(BlobContainer blobContainer) throws IOException {
    return BytesRefUtils.bytesToLong(
        Streams.readFully(blobContainer.readBlob(INDEX_LATEST_BLOB)).toBytesRef());
  }

  private long listBlobsToGetLatestIndexId(BlobContainer blobContainer) throws IOException {
    return latestGeneration(blobContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet());
  }

  private long latestGeneration(Collection<String> rootBlobs) {
    long latest = RepositoryData.EMPTY_REPO_GEN;
    for (String blobName : rootBlobs) {
      if (blobName.startsWith(INDEX_FILE_PREFIX) == false) {
        continue;
      }
      try {
        final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
        latest = Math.max(latest, curr);
      } catch (NumberFormatException nfe) {
        // the index- blob wasn't of the format index-N where N is a number,
        // no idea what this blob is but it doesn't belong in the repository!
      }
    }
    return latest;
  }

}
