package read;

import org.apache.spark.sql.SparkSession;

public class CustomDataSourceRunner {

  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession.builder()
        .appName("data_source_test")
        .config("spark.sql.catalog.s3snapshot",
            "org.opensearch.customdatasource.ossnapshot.OSSnapshotCatalog")
        .config("spark.sql.catalog.s3snapshot.s3.bucket", "reddyvam-os-snapshot")
        .config("spark.sql.catalog.s3snapshot.s3.region", "us-east-1")
        .config("spark.sql.catalog.s3snapshot.s3.access.key", "accesskey")
        .config("spark.sql.catalog.s3snapshot.s3.secret.key",
            "secretKey")
        .config("spark.sql.catalog.s3snapshot.snapshot.name", "testindex_snapshot")
        .config("spark.sql.catalog.s3snapshot.snapshot.base.path", "snaphsots")
        .master("local[*]")
        .getOrCreate();
    SnapshotDataSourceRunner snapshotDataSourceRunner = new SnapshotDataSourceRunner(sparkSession);
    snapshotDataSourceRunner.run();
    System.out.println(sparkSession.sparkContext().isStopped());
    sparkSession.stop();
    checkAndTerminateIfNecessary();
  }

  private static void checkAndTerminateIfNecessary() {
    if (hasNonDaemonThreadsOtherThanMain()) {
      System.out.println("A non-daemon thread in the driver is seen.");
      // Exit the JVM to prevent resource leaks and potential emr-s job hung.
      // A zero status code is used for a graceful shutdown without indicating an error.
      System.exit(0);
    }
  }

  private static boolean hasNonDaemonThreadsOtherThanMain() {
    ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
    while (rootGroup.getParent() != null) {
      rootGroup = rootGroup.getParent();
    }

    Thread[] threads = new Thread[rootGroup.activeCount()];
    rootGroup.enumerate(threads);

    for (Thread thread : threads) {
      if (thread != null && !thread.isDaemon() && !thread.getName().equals("main")) {
        return true;
      }
    }
    return false;
  }
}
