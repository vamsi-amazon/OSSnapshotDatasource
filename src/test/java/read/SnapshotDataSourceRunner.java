package read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opensearch.customdatasource.constants.Constants;

public class SnapshotDataSourceRunner implements Runnable {

  private final SparkSession sparkSession;

  SnapshotDataSourceRunner(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public void run() {
    try {
      Dataset<Row> dataset = sparkSession.read()
          .format("org.opensearch.customdatasource.ossnapshot.SnapshotSource")
          .option(Constants.SNAPSHOT_NAME, "testindex_snapshot")
          .option(Constants.BASE_PATH, "snaphsots")
          .option(Constants.S3_BUCKET, "reddyvam-os-snapshot")
          .option(Constants.S3_REGION, "us-east-1")
          .option(Constants.S3_ACCESS_KEY, "")
          .option(Constants.S3_SECRET_KEY, "")
          .option(Constants.TABLE_NAME, "testindex")
          .load();
      dataset.show(1000);

      String tableName = "s3snapshot.default.testindex";

      Dataset<Row> datasetSQL = sparkSession.sql("SELECT * FROM " + tableName);
      datasetSQL.show(10);

      System.out.println("1. Average price by category:");
      Dataset<Row> avgPriceByCategory = sparkSession.sql(
          "SELECT category, AVG(price) as avg_price " +
              "FROM " + tableName + " " +
              "GROUP BY category"
      );
      avgPriceByCategory.show();

      // 2. Total quantity by category
      System.out.println("2. Total quantity by category:");
      Dataset<Row> totalQuantityByCategory = sparkSession.sql(
          "SELECT category, SUM(quantity) as total_quantity " +
              "FROM " + tableName + " " +
              "GROUP BY category"
      );
      totalQuantityByCategory.show();

      // 3. Number of products and average rating by category
      System.out.println("3. Number of products and average rating by category:");
      Dataset<Row> productCountAndAvgRating = sparkSession.sql(
          "SELECT category, COUNT(*) as product_count, AVG(rating) as avg_rating " +
              "FROM " + tableName + " " +
              "GROUP BY category"
      );
      productCountAndAvgRating.show();

      // 4. Top 5 categories by total views
      System.out.println("4. Top 5 categories by total views:");
      Dataset<Row> topCategoriesByViews = sparkSession.sql(
          "SELECT category, SUM(views) as total_views " +
              "FROM " + tableName + " " +
              "GROUP BY category " +
              "ORDER BY total_views DESC " +
              "LIMIT 5"
      );
      topCategoriesByViews.show();

      // 5. Average price of products with rating above 4
      System.out.println("5. Average price of products with rating above 4:");
      Dataset<Row> avgPriceHighRated = sparkSession.sql(
          "SELECT AVG(price) as avg_high_rated_price " +
              "FROM " + tableName + " " +
              "WHERE rating > 4"
      );
      avgPriceHighRated.show();

      // 6. Number of products created per month
      System.out.println("6. Number of products created per month:");
      Dataset<Row> productsByMonth = sparkSession.sql(
          "SELECT DATE_TRUNC('month', created_at) as month, COUNT(*) as product_count " +
              "FROM " + tableName + " " +
              "GROUP BY DATE_TRUNC('month', created_at) " +
              "ORDER BY month"
      );
      productsByMonth.show();

      // 7. Distribution of products by price ranges
      System.out.println("7. Distribution of products by price ranges:");
      Dataset<Row> productsByPriceRange = sparkSession.sql(
          "SELECT " +
              "  CASE " +
              "    WHEN price < 10 THEN 'Under $10' " +
              "    WHEN price >= 10 AND price < 50 THEN '$10 - $49.99' " +
              "    WHEN price >= 50 AND price < 100 THEN '$50 - $99.99' " +
              "    ELSE '$100 and above' " +
              "  END as price_range, " +
              "  COUNT(*) as product_count " +
              "FROM " + tableName + " " +
              "GROUP BY price_range " +
              "ORDER BY price_range"
      );
      productsByPriceRange.show();

      // 1. Basic LIKE query
      System.out.println("1. Products with 'sports' in the name:");
      Dataset<Row> sportsProducts = sparkSession.sql(
          "SELECT name, category, price " +
              "FROM " + tableName + " " +
              "WHERE LOWER(name) LIKE '%sports%'"
      );
      sportsProducts.show();

      // 2. Multiple conditions with OR
      System.out.println("2. Products with 'sports' or 'outdoor' in the name:");
      Dataset<Row> sportsOrOutdoorProducts = sparkSession.sql(
          "SELECT name, category, price " +
              "FROM " + tableName + " " +
              "WHERE LOWER(name) LIKE '%sports%' OR LOWER(name) LIKE '%outdoor%'"
      );
      sportsOrOutdoorProducts.show();


      Dataset<Row> describeSQL = sparkSession.sql("Describe " + tableName);
      describeSQL.show();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
