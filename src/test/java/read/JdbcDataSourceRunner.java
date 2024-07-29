package read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import org.opensearch.customdatasource.constants.Constants;
import org.opensearch.customdatasource.jdbc.JdbcParams;
import org.opensearch.customdatasource.utils.JdbcUtil;
import org.opensearch.customdatasource.utils.TestDataCreator;

public class JdbcDataSourceRunner implements Runnable {

    private final SparkSession sparkSession;

    JdbcDataSourceRunner(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void run() {
        JdbcParams jdbcParams = null;
        try {
            jdbcParams = preRunSetup();
            Dataset<Row> dataset = sparkSession.read()
                    .format("org.opensearch.customdatasource.jdbc.JdbcSource")
                    .option(Constants.TABLE_NAME, jdbcParams.getTableName())
                    .option(Constants.JDBC_URL, jdbcParams.getJdbcUrl())
                    .option(Constants.USER, jdbcParams.getUserName())
                    .option(Constants.PASSWORD, jdbcParams.getPassword())
                    .option(Constants.JDBC_DRIVER, jdbcParams.getJdbcDriver())
                    .option(Constants.PARTITIONING_COLUMN, "id")
                    .option(Constants.NUM_PARTITIONS, "2")
                    .option(Constants.LOCALITY_INFO, "127.0.0.1")
                    .load();
            dataset.show();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (jdbcParams != null)
                JdbcUtil.dropTable(jdbcParams);
        }
    }

    private JdbcParams preRunSetup() throws SQLException, ClassNotFoundException {
        JdbcParams jdbcParams = new JdbcParams.JdbcParamsBuilder()
                .setTableName("SALES_DATA")
                .setJdbcDriver("org.h2.Driver")
                .setJdbcUrl("jdbc:h2:~/test")
                .setPassword("")
                .setUserName("sa")
                .build();

        TestDataCreator.createTestData(jdbcParams);
        return jdbcParams;
    }
}
