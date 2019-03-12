package hive.deepin;

import com.google.common.collect.Queues;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author buildupchao
 *         Date: 2019/3/10 18:27
 * @since JDK 1.8
 */
public class HiveStatementExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveStatementExample.class);

    private static final ExecutorService HIVE_LOG_HANLDERS = new ThreadPoolExecutor(
            5,
            5,
            0,
            TimeUnit.SECONDS,
            Queues.newArrayBlockingQueue(5),
            new BasicThreadFactory.Builder().namingPattern("hive-log-handler-%d").build(),
            new ThreadPoolExecutor.AbortPolicy()
    );

    private Connection connection;

    private void init() {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://master:10000/db1?auth=noSasl";
        String user = "jangz";
        String password = "jangz";
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getApplicationId() throws SQLException {
        init();

        HiveStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = (HiveStatement) connection.createStatement();
            System.out.println("statement: " + statement.toString());
            HIVE_LOG_HANLDERS.execute(new HiveLogHandler(statement));
            resultSet = statement.executeQuery("select count(*) from record");
            System.out.println("DONE>>>>>>>>>>>");
        } finally {
            close(connection, statement, resultSet);
            HIVE_LOG_HANLDERS.shutdown();
        }
    }

    public void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException ex) {
            LOGGER.error("Close resource error!");
        }
    }

    static class HiveLogHandler implements Runnable {

        private HiveStatement hiveStatement;

        public HiveLogHandler(HiveStatement hiveStatement) {
            this.hiveStatement = hiveStatement;
        }

        @Override
        public void run() {
            try {
                String keywords = "application_";
                String yarnApplicationId = new String();

                while (hiveStatement.hasMoreLogs()) {
                    for (String queryLog : hiveStatement.getQueryLog(true, 100)) {
//                            if (queryLog.contains(keywords)) {
                        yarnApplicationId = queryLog;
//                            yarnApplicationId = queryLog.substring(queryLog.indexOf(keywords) + keywords.length(), queryLog.length() - 1);

                        System.out.println(queryLog);
                        break;
//                            }
                    }
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
//               LOGGER.info("YARN APPLICATION ID: [{}]", yarnApplicationId);
            } catch (SQLException ex) {
                LOGGER.info("Cannot get YARN APPLICATION ID.");
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        new HiveStatementExample().getApplicationId();
    }
}
