package hive.deepin;

import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author buildupchao
 *         Date: 2019/3/10 18:27
 * @since JDK 1.8
 */
public class HiveStatementExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveStatementExample.class);

    private Connection connection;

    private void init() {
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://master:10000/practice3?auth=noSasl";
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

        HiveStatement statement = (HiveStatement) connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from employees");

        String keywords = "Submitted application";
        String yarnApplicationId = new String();
        System.out.println(statement.getQueryLog());
        for (String queryLog : statement.getQueryLog()) {
            if (queryLog.contains(keywords)) {
                yarnApplicationId = queryLog.substring(queryLog.indexOf(keywords) + keywords.length(), queryLog.length() - 1);
                break;
            }
        }
        LOGGER.info("YARN APPLICATION ID: [{}]", yarnApplicationId);
    }

    public static void main(String[] args) throws SQLException {
        new HiveStatementExample().getApplicationId();
    }
}
