package com.anvizent.datamart;

 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// Enum to represent different data source types
enum DataSourceType {
    MYSQL,
    SNOWFLAKE
}

public class DBHelper {

    // Helper function to return a Connection object
    public static Connection getConnection(DataSourceType dataSourceType) throws SQLException {
        Connection connection = null;

        switch (dataSourceType) {
            case MYSQL:
                // MySQL Connection sample
                String mysqlUrl = "jdbc:mysql://localhost:3306/your_database";
                String mysqlUser = "userName";
                String mysqlPassword = "password";
                connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                                
                // Enable auto-commit
                connection.setAutoCommit(true);
                break;

            default:
                throw new SQLException("Unsupported DataSourceType");
        }

        return connection;
    }

    public static void main(String[] args) {
        try {
            // Get MySQL connection
            Connection conn = DBHelper.getConnection(DataSourceType.MYSQL);

            if (conn != null) {
                System.out.println("Connection established!");
                // Remember to close the connection after usage
                conn.close();
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}