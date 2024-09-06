package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// Enum to represent different data source types
enum DataSourceType {
    MYSQL,
    SNOWFLAKE,
    SQLSERVER
}

public class DBHelper {

    // Helper function to return a Connection object
    public static Connection getConnection(DataSourceType dataSourceType) throws SQLException {
        Connection connection = null;

        switch (dataSourceType) {
            case MYSQL:
                // MySQL Connection Dummy
                String mysqlUrl = "jdbc:mysql://localhost:3306/databasename?noDatetimeStringSync=true";
                String mysqlUser = "userName";
                String mysqlPassword = "password";
                connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                                
                // Enable auto-commit
                connection.setAutoCommit(true);
                break;
            case SNOWFLAKE:
            case SQLSERVER:
                throw new SQLException(dataSourceType + " is not supported yet.");
            default:
                throw new SQLException("Unsupported DataSourceType: " + dataSourceType);
        }

        return connection;
    }

    public static void main(String[] args) {
        try {
            // Test MySQL connection
            Connection conn = DBHelper.getConnection(DataSourceType.MYSQL);

            if (conn != null) {
                System.out.println("Connection established!");
                conn.close();
            } else {
                System.out.println("Failed to make connection!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Helper function to get the default value based on the data type
    static String getDefaultForDataType(String dataType) {
        switch (dataType.toLowerCase()) {
            case "varchar":
            case "text":
            case "char":
                return "''";
            case "int":
                return "0";
            case "float":
            case "decimal":
                return "0.0";
            case "boolean":
                return "0";
            case "date":
                return "'0000-00-00'";
            default:
                return "''"; // Default to empty string for unknown types
        }
    }
}