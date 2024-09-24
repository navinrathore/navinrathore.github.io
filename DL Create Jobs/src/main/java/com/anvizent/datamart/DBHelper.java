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
                //String mysqlUrl = "jdbc:mysql://localhost:3306/databasename?noDatetimeStringSync=true";
                String mysqlUrl = "jdbc:mysql://172.25.25.124:4475/Mysql8_2_1009427_appdb?noDatetimeStringSync=true";
                String mysqlUser = "root";
                String mysqlPassword = "Explore@09";
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

    static String getDefaultForDataType(String dataType) {
        dataType = dataType.toLowerCase();
        
        if (dataType.startsWith("varchar") || dataType.startsWith("text") || dataType.startsWith("char")) {
            return "''";
        } else if (dataType.contains("int")) {
            return "0";
        } else if (dataType.contains("float") || dataType.contains("decimal")) {
            return "'0.0'";
        } else if (dataType.contains("boolean")) {
            return "0";
        } else if (dataType.contains("date")) {
            return "'0000-00-00'";
        } else {
            return " "; // Default to empty string for unknown types
        }
    }
    
}