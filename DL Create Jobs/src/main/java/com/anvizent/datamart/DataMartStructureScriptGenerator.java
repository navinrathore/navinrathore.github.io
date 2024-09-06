package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DataMartStructureScriptGenerator {
    private DataSourceType dataSourceType;
    private String dlId;
    private String dlName;
    private String jobId;

    SQLQueries sqlQueries;
    Connection conn;

    public DataMartStructureScriptGenerator(DataSourceType type, String dlId) {
        this.dataSourceType = type;
        this.dlId = dlId;
        init();
    }

    private void init() {
        sqlQueries = new SQLQueries(); // Creating instance of SQLQueries inner class
        try {
            // App DB connection
            conn = DBHelper.getConnection(dataSourceType);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public String getDlId() {
        return dlId;
    }

    // Method to trigger generation of different scripts
    public void generateScripts() {
        // The config script generator
        if (new DataMartConfigScriptGenerator().generateConfigScript() != Status.SUCCESS ) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }
        // The value script generator
        if (new DataMartValueScriptGenerator().generateValueScript() != Status.SUCCESS) {
            System.out.println("Value script generation failed. Stopping process.");
            return;
        }
        // The create script generator
        if (new DataMartCreateScriptGenerator().generateCreateScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }
        // The alter script generator
        if (new DataMartAlterScriptGenerator().generateAlterScript() != Status.SUCCESS) {
            System.out.println("Alter script generation failed. Stopping process.");
            return;
        }
        // The saved script generator
        if (new DataMartSavedScriptGenerator().generateSavedScript() != Status.SUCCESS) {
            System.out.println("Saved script generation failed. Stopping process.");
            return;
        }

        // If all the above operations are successful, delete the following table
        executeDropTableQuery(conn, dlName, dlId, jobId);
    }

    public class DataMartConfigScriptGenerator {
        public DataMartConfigScriptGenerator() {
         }

        public Status generateConfigScript() {
            System.out.println("Generating config script for DL_ID: " + dlId);
            return Status.SUCCESS;
         }
    }

    public class DataMartValueScriptGenerator {
        public DataMartValueScriptGenerator() {
         }

        public Status generateValueScript() {
            System.out.println("Generating value script for DL_ID: " + dlId);
            return Status.SUCCESS;
         }
    }

    public class DataMartAlterScriptGenerator {
        public DataMartAlterScriptGenerator() {
         }

        public Status generateAlterScript() {
            System.out.println("Generating alter script for DL_ID: " + dlId);
            return Status.SUCCESS;
         }
    }

    public class DataMartSavedScriptGenerator {
        public DataMartSavedScriptGenerator() {
         }

        public Status generateSavedScript() {
            System.out.println("Generating saved script for DL_ID: " + dlId);
            return Status.SUCCESS;
         }
    }

    public class DataMartCreateScriptGenerator {
        public DataMartCreateScriptGenerator() {
        }

        public Status generateCreateScript() {
            System.out.println("Generating create script for DL_ID: " + dlId);
            getDataSourceType();
            dataSourceType = DataSourceType.MYSQL;
            getDlId();
            return Status.SUCCESS;
         }
    }

    public static class SQLQueries {
        // Query to drop a table
        public String getDropTableQuery(String dlName, String dlId, String jobId) {
            return "DROP TABLE IF EXISTS `" + dlName + dlId + jobId + "`";
        }
    }

    // Enum to represent different data source types
    enum DataSourceType {
        MYSQL,
        SNOWFLAKE,
        SQLSERVER
    }
    // Enum to represent different return status
    public enum Status {
        SUCCESS,
        FAILURE
    }
    static public class DBHelper {
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

        // Helper function to get the default value based on the data type
        public String getDefaultForDataType(String dataType) {
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

    private void executeDropTableQuery(Connection conn, String dlName, String dlId, String jobId) {
        String sql = sqlQueries.getDropTableQuery(dlName, dlId, jobId);
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Table dropped: " + dlName + dlId + jobId);
        } catch (SQLException e) {
            System.err.println("Error executing drop table query: " + e.getMessage());
        }
    }
}
