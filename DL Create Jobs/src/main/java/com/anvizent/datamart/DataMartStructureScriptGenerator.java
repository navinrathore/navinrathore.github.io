package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

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
            boolean status = false;
            System.out.println("Generating alter script for DL_ID: " + dlId);
            // Step 1:
            status = updateActiveFlag(conn, dlId);
            // Format of the dateTime is supposed to be "yyyy-MM-dd HH:mm:ss", Hence TimeStamp is used.
            Timestamp dateTime = getMaxUpdatedDate(conn, dlId);
            boolean dlIdExists = checkDLIdExists(conn, dlId);
            // Step 2:
            if (dlIdExists) {
                // call Alter delete funcitons
                String dlName = ""; // Input Param
                String TargetDB = ""; // Input Param
                boolean tableExists = doesTableExist(conn, dlName, TargetDB);
                if (tableExists) {
                    System.out.println("Table Exists. Execute Alter Statement.");
                    status = fetchUniqueMappingInfoAndInsertIntoAlterScriptInfo(conn, dlId);
                } else {
                    System.out.println("Table Doesn't Exists. Proceed with Create Statement.");
                    // TBD: This case is not specified properly. Is it redundant?
                }
            }
            // Step 3:
            boolean hasRecentUpdateForDLId = checkDLIdExistsWithUpdatedDate(conn, dlId, dateTime);
            if (hasRecentUpdateForDLId) {
                // call Alter Services

                buildChangeColumnNotNullQuery(conn, dlId, dlName);
            }

            return Status.SUCCESS;
        }

        private boolean updateActiveFlag(Connection conn, String dlId) {
            String updateQuery = SQLQueries.UPDATE_ACTIVE_FLAG;
            try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                stmt.setString(1, dlId);
                return stmt.executeUpdate() > 0;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        private Timestamp getMaxUpdatedDate(Connection conn, String dlId) {
            String selectQuery = SQLQueries.SELECT_MAX_UPDATED_DATE;
            try (PreparedStatement stmt = conn.prepareStatement(selectQuery)) {
                stmt.setString(1, dlId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getTimestamp(1);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }

        private boolean checkDLIdExists(Connection conn, String dlId) {
            String checkQuery = "SELECT 1 FROM ELT_DL_Mapping_Info_Saved WHERE DL_Id = ? LIMIT 1";
            try (PreparedStatement stmt = conn.prepareStatement(checkQuery)) {
                stmt.setString(1, dlId);
                try (ResultSet rs = stmt.executeQuery()) {
                    return rs.next();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Function to check if a DL_Id exists with Updated_Date greater than a specified value
        private boolean checkDLIdExistsWithUpdatedDate(Connection conn, String dlId, Timestamp maxUpdatedDate) {
            String checkQuery = "SELECT 1 FROM ELT_DL_Mapping_Info_Saved WHERE DL_Id = ? AND Updated_Date > ? LIMIT 1";
            try (PreparedStatement stmt = conn.prepareStatement(checkQuery)) {
                stmt.setString(1, dlId);
                stmt.setTimestamp(2, maxUpdatedDate);
                try (ResultSet rs = stmt.executeQuery()) {
                    // Return true if a result is found, otherwise false
                    return rs.next();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Function to check if a table exists in the target database, excluding views
        public boolean doesTableExist(Connection conn, String tableName, String databaseName) {
            String checkQuery = "SELECT COUNT(*) FROM information_schema.tables " +
                                "WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'";
            try (PreparedStatement stmt = conn.prepareStatement(checkQuery)) {
                stmt.setString(1, databaseName);
                stmt.setString(2, tableName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // Return true if count is greater than 0, indicating the table exists
                        return rs.getInt(1) > 0;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;
        }
    
        // Function to retrieve unique column values from Mapping_info table. Also, insert the alter script into ELT_DL_Alter_Script_Info
        public boolean fetchUniqueMappingInfoAndInsertIntoAlterScriptInfo(Connection conn, String dlId) {
            String query = SQLQueries.SELECT_UNIQUE_MAPPING_INFO_QUERY;
            StringBuilder combinedDropColumnDefinition = new StringBuilder();
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, dlId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        // Retrieve and process the columns from the result set
                        String dlIdResult = rs.getString("DL_Id");
                        String dlName = rs.getString("DL_Name");
                        String dlColumnNames = rs.getString("DL_Column_Names");
                        String constraints = rs.getString("Constraints");
                        String dlDataTypes = rs.getString("DL_Data_Types");

                        String dropColumnDefinition = "Drop Column `" + dlColumnNames + "`";
                        if (combinedDropColumnDefinition.length() > 0) {
                            combinedDropColumnDefinition.append(", ");
                        }
                        combinedDropColumnDefinition.append(dropColumnDefinition);
                    }
                    System.out.println("SQL Combined Column Definition: " + combinedDropColumnDefinition.toString());
                    String sqlAlterTableDefinition = "ALTER TABLE "+ dlName +" ";
                    final String END_OF_SCRIPT_TEXT = ";";
                    // Append all the individual part definitions
                    StringBuilder finalAlterScriptBuilder = new StringBuilder();
                    finalAlterScriptBuilder.append(sqlAlterTableDefinition)
                            .append("\n")
                            .append(combinedDropColumnDefinition)
                            .append("\n")
                            .append(END_OF_SCRIPT_TEXT);

                    // The final alter script definition
                    String finalAlterScript = finalAlterScriptBuilder.toString();
                    System.out.println(finalAlterScript);
                    // Insert the record into the table "ELT_DL_Alter_Script_Info"
                    return insertAlterScriptInfo(conn, dlId, dlName, finalAlterScript);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;
        }

        // Function to insert a record into ELT_DL_Alter_Script_Info
        public boolean insertAlterScriptInfo(
                Connection conn, 
                String dlId, 
                String dlName, 
                String alterScript) {

            String query = SQLQueries.INSERT_ALTER_SCRIPT_INFO_QUERY;
            // A few more fields initialized appropriately
            // TBD: Update needed check Date/Timestamp DataType
            boolean activeFlag = true;
            String addedUser = "";
            Timestamp addedDate = null; // TBD chech Data Type
            String updatedUser = "";
            Timestamp updatedDate = null;  // TBD
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, dlId);
                stmt.setString(2, dlName);
                stmt.setString(3, alterScript);
                stmt.setBoolean(4, activeFlag);
                stmt.setTimestamp(5, addedDate); // TBD
                stmt.setString(6, addedUser);
                stmt.setTimestamp(7, updatedDate); // TBD
                stmt.setString(8, updatedUser);

                int rowsAffected = stmt.executeUpdate();
                return rowsAffected > 0; // Return true if the insert was successful
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

    }
    
    // Function to execute the left outer join query
    public void buildChangeColumnNotNullQuery(Connection conn, String dlId, String dlName) {
        String query = SQLQueries.JOIN_OUTER_ELT_DL_MAPPING_INFO_SAVED_AND_INFO;

        StringBuilder combinedChangeColumnDefBuilder = new StringBuilder();
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            // Set the DL_Id parameter in the query
            stmt.setString(1, dlId);
            stmt.setString(1, dlName);

            // Execute the query
            try (ResultSet rs = stmt.executeQuery()) {
                // Process the result set
                while (rs.next()) {
                    String savedDlId = rs.getString("DL_Id");
                    String savedColumnName = rs.getString("DL_Column_Names");
                    String lookupColumnNames = rs.getString("lookup_column_names");
                    String lookupDataTypes = rs.getString("lookup_data_types");
                    String savedConstraints = rs.getString("saved_constraints");
                    String lookupConstraints = rs.getString("lookup_constraints");
                    // Form Change Column Definition and append to combined Definition
                    String changeColumnDefinition = sqlChangeColumnDefinition(savedColumnName, lookupDataTypes,
                            savedConstraints, lookupConstraints);
                    if (combinedChangeColumnDefBuilder.length() > 0) {
                        combinedChangeColumnDefBuilder.append(", ");
                    }
                    combinedChangeColumnDefBuilder.append(changeColumnDefinition);
                }

                String finalChangeColumnNotNullScript = combinedChangeColumnDefBuilder.toString();
                System.out.println(finalChangeColumnNotNullScript);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String sqlChangeColumnDefinition(String columnName, String lookupDataTypes, String savedConstraints,
            String lookupConstraints) {
        final String CHANGE_COLUMN_DEF_START = "Change column ";
        final String NOT_NULL = " not NULL ";
        StringBuilder finalScriptBuilder = new StringBuilder();
        if (lookupDataTypes != null) {
            if ("pk".equalsIgnoreCase(savedConstraints) && "".equalsIgnoreCase(lookupConstraints)) {
                finalScriptBuilder.setLength(0); // ensure start afresh
                finalScriptBuilder.append(CHANGE_COLUMN_DEF_START).append(" ")
                        .append('`').append(columnName).append('`').append(" ")
                        .append('`').append(columnName).append('`').append(" ")  
                        .append(NOT_NULL);
            }
        }
        return finalScriptBuilder.toString();
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

        // SQL Query to update the Active_flag in ELT_DL_Alter_Script_Info table
        public static final String UPDATE_ACTIVE_FLAG = "UPDATE ELT_DL_Alter_Script_Info SET Active_flag = 0 WHERE DL_Id = ?";

        public static final String SELECT_MAX_UPDATED_DATE = "SELECT MAX(Updated_Date) FROM ELT_DL_Mapping_Info WHERE DL_Id = ?";

        // Query to retrieve unique columns from joined tables
        public static final String SELECT_UNIQUE_MAPPING_INFO_QUERY = 
                "SELECT DISTINCT " +
                "    mi.DL_Id, " +
                "    mi.DL_Name, " +
                "    mi.DL_Column_Names, " +
                "    mi.Constraints, " +
                "    mi.DL_Data_Types " +
                "FROM " +
                "    ELT_DL_Mapping_Info mi " +
                "INNER JOIN " +
                "    ELT_DL_Mapping_Info_Saved mis " +
                "ON " +
                "    mi.DL_Id = mis.DL_Id " +
                "WHERE " +
                "    mi.DL_Id = ?";

        // Query to insert a record into ELT_DL_Alter_Script_Info
        public static final String INSERT_ALTER_SCRIPT_INFO_QUERY = 
                "INSERT INTO ELT_DL_Alter_Script_Info (" +
                "    DL_Id, " +
                "    DL_Name, " +
                "    Alter_Script, " +
                "    Active_Flag, " +
                "    Added_Date, " +
                "    Added_User, " +
                "    Updated_Date, " +
                "    Updated_User" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        // SQL Query to perform the left outer join between ELT_DL_Mapping_Info_Saved
        // and ELT_DL_Mapping_Info
        public static final String JOIN_OUTER_ELT_DL_MAPPING_INFO_SAVED_AND_INFO =
                // TBD: Performance - all are not keys?
                "SELECT DISTINCT " +
                        "    saved.DL_Id, " +
                        "    saved.DL_Column_Names, " +
                        // "    CONCAT('`', saved.DL_Column_Names, '`') AS tilt_columns, " + // redundant Used removed
                        "    saved.Constraints AS saved_constraints, " + // Used
                        "    lookup.Constraints AS lookup_constraints, " + // Used
                        "    saved.DL_Data_Types AS saved_data_types, " + // Used
                        "    lookup.DL_Data_Types AS lookup_data_types " + // Used
                        "FROM " +
                        "    ELT_DL_Mapping_Info_Saved saved " +
                        "LEFT OUTER JOIN " +
                        "    ELT_DL_Mapping_Info lookup " +
                        "ON " +
                        "    saved.DL_Id = lookup.DL_Id " +
                        "    AND saved.DL_Column_Names = lookup.DL_Column_Names " +
                        // " AND saved.DL_Data_Types = lookup.DL_Data_Types " + #TBD this check is not
                        // required??
                        // TBD Check what should eb the functionality, check on types will become very
                        // restrictive
                        "WHERE " +
                        "    saved.DL_Id = ? " + // DL_Id as a parameter
                        "    AND saved.DL_Name = ? " + // DL_Name as a parameter
                        "    AND saved.Constraints = 'PK' " +
                        "ORDER BY saved.DL_Column_Names";
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
