package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * The {@code DataMartCreateScriptGenerator} class is responsible for generating 
 * SQL "CREATE TABLE" scripts for a data mart based on the mapping information stored 
 * in the database. It dynamically builds column definitions, primary and secondary 
 * key constraints, and appends the necessary storage engine and character set 
 * configurations for the table.
 * 
 * <p> It retrieves table metadata from the database and generates 
 * the corresponding SQL script with the specified charset and collation.
 * 
 * <p>The generated SQL script is inserted into the database, 
 * and any previous definitions for the same table are deleted before insertion.
 * 
 * <p>Example usage:
 * <pre>
 *     DataMartCreateScriptGenerator generator = new DataMartCreateScriptGenerator(DataSourceType.MYSQL, "tableId");
 *     generator.generateCreateScript();
 * </pre> 
 * @author navin
 * @version 0.1
 */

public class DataMartCreateScriptGenerator {
    private static final Logger logger = Logger.getLogger(DataMartCreateScriptGenerator.class.toString());

    public static final String CHARSET_UTF8MB4 = "utf8mb4"; // Newer Charset
    public static final String CHARSET_UTF8MB4_COLLATION = "utf8mb4_0900_ai_ci"; // Newer Charset Collation
    // public static final String CHARSET_UTF8 = "utf8";
    // public static final String CHARSET_UTF8_COLLATION = "utf8mb4_0900_ai_ci";

    public static final String CHARSET = CHARSET_UTF8MB4;
    public static final String CHARSET_COLLATION = CHARSET_UTF8MB4_COLLATION;

    private static final String END_OF_SCRIPT_TEXT = ") ENGINE=InnoDB DEFAULT CHARSET=" + CHARSET + " COLLATE=" + CHARSET_COLLATION;

    private final DataSourceType dataSourceType;
    private final String dlId;
    
    public DataMartCreateScriptGenerator(DataSourceType type, String tableId) {
        dataSourceType = type;
        dlId = tableId;
    }

    public void generateCreateScript() {
        try (Connection conn = DBHelper.getConnection(DataSourceType.MYSQL)) {
            if (conn != null) {
                String tableName = "";
                String id = "";
                String columnName = "";
                StringBuilder combinedColumnDefinitions = new StringBuilder();
                StringBuilder primaryKeys = new StringBuilder();
                StringBuilder secondaryKeys = new StringBuilder();

                try (PreparedStatement pstmt = conn.prepareStatement(SQLQueries.SELECT_DETAILS_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY)) {
                    pstmt.setString(1, dlId);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        String columnDefinition = "";
                        while (rs.next()) {
                            id = rs.getString("DL_Id");
                            tableName = rs.getString("DL_Name");
                            columnName = rs.getString("DL_Column_Names");
                            String constraints = rs.getString("Constraints");
                            String dataTypes = rs.getString("DL_Data_Types");
                           
                            // Consolidated Primary keys
                            if ("PK".equalsIgnoreCase(constraints)) {
                                if (primaryKeys.length() > 0) {
                                    primaryKeys.append(", ");
                                }
                                primaryKeys.append(columnName);
                            }
                            // Consolidated Secondary keys
                            else if ("SK".equalsIgnoreCase(constraints)) {
                                if (secondaryKeys.length() > 0) {
                                    secondaryKeys.append(", ");
                                }
                                secondaryKeys.append(columnName);
                            }

                            // Form Column Definition and make a list of all column Definitions
                            columnDefinition = buildColumnDefinition(columnName, constraints, dataTypes);
                            if (combinedColumnDefinitions.length() > 0) {
                                combinedColumnDefinitions.append(", ");
                            }
                            combinedColumnDefinitions.append(columnDefinition);
                        }
                    }
 
                    String sqlCreateTableDefinition = "CREATE TABLE IF NOT EXISTS "+ tableName +" (";
                    String key = "";
                    if (!primaryKeys.toString().isEmpty() && !secondaryKeys.toString().isEmpty()) {
                        key = primaryKeys.toString() + ", " + secondaryKeys.toString();
                    } else if (!primaryKeys.toString().isEmpty()) {
                        key = primaryKeys.toString();
                    } else if (!secondaryKeys.toString().isEmpty()) {
                        key = secondaryKeys.toString();
                    }
                    int len = key.length();
                    String primaryKey = key.substring(0, len);
                    String sqlPrimaryKeyDefinition = " Primary Key (" + primaryKey + ") ";

                    // Append all the individual part definitions
                    StringBuilder finalScriptBuilder = new StringBuilder();
                    finalScriptBuilder.append(sqlCreateTableDefinition)
                            .append(combinedColumnDefinitions)
                            .append(",\n")
                            .append(sqlPrimaryKeyDefinition)
                            .append("\n")
                            .append(END_OF_SCRIPT_TEXT);

                    // The final script definition
                    String finalCreateScript = finalScriptBuilder.toString();

                    // Delete the existing rows, if any.
                    deleteFromEltDlCreateInfo(conn, tableName);

                    // Put data into the table
                    insertIntoEltDlCreateInfo(conn, id, tableName, finalCreateScript);
                }
            } else {
                System.out.println("Failed to make connection!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // Build the column definition
    private static String buildColumnDefinition(String columnName, String constraint, String dataType) {
        StringBuilder columnDefinitionBuilder = new StringBuilder();

        // TBD: Taken from another create Script.
        // if dataType is "text" and constraint is "pk", it is changed to varchar(150)
        if ("text".equalsIgnoreCase(dataType) && "pk".equalsIgnoreCase(constraint)) {
            dataType = "varchar(150)";
        }

        columnDefinitionBuilder.append("\n")
                .append(columnName)
                .append(" ")
                .append(dataType)
                .append(dataType.startsWith("varchar") ? " COLLATE " + CHARSET_COLLATION + " " : " ");

        if ("pk".equalsIgnoreCase(constraint)) {
            columnDefinitionBuilder.append(" NOT NULL DEFAULT ");
            columnDefinitionBuilder.append(DBHelper.getDefaultForDataType(dataType));
        } else if ("sk".equalsIgnoreCase(constraint)) {
                columnDefinitionBuilder.append(" NOT NULL DEFAULT ");
                columnDefinitionBuilder.append(DBHelper.getDefaultForDataType(dataType)); // Why not
        } else {
            columnDefinitionBuilder.append(" DEFAULT NULL");
        }
        return columnDefinitionBuilder.toString();
    }

    /**
     * Inserts a record into the 'ELT_DL_Create_Info' table with the given 'DL_ID', 'DL_NAME', and 'script'.
     * 
     * @param conn The connection object to the database.
     * @param dlId The DL_ID column.
     * @param dlName The DL_NAME column.
     * @param script The script column.
     * @throws SQLException If an SQL error occurs during the operation.
     */
    public void insertIntoEltDlCreateInfo(Connection conn, String dlId, String dlName, String script) throws SQLException {
        String sqlInsertQuery = SQLQueries.INSERT_INTO_ELT_DL_CREATE_INFO_QUERY;
        try (PreparedStatement pstmt = conn.prepareStatement(sqlInsertQuery)) {
            pstmt.setString(1, dlId);
            pstmt.setString(2, dlName);
            pstmt.setString(3, script);
            int rowsAffected = pstmt.executeUpdate();
            System.out.println("Rows Inserted: " + rowsAffected);
        }
    }

    /**
     * Removes records from 'ELT_DL_Create_Info' where 'DL_Name' matches the given parameter.
     * 
     * @param conn The connection object to the database.
     * @param dlName The DL_Name column.
     * @throws SQLException If an SQL error occurs during the operation.
     */
    public void deleteFromEltDlCreateInfo(Connection conn, String dlName) throws SQLException {
        String sqlDeleteQuery = SQLQueries.DELETE_FROM_ELT_DL_CREATE_INFO_QUERY;
        try (PreparedStatement pstmt = conn.prepareStatement(sqlDeleteQuery)) {
            pstmt.setString(1, dlName);
            int rowsAffected = pstmt.executeUpdate();         
            System.out.println("Rows Deleted: " + rowsAffected);
        }
    }

    public static class SQLQueries {
        // Query for deleting records
        public static final String DELETE_FROM_ELT_DL_CREATE_INFO_QUERY = "DELETE FROM ELT_DL_Create_Info WHERE DL_Name = ?";
        // Query for inserting records
        public static final String INSERT_INTO_ELT_DL_CREATE_INFO_QUERY = "INSERT INTO ELT_DL_Create_Info (DL_ID, DL_NAME, script) VALUES (?, ?, ?)";
        // Query for retrieving detailed information from the ELT_DL_Mapping_Info_Saved table.
        public static final String SELECT_DETAILS_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY = "SELECT " +
                "    `ELT_DL_Mapping_Info_Saved`.`DL_Id`, " +
                "    `ELT_DL_Mapping_Info_Saved`.`DL_Name`, " +
                "    `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`, " +
                "    `ELT_DL_Mapping_Info_Saved`.`Constraints`, " +
                "    `ELT_DL_Mapping_Info_Saved`.`DL_Data_Types` " +
                "FROM " +
                "    `ELT_DL_Mapping_Info_Saved` " +
                "WHERE " +
                "    `DL_Id` = ?";
    }

    public static void main(String[] args) {
        String tableId = "9"; // Input paramter along with APP DB in Context
        DataMartCreateScriptGenerator generator = new DataMartCreateScriptGenerator(DataSourceType.MYSQL, tableId);
        generator.generateCreateScript();
    }
}

// Enum to represent different data source types
enum DataSourceType {
    MYSQL,
    SNOWFLAKE,
    SQLSERVER
}
// Enum to represent different return status
enum Status {
    SUCCESS,
    FAILURE
}
