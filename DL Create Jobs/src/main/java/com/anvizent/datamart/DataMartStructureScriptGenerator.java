package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataMartStructureScriptGenerator {

    public static final String END_OF_SCRIPT_TEXT = ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci";
    private DataSourceType dataSourceType;
    private String dlId;
    // TBD                                          ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
    // Is semicolon neeeded?
    
    public DataMartStructureScriptGenerator(DataSourceType type, String tableId) {
        dataSourceType = type;
        dlId = tableId;
    }

    public void generateCreateScript() {
        try {
            // Get DB connection for the dataSourceType
            Connection conn = DBHelper.getConnection(dataSourceType);

            if (conn != null) {
                System.out.println("DB Connection established");

                //String dlId = "dl_id";
                String tableName = "";
                String id = "";
                String columnName = "";
                StringBuilder combinedColumnDefinitions = new StringBuilder();
                //String sqlPrimaryKeyDefinition = "";
                StringBuilder primaryKeys = new StringBuilder();
                StringBuilder secondaryKeys = new StringBuilder();

                try (PreparedStatement pstmt = conn.prepareStatement(SQLQueries.SELECT_DETAILS_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY)) {
                    pstmt.setString(1, dlId); // Set the parameter for DL_Id

                    try (ResultSet rs = pstmt.executeQuery()) {

                        String columnDefinition = "";
                        while (rs.next()) {
                            // Process the result set
                            id = rs.getString("DL_Id");
                            tableName = rs.getString("DL_Name");
                            columnName = rs.getString("DL_Column_Names");
                            String constraints = rs.getString("Constraints");
                            String dataTypes = rs.getString("DL_Data_Types");

                            // Operations on data
                            // tableName - no change
                            // create - fixed
                            // PKs - aggregated
                            // SKs - aggregated
                            // ColumnNames - aggregated
                           
                            // Consolidated Primary keys
                            if ("PK".equalsIgnoreCase(constraints)) {
                                if (primaryKeys.length() > 0) {
                                    primaryKeys.append(", ");
                                }
                                primaryKeys.append(columnName);
                            }
                            // Consolidated Secondary keys
                            // Append secondary keys if the constraint is 'SK' or any other key type
                            // TBD: is UK deined in our tables? if not it should be removed
                            else if ("SK".equalsIgnoreCase(constraints) || "UK".equalsIgnoreCase(constraints)) {
                                if (secondaryKeys.length() > 0) {
                                    secondaryKeys.append(", ");
                                }
                                secondaryKeys.append(columnName);
                            }

                            // Form Column Definition and append to combined Column Definition
                            columnDefinition = buildColumnDefinition(columnName, constraints, dataTypes);
                            if (combinedColumnDefinitions.length() > 0) {
                                combinedColumnDefinitions.append(", ");
                            }
                            combinedColumnDefinitions.append(columnDefinition);
                        }
                    }
 
                    System.out.println("SQL Combined Column Definition: " + combinedColumnDefinitions.toString());
                    String sqlCreateTableDefinition = "CREATE TABLE IF NOT EXISTS "+ tableName +" (";
                    // TBD - Need to check the text after these two sets of keys are combined.
                    String key = primaryKeys.toString() + secondaryKeys.toString();
                    int len = key.length();
                    String primaryKey = key.substring(0, len - 1);
                    String sqlPrimaryKeyDefinition = " Primary Key (" + primaryKey + " ) ";

                    // Append all the individual part definitions
                    StringBuilder finalScriptBuilder = new StringBuilder();
                    finalScriptBuilder.append(sqlCreateTableDefinition)
                            .append("\n")
                            .append(combinedColumnDefinitions)
                            .append("\n")
                            .append(sqlPrimaryKeyDefinition)
                            .append("\n")
                            .append(END_OF_SCRIPT_TEXT);

                    // The final script definition
                    String finalCreateScript = finalScriptBuilder.toString();
                    System.out.println(finalCreateScript);

                    // Final step that is to put data into the table
                    insertIntoELT_DL_Create_Info(conn, id, tableName, finalCreateScript);
                }
                conn.close();
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

        columnDefinitionBuilder.append("\n")
                .append(columnName)
                .append(" ")
                .append(dataType)
                .append(dataType.startsWith("varchar") ? " COLLATE utf8_unicode_ci" : " ");

        if (constraint.equals("pk")) {
            columnDefinitionBuilder.append(" NOT NULL DEFAULT ");
            columnDefinitionBuilder.append(getDefaultForDataType(dataType));
        } else {
            columnDefinitionBuilder.append(" DEFAULT NULL");
        }
        return columnDefinitionBuilder.toString();
    }

    // Helper function to get the default value based on the data type
    private static String getDefaultForDataType(String dataType) {
        switch (dataType.toLowerCase()) {
            case "varchar":
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

    /**
     * Inserts a record into the 'ELT_DL_Create_Info' table with the given 'DL_ID', 'DL_NAME', and 'script'.
     * 
     * @param conn The connection object to the database.
     * @param dlId The DL_ID column.
     * @param dlName The DL_NAME column.
     * @param script The script column.
     * @throws SQLException If an SQL error occurs during the operation.
     */
    public void insertIntoELT_DL_Create_Info(Connection conn, String dlId, String dlName, String script) throws SQLException {
        String sqlInsertQuery = SQLQueries.INSERT_INTO_ELT_DL_CREATE_INFO_QUERY;
        try (PreparedStatement pstmt = conn.prepareStatement(sqlInsertQuery)) {
            pstmt.setString(1, dlId);
            pstmt.setString(2, dlName);
            pstmt.setString(3, script);
            int rowsAffected = pstmt.executeUpdate();
            System.out.println("Rows inserted: " + rowsAffected);
        }
    }

    /**
     * Removes records from 'ELT_DL_Create_Info' where 'DL_Name' matches the given parameter.
     * 
     * @param conn The connection object to the database.
     * @param dlName The DL_Name column.
     * @throws SQLException If an SQL error occurs during the operation.
     */
    public void deleteFromELT_DL_Create_Info(Connection conn, String dlName) throws SQLException {
        String sqlDeleteQuery = SQLQueries.DELETE_FROM_ELT_DL_CREATE_INFO_QUERY;
        try (PreparedStatement pstmt = conn.prepareStatement(sqlDeleteQuery)) {
            pstmt.setString(1, dlName);
            int rowsAffected = pstmt.executeUpdate();         
            System.out.println("Rows affected: " + rowsAffected);
        }
    }

    public static void main(String[] args) {
        String tableId = "XNXNXN";
        DataMartStructureScriptGenerator generator = new DataMartStructureScriptGenerator(DataSourceType.MYSQL, tableId);
        generator.generateCreateScript();
    }
}
