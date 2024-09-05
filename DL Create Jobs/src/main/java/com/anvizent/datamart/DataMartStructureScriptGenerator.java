package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataMartStructureScriptGenerator {

    public static final String END_OF_SCRIPT_TEXT = ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci";
    // TBD                                          ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
    public DataMartStructureScriptGenerator() {
        // Initialize any resources if needed
    }

    /**
     * Main method to generate the Data Mart structure create script.
     *
     * @param dataMartName The name of the Data Mart.
     * @return The SQL script to create the Data Mart structure.
     */
    public String generateCreateScript(String dataMartName) {
        try {
            Connection conn = DBHelper.getConnection(DataSourceType.MYSQL);

            if (conn != null) {
                String dlId = "dl_id";
                try (PreparedStatement pstmt = conn.prepareStatement(SQLQueries.SELECT_DISTINCT_QUERY)) {
                    pstmt.setString(1, dlId);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            // Process the result set
                            String tableName = rs.getString("Table_Name");
                            String columnAlias = rs.getString("Column_Name_Alias");
                            String constraints = rs.getString("Constraints");
                            String dataTypes = rs.getString("DL_Data_Types");
                            String id = rs.getString("DL_Id");

                            // Print or process the data
                            System.out.printf(
                                    "Table Name: %s, Column Alias: %s, Constraints: %s, Data Types: %s, DL Id: %s%n",
                                    tableName, columnAlias, constraints, dataTypes, id);
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Logic to add tables, indexes, etc.
        String createScript = generateTableCreateScripts();
        generateCreateScript();

        return createScript;
    }

    public void generateCreateScript() {
        try {
            // Get MySQL connection with auto-commit enabled
            Connection conn = DBHelper.getConnection(DataSourceType.MYSQL);

            if (conn != null) {
                System.out.println("Connection established with auto-commit enabled!");

                // Prepare and execute the SQL query
                String dlId = "some_id"; // Example value for DL_Id

                String tableName = "";
                String id = "";
                String columnName = "";
                StringBuilder sqlColumnDefinition = new StringBuilder();
                //String sqlPrimaryKeyDefinition = "";
                StringBuilder primaryKeys = new StringBuilder();
                StringBuilder secondaryKeys = new StringBuilder();

                try (PreparedStatement pstmt = conn.prepareStatement(SQLQueries.SELECT_DETAILS_QUERY)) {
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

                            // Operations
                            // tableName - no change
                            // create - fixed
                            // PKs - aggregated
                            // SKs - aggregated
                            // ColumnNames - aggregated
                           
                            // Append primary keys if the constraint is 'PK'
                            if ("PK".equalsIgnoreCase(constraints)) {
                                if (primaryKeys.length() > 0) {
                                    primaryKeys.append(", ");
                                }
                                primaryKeys.append(columnName);
                            }
                            // Append secondary keys if the constraint is 'SK' or any other key type
                            // TBD UK is deined in out tables
                            else if ("SK".equalsIgnoreCase(constraints) || "UK".equalsIgnoreCase(constraints)) {
                                if (secondaryKeys.length() > 0) {
                                    secondaryKeys.append(", ");
                                }
                                secondaryKeys.append(columnName);
                            }

                            columnDefinition = buildSQLColumnDefinition(columnName, constraints, dataTypes);
                            // Append to the StringBuilder for SQL column definitions
                            // TBD - check in the above funciton comma is not already added
                            if (sqlColumnDefinition.length() > 0) {
                                sqlColumnDefinition.append(", ");
                            }
                            sqlColumnDefinition.append(columnDefinition);
                            // Print or process the data
                            System.out.printf("DL_Id: %s, DL_Name: %s, DL_Column_Names: %s, Constraints: %s, DL_Data_Types: %s%n",
                                              id, tableName, columnName, constraints, dataTypes);
                        }
                    }
 
                    System.out.println("SQL Column Definitions: " + sqlColumnDefinition.toString());

                    String sqlCreateTableDefinition = "CREATE TABLE IF NOT EXISTS "+ tableName +" (";

                    String key = primaryKeys.toString() + secondaryKeys.toString();
                    int len = key.length();
                    // Check for the case when there is a zero length string
                    String primaryKey = key.substring(0, len -1);
                    String sqlPrimaryKeyDefinition = " Primary Key (" + primaryKey + " ) ";

                    // Create a StringBuilder object
                    StringBuilder sqlBuilder = new StringBuilder();

                    // Append the variables to the StringBuilder
                    sqlBuilder.append(sqlCreateTableDefinition)
                            .append("\n")
                            .append(sqlColumnDefinition)
                            .append("\n")
                            .append(sqlPrimaryKeyDefinition)
                            .append("\n")
                            .append(END_OF_SCRIPT_TEXT);

                    // Print the final SQL statement
                    String finalSQLStatement = sqlBuilder.toString();
                    System.out.println(finalSQLStatement);

                    // Final step that is put data into the table
                    insertIntoELT_DL_Create_Info(conn, id, tableName, finalSQLStatement);
                    // TBD what if insertion fails
                }
                conn.close();
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String buildSQLColumnDefinition(String columnName, String constraint, String dataType) {
        // Build the SQL column definition
        StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("\n")
                .append(columnName)
                .append(" ")
                .append(dataType)
                .append(dataType.startsWith("varchar") ? " COLLATE utf8_unicode_ci" : " ");

        if (constraint.equals("pk")) {
            sqlBuilder.append(" NOT NULL DEFAULT ");
            // TBD check which approach is better
            //sqlBuilder.append(getDefaultForDataType(dataType));

            if (dataType.startsWith("varchar")) {
                sqlBuilder.append("''");
            } else if (dataType.contains("int")) {
                sqlBuilder.append("0");
            } else if (dataType.contains("decimal") || dataType.contains("float")) {
                sqlBuilder.append("'0.0'");
            } else if (dataType.contains("boolean")) {
                sqlBuilder.append("0");
            } else if (dataType.contains("date")) {
                sqlBuilder.append("'0000-00-00'");
            } else {
                // Default case for any other data types
                sqlBuilder.append("\" \"");
            }
        } else {
            sqlBuilder.append(" DEFAULT NULL");
        }
        return sqlBuilder.toString();
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
     * Inserts a record into the ELT_DL_Create_Info table with the given DL_ID, DL_NAME, and script.
     * 
     * @param conn The connection object to the database.
     * @param dlId The DL_ID column.
     * @param dlName The DL_NAME column.
     * @param script The script column.
     * @throws SQLException If an SQL error occurs during the operation.
     */
    public void insertIntoELT_DL_Create_Info(Connection conn, String dlId, String dlName, String script) throws SQLException {
        // Define the SQL INSERT query
        String sqlInsertQuery = "INSERT INTO ELT_DL_Create_Info (DL_ID, DL_NAME, script) VALUES (?, ?, ?)";
        
        // Use try-with-resources to ensure the PreparedStatement is closed properly
        try (PreparedStatement pstmt = conn.prepareStatement(sqlInsertQuery)) {
            pstmt.setString(1, dlId);
            pstmt.setString(2, dlName);
            pstmt.setString(3, script);
            int rowsAffected = pstmt.executeUpdate();
            System.out.println("Rows inserted: " + rowsAffected);
        }
    }

    /**
     * Executes a DELETE query to remove records from ELT_DL_Create_Info where DL_Name matches the given parameter.
     * 
     * @param conn The connection object to the database.
     * @param dlName The value to match against DL_Name column.
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


    /**
     * Method to generate SQL scripts for creating tables in the Data Mart.
     *
     * @return SQL script for creating tables.
     */
    private String generateTableCreateScripts() {
        // Placeholder for table creation script generation
        String tableScript = "CREATE TABLE ExampleTable (\n"
                           + "   id INT PRIMARY KEY,\n"
                           + "   name VARCHAR(100)\n"
                           + ");\n";
        
        // Add more table scripts as needed
        return tableScript;
    }

    public static void main(String[] args) {
        DataMartStructureScriptGenerator generator = new DataMartStructureScriptGenerator();
        String script = generator.generateCreateScript("ExampleDataMart");
        System.out.println(script);
    }
}
