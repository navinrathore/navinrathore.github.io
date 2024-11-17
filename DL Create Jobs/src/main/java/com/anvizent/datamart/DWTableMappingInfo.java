package com.anvizent.datamart;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * Table Mapping Info, DW Table Mapping generation mapping 
 */
public class DWTableMappingInfo {

	
    private static final Map<String, String> context = new HashMap<>();
    private static final Map<String, Object> globalMap = new HashMap<>();

    public static void main(String[] args) {
        loadContext();
        mainProcess();
    }
    
   public static void initiateJob(Map<String, String> newContext){
	   if (newContext == null) {
           throw new IllegalArgumentException("Context cannot be null");
       }
       synchronized (context) {
           context.clear();
           context.putAll(newContext);
           mainProcess();
       }
    }
    

    private static void loadContext() {
    	 synchronized (context) {
	    	context.put("APP_HOST", "172.25.25.124:4475");
			context.put("APP_DBNAME", "Mysql8_2_1009427_appdb");
			context.put("APP_UN", "Bk16Tt55");
			context.put("APP_PW", "Tt5526");
			context.put("Schema_Name", "");
			context.put("Table_Name","");
			context.put("CONNECTION_ID", "41"); // 2,3,4
			context.put("DATASOURCENAME","syspro");
			context.put("CLIENT_ID", "1009427");
			context.put("TableType", "D");
            context.put("Custom_Flag", "0");
            context.put("Selective_Tables","'Finished_Goods_BOM'");

            
    	 }
    }

    private static Connection getDbConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://" + context.get("APP_HOST") + "/" + context.get("APP_DBNAME");
            return DriverManager.getConnection(url, context.get("APP_UN"), context.get("APP_PW"));
        } catch (Exception e){
            System.out.println("Error connecting to the database: " + e.getMessage());
            return null;
        }
    }

    // OK - Yes
    private static void createQueryConditions() {
        String schemaName = context.get("Schema_Name");
        String querySchemaCondition = "";
        String querySchemaCondition1 = "";

        if (schemaName == null || schemaName.equalsIgnoreCase("NULL") || schemaName.isEmpty()) {
            querySchemaCondition = "";
            querySchemaCondition1 = "";
        } else {
            querySchemaCondition = " AND TABLE_SCHEMA = '" + schemaName + "'";
            querySchemaCondition1 = " AND Schema_Name = '" + schemaName + "'";
        }

        context.put("query_schema_cond", querySchemaCondition);
        context.put("query_schema_cond1", querySchemaCondition1);
    }

    private static void deleteRecords(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            String connectionId = context.get("CONNECTION_ID");
            String querySchemaCondition = context.get("query_schema_cond");

            String deleteQuery = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE IL_Table_Name IN (" +
                    "    SELECT DISTINCT IL_Table_Name" +
                    "    FROM ELT_IL_Source_Mapping_Info_History" +
                    "    WHERE Source_Table_Name NOT IN (" +
                    "        SELECT DISTINCT Table_Name" +
                    "        FROM ELT_Selective_Source_Metadata" +
                    "        WHERE Isfileupload != '1'" +
                    "        AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition +
                    "    )" +
                    "    AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition +
                    ")" +
                    "AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition;

            int rowsAffected = stmt.executeUpdate(deleteQuery);
            System.out.println("Deleted " + rowsAffected + " rows from ELT_IL_Source_Mapping_Info_Saved.");
        } catch (SQLException e) {
            System.out.println("Error executing delete query: " + e.getMessage());
        }
    }

    private static void mainProcess() {
        Connection connection = getDbConnection();
        if (connection == null) {
            System.out.println("Failed to establish database connection. Exiting.");
            return;
        }

        try {
            createQueryConditions();
            deleteRecords(connection);

            int customFlag = Integer.parseInt(context.get("Custom_Flag"));

            if (customFlag == 0) { //DB,Metadata file
                System.out.println("Running process when Custom_Flag = 0");
                mainProcessForFlagZero();
            } else if (customFlag == 1) {  //SHARED_FOLDER, FLAT_FILE, WEB_SERVICE , ONEDRIVE, SAGE_INTACCT, SALESFORCESOAP
                System.out.println("Running process when Custom_Flag = 1");
                processForFlagOne(connection);
            } else {
                System.out.println("Invalid Custom_Flag value. Exiting.");
            }
        } finally {
            try {
                connection.close();
                System.out.println("Main database connection closed.");
            } catch (SQLException e) {
                System.out.println("Error closing database connection: " + e.getMessage());
            }
        }
    }
          // ... (previous code remains the same)


    
          
        public static void mainProcessForFlagZero() {
            Connection dbConnection = getDbConnection();
            if (dbConnection == null) {
                System.out.println("Failed to establish database connection. Exiting.");
                return;
            }

            try {
                // Done navin
                processSelectiveTables(dbConnection); //Form Table Name with Suffix 
                System.out.println("Selective tables processing completed successfully.");

                // Step 2 - iteration

                // call setACtiveFlag()
                // TODO approproate arguments inside iterative loop
                Map<String, Map<String, Object>> resultMetadata = processSelectiveSourceMetadata(dbConnection, null, null, null);





                Map<String, Object> updateResult = updateIncrementalColumnAndPrepareBulk(dbConnection); //Incremental Column updations D, T 
                System.out.println("update_incremental_column_and_prepare_bulk completed successfully.");

                List<Map<String, Object>> eltSourceMetadataAdd = joinAndCalculateIlDataType(dbConnection, (List<Map<String, Object>>) updateResult.get("OUT")); // Compute Datatype Mapping
                System.out.println("join_and_calculate_il_data_type completed successfully.");

                List<Map<String, Object>> result = mergeMetadataAndCalculateConstraints(dbConnection, eltSourceMetadataAdd); //Pk FK Column definations and formations 
                System.out.println("merge_settings_and_calculate_il_table_name completed successfully.");

                List<Map<String, Object>> out2 = createMetadataConstants(dbConnection);
                System.out.println("create_metadata_constants completed successfully.");

                List<Map<String, Object>> row1 = uniteResultOut2(result, out2);
                System.out.println("tUnite_Result_out2 completed successfully.");

                bulk(dbConnection, row1);
                System.out.println("Bulk operation completed successfully.");

            } catch (Exception e) {
                System.out.println("An error occurred during execution: " + e.getMessage());
            } finally {
                try {
                    dbConnection.close();
                    System.out.println("Database connection closed.");
                } catch (SQLException e) {
                    System.out.println("Error closing database connection: " + e.getMessage());
                }
            }
        }
        
        
        private static void processForFlagOne(Connection connection) {
            try {

                // Done - Navin
                String selectTables = context.get("Selective_Tables");
                String connectionId = context.get("CONNECTION_ID");
                String querySchemaCondition1 = context.get("query_schema_cond1");
                getSelectiveSourceMetadataWithSettingsSharedFolderWS(selectTables, connectionId, querySchemaCondition1, connection);

                // Step 2 - iteration

                List<Map<String, Object>> eltMetadata = fetchEltMetadata(connection);
                Map<String, Object> row1 = fetchEltSettings(connection).get(0);
                Map<String, Object> row2 = fetchWsConnectionSuffix(connection).get(0);
                Map<String, Object> row3 = fetchSharedconnectionsInfo(connection).get(0);

                List<Map<String, Object>> transformedRows = eltMetadata.stream()
                    .map(row -> transformTableNameAndSuffix(row, row1, row2, row3))
                    .collect(Collectors.toList());

                List<Map<String, Object>> eltIlSourceMappingInfoSavedUpload = transformedRows;

                Map<String, Object> processResult = processAndTransformEltData(connection);
                List<Map<String, Object>> updateDf = (List<Map<String, Object>>) processResult.get("update_df");
                List<Map<String, Object>> outDf = (List<Map<String, Object>>) processResult.get("OUT");

                List<Map<String, Object>> eltSourceMetadataAdd = mergeMetadataAndCalculateConstraints(connection, outDf);

                List<Map<String, Object>> result = mergeSettingsAndCalculateIlTableName(connection, eltSourceMetadataAdd);

                List<Map<String, Object>> out2 = createConstantColumnsAndIlTableName(connection);

                List<Map<String, Object>> row1Final = uniteDataframes(connection, eltSourceMetadataAdd);

                bulk(connection, row1Final);

                System.out.println("Processing for custom_flag ==custom_flag 1 completed successfully.");
            } catch (Exception e) {
                System.out.println("An error occurred during processing for custom_flag == 1: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private static List<Map<String, Object>> fetchEltMetadata(Connection connection) throws SQLException {
            String query = "SELECT Connection_Id, Schema_Name, Table_Name, Dimension_Transaction, IsWebService " +
                           "FROM ELT_Selective_Source_Metadata " +
                           "WHERE Connection_Id = ?";
            
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, context.get("CONNECTION_ID"));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Dimension_Transaction", rs.getString("Dimension_Transaction"));
                        row.put("IsWebService", rs.getString("IsWebService"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static List<Map<String, Object>> fetchEltSettings(Connection connection) throws SQLException {
            String query = "SELECT Setting_Value " +
                           "FROM ELT_IL_Settings_Info " +
                           "WHERE Settings_Category = 'Suffix' " +
                           "AND Active_Flag = '1' " +
                           "AND Connection_Id = ? " +
                           context.get("query_schema_cond1");
            
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, context.get("CONNECTION_ID"));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Setting_Value", rs.getString("Setting_Value"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static List<Map<String, Object>> fetchWsConnectionSuffix(Connection connection) throws SQLException {
            String query = "SELECT id, suffix " +
                           "FROM minidwcs_ws_connections_mst " +
                           "WHERE id = ?";
            
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, context.get("CONNECTION_ID"));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("id", rs.getString("id"));
                        row.put("suffix", rs.getString("suffix"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static List<Map<String, Object>> fetchSharedconnectionsInfo(Connection connection) throws SQLException {
            String query = "SELECT File_Id, File_Path " +
                           "FROM sharedconnections_file_path_info " +
                           "WHERE Connection_Id = ?";
            
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, context.get("CONNECTION_ID"));
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("File_Id", rs.getString("File_Id"));
                        row.put("File_Path", rs.getString("File_Path"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static Map<String, Object> transformTableNameAndSuffix(Map<String, Object> eltSelectiveSourceMetadata, 
                                                                       Map<String, Object> row1, 
                                                                       Map<String, Object> row2, 
                                                                       Map<String, Object> row3) {
            String suffix = null;
            if (context.get("Custom_Type") == null) {
                suffix = (String) row2.get("suffix");
            } else if ("shared_folder".equals(context.get("Custom_Type"))) {
                suffix = (String) row2.get("suffix");
            } else if (Arrays.asList("web_service", "OneDrive", "SageIntacct").contains(context.get("Custom_Type"))) {
                suffix = (String) row3.get("suffix");
            } else {
                suffix = (String) row1.get("Setting_Value");
            }

            Map<String, Object> out = new HashMap<>(eltSelectiveSourceMetadata);
            out.put("Setting_Value", suffix);
            out.put("DW_Table_Name", suffix != null ? 
                    eltSelectiveSourceMetadata.get("Table_Name") + "_" + suffix : 
                    eltSelectiveSourceMetadata.get("Table_Name"));

            return out;
        }

        private static Map<String, Object> processAndTransformEltData(Connection connection) throws SQLException {
            // Implementation for processAndTransformEltData
            // This method should execute the necessary SQL queries and data transformations
            // Similar to the Python implementation, but using JDBC and Java collections
            // ...

            // Placeholder implementation
            Map<String, Object> result = new HashMap<>();
            result.put("update_df", new ArrayList<>());
            result.put("OUT", new ArrayList<>());
            return result;
        }


        private static List<Map<String, Object>> mergeSettingsAndCalculateIlTableName(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
            String connectionId = context.get("CONNECTION_ID");
            String customType = context.get("Custom_Type");
            String querySchemaCondition1 = context.get("query_schema_cond1");

            // Query for ELT_IL_Settings_Info (Active Alias Values)
            String queryActiveAlias = "SELECT Connection_Id, Schema_Name, Setting_Value " +
                                      "FROM ELT_IL_Settings_Info " +
                                      "WHERE Settings_Category = 'Suffix' " +
                                      "AND Active_Flag = '1' " +
                                      "AND Connection_Id = ? " +
                                      querySchemaCondition1;

            // Query for sharedconnections_file_path_info
            String querySharedFilePath = "SELECT Connection_Id, param_or_schema_name, suffix " +
                                         "FROM sharedconnections_file_path_info " +
                                         "WHERE Connection_Id = ?";

            // Query for minidwcs_ws_connections_mst
            String queryWsConnections = "SELECT id, suffix " +
                                        "FROM minidwcs_ws_connections_mst " +
                                        "WHERE id = ?";

            List<Map<String, Object>> activeAliasValues = new ArrayList<>();
            List<Map<String, Object>> sharedFilePathInfo = new ArrayList<>();
            List<Map<String, Object>> wsConnectionsInfo = new ArrayList<>();

            // Fetch Active Alias Values
            try (PreparedStatement pstmt = connection.prepareStatement(queryActiveAlias)) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Setting_Value", rs.getString("Setting_Value"));
                        activeAliasValues.add(row);
                    }
                }
            }

            // Fetch Shared File Path Info
            try (PreparedStatement pstmt = connection.prepareStatement(querySharedFilePath)) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("param_or_schema_name", rs.getString("param_or_schema_name"));
                        row.put("suffix", rs.getString("suffix"));
                        sharedFilePathInfo.add(row);
                    }
                }
            }

            // Fetch WS Connections Info
            try (PreparedStatement pstmt = connection.prepareStatement(queryWsConnections)) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("id", rs.getString("id"));
                        row.put("suffix", rs.getString("suffix"));
                        wsConnectionsInfo.add(row);
                    }
                }
            }

            // Perform joins and calculate IL table names
            return eltSourceMetadataAdd.stream()
                .flatMap(metaRow -> activeAliasValues.stream()
                    .filter(aliasRow -> metaRow.get("Connection_Id").equals(aliasRow.get("Connection_Id"))
                                        && metaRow.get("TABLE_SCHEMA").equals(aliasRow.get("Schema_Name")))
                    .flatMap(aliasRow -> sharedFilePathInfo.stream()
                        .filter(sharedRow -> metaRow.get("Connection_Id").equals(sharedRow.get("Connection_Id")))
                        .flatMap(sharedRow -> wsConnectionsInfo.stream()
                            .filter(wsRow -> metaRow.get("Connection_Id").equals(wsRow.get("id")))
                            .map(wsRow -> {
                                Map<String, Object> mergedRow = new HashMap<>(metaRow);
                                mergedRow.putAll(aliasRow);
                                mergedRow.putAll(sharedRow);
                                mergedRow.putAll(wsRow);
                                return calculateIlTableName(mergedRow, customType);
                            }))))
                .collect(Collectors.toList());
        }

        private static Map<String, Object> calculateIlTableName(Map<String, Object> row, String customType) {
            String settingValue;
            if (customType == null) {
                settingValue = (String) row.get("suffix");
            } else if ("shared_folder".equals(customType)) {
                settingValue = (String) row.get("suffix");
            } else if (Arrays.asList("web_service", "OneDrive", "SageIntacct").contains(customType)) {
                settingValue = (String) row.get("suffix");
            } else {
                settingValue = (String) row.get("Setting_Value");
            }

            String ilTableName = settingValue != null ? 
                row.get("Table_Name") + "_" + settingValue : 
                (String) row.get("Table_Name");

            row.put("Setting_Value", settingValue);
            row.put("IL_Table_Name", ilTableName);
            row.put("IL_Column_Name", row.get("Column_Name"));

            return row;
        }

        private static List<Map<String, Object>> createConstantColumnsAndIlTableName(Connection connection) throws SQLException {
            final String connectionId = context.get("CONNECTION_ID");
            final String schemaName = context.get("Schema_Name");
            final String tableName = context.get("Table_Name");
            final String dataSourceName = context.get("DATASOURCENAME");
            final String companyId = context.get("Company_Id");
            final String querySchemaCondition1 = context.get("query_schema_cond1");

            // Query for constant metadata
            String queryConstantMetadata = 
                "SELECT 'DataSource_Id' AS IL_Column_Name, 'varchar(50)' AS IL_Data_Type, 'varchar' AS Source_Data_Type, " +
                "'PK' AS Constraints, 'PK' AS PK_Constraint, 'Y' AS Constant_Insert_Column, ? AS Constant_Insert_Value " +
                "FROM ELT_Selective_Source_Metadata " +
                "UNION ALL " +
                "SELECT ? AS IL_Column_Name, 'bigint(32)' AS IL_Data_Type, 'bigint' AS Source_Data_Type, " +
                "'SK' AS Constraints, 'SK' AS PK_Constraint, 'N' AS Constant_Insert_Column, NULL AS Constant_Insert_Value " +
                "FROM ELT_Selective_Source_Metadata " +
                "UNION ALL " +
                "SELECT 'Company_Id' AS IL_Column_Name, 'varchar(50)' AS IL_Data_Type, 'varchar' AS Source_Data_Type, " +
                "'PK' AS Constraints, 'PK' AS PK_Constraint, 'Y' AS Constant_Insert_Column, ? AS Constant_Insert_Value " +
                "FROM ELT_Selective_Source_Metadata";

            // Query for ELT_Selective_Source_Metadata
            String querySelectiveMetadata = 
                "SELECT DISTINCT Connection_Id, Schema_Name, Table_Name, Dimension_Transaction " +
                "FROM ELT_Selective_Source_Metadata " +
                "WHERE Table_Name = ? AND IsFileUpload != '1' AND Connection_Id = ? " + querySchemaCondition1;

            // Query for ELT_IL_Settings_Info
            String queryIlSettings = 
                "SELECT Connection_Id, Schema_Name, Setting_Value " +
                "FROM ELT_IL_Settings_Info " +
                "WHERE Settings_Category = 'Suffix' AND Active_Flag = '1' AND Connection_Id = ? " + querySchemaCondition1;

            List<Map<String, Object>> constantMetadata = fetchConstantMetadata(connection, queryConstantMetadata, dataSourceName, tableName, companyId);
            List<Map<String, Object>> selectiveMetadata = fetchSelectiveMetadata(connection, querySelectiveMetadata, tableName, connectionId);
            List<Map<String, Object>> ilSettings = fetchIlSettings(connection, queryIlSettings, connectionId);

            return constantMetadata.stream()
                .map(constRow -> processConstantRow(constRow, connectionId, schemaName, tableName, selectiveMetadata, ilSettings))
                .collect(Collectors.toList());
        }

        private static Map<String, Object> processConstantRow(
                Map<String, Object> constRow, 
                String connectionId, 
                String schemaName, 
                String tableName,
                List<Map<String, Object>> selectiveMetadata,
                List<Map<String, Object>> ilSettings) {
            
            Map<String, Object> resultRow = new HashMap<>(constRow);
            resultRow.put("Connection_Id", connectionId);
            resultRow.put("TABLE_SCHEMA", schemaName);
            resultRow.put("Source_Table_Name", tableName);
            resultRow.put("Active_Flag", true);

            // Find matching selective metadata
            selectiveMetadata.stream()
                .filter(selRow -> tableName.equals(selRow.get("Table_Name")))
                .findFirst()
                .ifPresent(selRow -> resultRow.put("Dimension_Transaction", selRow.get("Dimension_Transaction")));

            // Find matching IL setting
            ilSettings.stream()
                .filter(ilRow -> schemaName.equals(ilRow.get("Schema_Name")))
                .findFirst()
                .ifPresent(ilRow -> {
                    String settingValue = (String) ilRow.get("Setting_Value");
                    resultRow.put("IL_Table_Name", settingValue != null ? tableName + "_" + settingValue : tableName);
                });

            return createFinalConstantRow(resultRow);
        }
        
        private static Map<String, Object> createFinalConstantRow(Map<String, Object> row) {
            Map<String, Object> finalRow = new HashMap<>();
            finalRow.put("Connection_Id", row.get("Connection_Id"));
            finalRow.put("TABLE_SCHEMA", row.get("TABLE_SCHEMA"));
            finalRow.put("IL_Table_Name", row.get("IL_Table_Name"));
            finalRow.put("IL_Column_Name", row.get("IL_Column_Name"));
            finalRow.put("IL_Data_Type", row.get("IL_Data_Type"));
            finalRow.put("Constraints", row.get("Constraints"));
            finalRow.put("Source_Table_Name", row.get("Source_Table_Name"));
            finalRow.put("Source_Column_Name", row.get("IL_Column_Name"));
            finalRow.put("Source_Data_Type", row.get("Source_Data_Type"));
            finalRow.put("PK_Constraint", row.get("PK_Constraint"));
            finalRow.put("PK_Column_Name", null);
            finalRow.put("FK_Constraint", null);
            finalRow.put("FK_Column_Name", null);
            finalRow.put("Constant_Insert_Column", row.get("Constant_Insert_Column"));
            finalRow.put("Constant_Insert_Value", row.get("Constant_Insert_Value"));
            finalRow.put("Dimension_Transaction", row.get("Dimension_Transaction"));
            finalRow.put("Dimension_Key", null);
            finalRow.put("Dimension_Name", null);
            finalRow.put("Dimension_Join_Condition", null);
            finalRow.put("Incremental_Column", null);
            finalRow.put("Isfileupload", 0);
            finalRow.put("File_Id", 0);
            finalRow.put("Column_Type", "Anvizent");
            finalRow.put("Active_Flag", row.get("Active_Flag"));
            finalRow.put("Added_Date", LocalDateTime.now());
            finalRow.put("Added_User", "ETL Admin");
            finalRow.put("Updated_Date", LocalDateTime.now());
            finalRow.put("Updated_User", "ETL Admin");
            return finalRow;
        }


        private static List<Map<String, Object>> fetchConstantMetadata(
                Connection connection, String query, String dataSourceName, String tableName, String companyId) throws SQLException {
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, dataSourceName);
                pstmt.setString(2, tableName + "_Key");
                pstmt.setString(3, companyId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("IL_Column_Name", rs.getString("IL_Column_Name"));
                        row.put("IL_Data_Type", rs.getString("IL_Data_Type"));
                        row.put("Source_Data_Type", rs.getString("Source_Data_Type"));
                        row.put("Constraints", rs.getString("Constraints"));
                        row.put("PK_Constraint", rs.getString("PK_Constraint"));
                        row.put("Constant_Insert_Column", rs.getString("Constant_Insert_Column"));
                        row.put("Constant_Insert_Value", rs.getString("Constant_Insert_Value"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static List<Map<String, Object>> fetchSelectiveMetadata(
                Connection connection, String query, String tableName, String connectionId) throws SQLException {
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Dimension_Transaction", rs.getString("Dimension_Transaction"));
                        result.add(row);
                    }
                }
            }
            return result;
        }

        private static List<Map<String, Object>> fetchIlSettings(
                Connection connection, String query, String connectionId) throws SQLException {
            List<Map<String, Object>> result = new ArrayList<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Setting_Value", rs.getString("Setting_Value"));
                        result.add(row);
                    }
                }
            }
            return result;
        }


        private static List<Map<String, Object>> uniteDataframes(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
            // Fetch Result dataframe
            List<Map<String, Object>> result = mergeSettingsAndCalculateIlTableName(connection, eltSourceMetadataAdd);
            
            // Fetch out2 dataframe
            List<Map<String, Object>> out2 = createConstantColumnsAndIlTableName(connection);

            // Determine the common set of keys
            Set<String> commonKeys = new HashSet<>();
            if (!result.isEmpty() && !out2.isEmpty()) {
                commonKeys.addAll(result.get(0).keySet());
                commonKeys.retainAll(out2.get(0).keySet());
            }

            // Combine the two lists, keeping only the common keys
            List<Map<String, Object>> combinedList = Stream.concat(
                    result.stream().map(row -> filterCommonKeys(row, commonKeys)),
                    out2.stream().map(row -> filterCommonKeys(row, commonKeys))
                )
                .collect(Collectors.toList());

            return combinedList;
        }

        private static void saveToCsv(List<Map<String, Object>> data, String filePath) throws IOException {
            if (data.isEmpty()) {
                return;
            }

            List<String> lines = new ArrayList<>();
            lines.add(String.join(",", data.get(0).keySet())); // Header

            for (Map<String, Object> row : data) {
                lines.add(row.values().stream()
                             .map(Object::toString)
                             .collect(Collectors.joining(",")));
            }

            Files.write(Paths.get(filePath), lines);
        }


        private static void processSelectiveTables(Connection connection) throws SQLException {
            String selectTables = context.get("Selective_Tables");
            String connectionId = context.get("CONNECTION_ID");
            String querySchemaCondition1 = context.get("query_schema_cond1");

            // StringBuilder queryEltSelective = new StringBuilder();
            // queryEltSelective.append("SELECT DISTINCT ")
            //         .append("ELT_Selective_Source_Metadata.Connection_Id, ")
            //         .append("ELT_Selective_Source_Metadata.Schema_Name, ")
            //         .append("ELT_Selective_Source_Metadata.Table_Name, ")
            //         .append("Dimension_Transaction ")
            //         .append("FROM ELT_Selective_Source_Metadata ")
            //         .append("WHERE Table_Name IN (").append(selectTables).append(") ")
            //         .append("AND Isfileupload != '1' ")
            //         .append("AND Connection_Id = ? ").append(querySchemaCondition1);

            // StringBuilder queryEltIlSettings = new StringBuilder();
            // queryEltIlSettings.append("SELECT ")
            //         .append("ELT_IL_Settings_Info.Connection_Id, ")
            //         .append("ELT_IL_Settings_Info.Schema_Name, ")
            //         .append("ELT_IL_Settings_Info.Setting_Value ")
            //         .append("FROM ELT_IL_Settings_Info ")
            //         .append("WHERE Settings_Category = 'Suffix' ")
            //         .append("AND Active_Flag = '1' ")
            //         .append("AND Connection_Id = ? ").append(querySchemaCondition1);

            // List<Map<String, Object>> dfEltSelective = fetchDataFromDb(queryEltSelective, connectionId,connection);
            // List<Map<String, Object>> dfEltIlSettings = fetchDataFromDb(queryEltIlSettings, connectionId,connection);

            List<Map<String, Object>> dfOut = getSelectiveSourceMetadataJoinedWithSettings(selectTables, connectionId, querySchemaCondition1, connection);

            //List<Map<String, Object>> dfOut = mergeData(dfEltSelective, dfEltIlSettings);

            // Done inside the function

            // for (Map<String, Object> row : dfOut) {
            //     String tableName = (String) row.get("Table_Name");
            //     String settingValue = (String) row.get("Setting_Value");
            //     String dwTableName = settingValue == null ? tableName : tableName + "_" + settingValue;
            //     globalMap.put("DW_Table_Name", dwTableName);
            // }

            System.out.println(dfOut);
        }
        
        private static List<Map<String, Object>> fetchDataFromDb(StringBuilder queryEltIlSettings, String connectionId, Connection con) throws SQLException {
            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement stmt = con.prepareStatement(queryEltIlSettings.toString())) {
                stmt.setString(1, connectionId);
                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(metaData.getColumnName(i), rs.getObject(i));
                        }
                        results.add(row);
                    }
                }
            }
            return results;
        }

        // TODO : rename function Name
        public static List<Map<String, Object>> getSelectiveSourceMetadataWithSettingsSharedFolderWS (
                String selectiveTables, String connectionId, String querySchemaCond1, Connection con) throws SQLException {

            List<Map<String, Object>> results = new ArrayList<>();
            String query = "SELECT DISTINCT " +
                        "    main.Connection_Id, " +
                        "    main.Schema_Name, " +
                        "    main.Table_Name, " +
                        "    main.Dimension_Transaction, " +
                        "    main.Custom_Type, " +
                        "    il.Setting_Value AS IL_Setting_Value, " +
                        "    shared.suffix AS Shared_Folder_Suffix, " +
                        "    ws.suffix AS Webservice_Suffix " +
                        "FROM " +
                        "    ELT_Selective_Source_Metadata AS main " +
                        "LEFT OUTER JOIN " +
                        "    ELT_IL_Settings_Info AS il " +
                        "ON " +
                        "    main.Connection_Id = il.Connection_Id " +
                        "    AND main.Schema_Name = il.Schema_Name " +
                        "    AND il.Settings_Category = 'Suffix' " +
                        "    AND il.Active_Flag = '1' " +
                        "LEFT OUTER JOIN " +
                        "    sharedconnections_file_path_info AS shared " +
                        "ON " +
                        "    main.Connection_Id = shared.Connection_Id " +
                        "    AND main.Schema_Name = shared.param_or_schema_name " +
                        "LEFT OUTER JOIN " +
                        "    minidwcs_ws_connections_mst AS ws " +
                        "ON " +
                        "    main.Connection_Id = ws.id " +
                        "WHERE " +
                        "    main.Table_Name IN (" + selectiveTables + ") " + // TODO what if multiple files single quotes support
                        "    AND main.Isfileupload != '1' " +
                        "    AND main.Custom_Type <> 'Common' " +
                        "    AND main.Connection_Id = ? " +
                        querySchemaCond1 + ";";

            try (PreparedStatement stmt = con.prepareStatement(query)) {
                stmt.setString(1, connectionId);

                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();

                        for (int i = 1; i <= columnCount; i++) {
                            row.put(metaData.getColumnName(i), rs.getObject(i));
                        }

                        String schemaName = (String) row.get("Schema_Name");
                        //String tableName = (String) row.get("Table_Name");
                        //String settingValue = (String) row.get("Setting_Value");

                        String settingValue;
                        String customType = ""; // or null // TO Context variable ; intialize in context
                        if (customType == null) {
                            settingValue = (String) row.get("Shared_Folder_Suffix");
                        } else if (customType.equals("shared_folder")) {
                            settingValue = (String) row.get("Shared_Folder_Suffix");
                        } else if (customType.equals("web_service") ||
                            customType.equals("OneDrive") ||
                            customType.equals("SageIntacct")) {
                            settingValue = (String) row.get("Webservice_Suffix");
                        } else {
                            settingValue = (String) row.get("IL_Setting_Value");
                        }

                        String tableName = (String) row.get("Table_Name");  // ELT_Selective_Source_Metadata.Table_Name
                        String dwTableName = (settingValue != null) ? tableName + "_" + settingValue : tableName;

                        // Additional Output
// Setting_Value
// DW_Table_Name
                        results.add(row);
                    }
                }
            }

            return results;
        }

        private static List<Map<String, Object>> mergeData(List<Map<String, Object>> df1, List<Map<String, Object>> df2) {
            // Implement merging logic here
            // This is a simplified version and may need to be adjusted based on your specific requirements
            Map<String, Map<String, Object>> mergedMap = new HashMap<>();

            for (Map<String, Object> row : df1) {
                String key = row.get("Connection_Id") + "|" + row.get("Schema_Name");
                mergedMap.put(key, new HashMap<>(row));
            }

            for (Map<String, Object> row : df2) {
                String key = row.get("Connection_Id") + "|" + row.get("Schema_Name");
                if (mergedMap.containsKey(key)) {
                    mergedMap.get(key).putAll(row);
                } else {
                    mergedMap.put(key, new HashMap<>(row));
                }
            }

            return new ArrayList<>(mergedMap.values());
        }

        // Navin First non-iterative 
        public static List<Map<String, Object>> getSelectiveSourceMetadataJoinedWithSettings(String selectiveTables, String connectionId, String querySchemaCond1, Connection con) throws SQLException {
            List<Map<String, Object>> results = new ArrayList<>();
            // Left Outer Join
            String query = "SELECT DISTINCT " +
                        "    src.Connection_Id, " +
                        "    src.Schema_Name, " +
                        "    src.Table_Name, " +
                        "    src.Dimension_Transaction, " +
                        "    il.Setting_Value " +
                        "FROM " +
                        "    ELT_Selective_Source_Metadata AS src " +
                        "LEFT OUTER JOIN " +
                        "    ELT_IL_Settings_Info AS il " +
                        "ON " +
                        "    src.Connection_Id = il.Connection_Id " +
                        "    AND src.Schema_Name = il.Schema_Name " +
                        "    AND il.Settings_Category = 'Suffix' " +
                        "    AND il.Active_Flag = '1'" +
                        "WHERE " +
                        "    src.Table_Name IN (" + selectiveTables + ") " + // TODO if more than table
                        "    AND src.Isfileupload != '1' " +
                        "    AND src.Connection_Id = " + connectionId + " " +
                        querySchemaCond1 + ";";// TODO see how to  relate to first table
;

            try (PreparedStatement stmt = con.prepareStatement(query)) {
                //stmt.setString(1, connectionId);

                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(metaData.getColumnName(i), rs.getObject(i));
                        }
                        String tableName = (String) row.get("Table_Name");
                        String settingValue = (String) row.get("Setting_Value");
                        String dwTableName = (settingValue == null) ? tableName : tableName + "_" + settingValue;
                        row.put("DW_Table_Name", dwTableName);
                        results.add(row);
                    }
                }
            }
            return results;
        }

        private static Map<String, Object> updateIncrementalColumnAndPrepareBulk(Connection connection) throws SQLException, IOException {
            String tableName = context.get("Table_Name");
            String connectionId = context.get("CONNECTION_ID");
            String querySchemaCondition = context.get("query_schema_cond");
            String querySchemaCondition1 = context.get("query_schema_cond1");

            StringBuilder queryEltSelective = new StringBuilder();
            queryEltSelective.append("SELECT Connection_Id, Schema_Name, Table_Name, Column_Name, Dimension_Transaction ")
                    .append("FROM ELT_Selective_Source_Metadata ")
                    .append("WHERE Table_Name = ? ")
                    .append("AND IsFileUpload != '1' ")
                    .append("AND Connection_Id = ? ")
                    .append(querySchemaCondition1);

            StringBuilder queryEltIlMapping = new StringBuilder();
            queryEltIlMapping.append("SELECT Connection_Id, Table_Schema, Source_Table_Name, Source_Column_Name, ")
                    .append("Dimension_Transaction, Incremental_Column ")
                    .append("FROM ELT_IL_Source_Mapping_Info_Saved ")
                    .append("WHERE Source_Table_Name = ? ")
                    .append("AND Connection_Id = ? ")
                    .append(querySchemaCondition);

            List<Map<String, Object>> eltSelectiveData = new ArrayList<>();
            List<Map<String, Object>> eltIlMappingData = new ArrayList<>();

            try (PreparedStatement stmtEltSelective = connection.prepareStatement(queryEltSelective.toString());
                 PreparedStatement stmtEltIlMapping = connection.prepareStatement(queryEltIlMapping.toString())) {

                stmtEltSelective.setString(1, tableName);
                stmtEltSelective.setString(2, connectionId);
                stmtEltIlMapping.setString(1, tableName);
                stmtEltIlMapping.setString(2, connectionId);

                ResultSet rsEltSelective = stmtEltSelective.executeQuery();
                while (rsEltSelective.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Connection_Id", rsEltSelective.getString("Connection_Id"));
                    row.put("Schema_Name", rsEltSelective.getString("Schema_Name"));
                    row.put("Table_Name", rsEltSelective.getString("Table_Name"));
                    row.put("Column_Name", rsEltSelective.getString("Column_Name"));
                    row.put("Dimension_Transaction", rsEltSelective.getString("Dimension_Transaction"));
                    eltSelectiveData.add(row);
                }

                ResultSet rsEltIlMapping = stmtEltIlMapping.executeQuery();
                while (rsEltIlMapping.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Connection_Id", rsEltIlMapping.getString("Connection_Id"));
                    row.put("Table_Schema", rsEltIlMapping.getString("Table_Schema"));
                    row.put("Source_Table_Name", rsEltIlMapping.getString("Source_Table_Name"));
                    row.put("Source_Column_Name", rsEltIlMapping.getString("Source_Column_Name"));
                    row.put("Dimension_Transaction", rsEltIlMapping.getString("Dimension_Transaction"));
                    row.put("Incremental_Column", rsEltIlMapping.getString("Incremental_Column"));
                    eltIlMappingData.add(row);
                }
            }

            // Perform the join and transformation
            List<Map<String, Object>> mergedData = new ArrayList<>();
            for (Map<String, Object> eltRow : eltSelectiveData) {
                for (Map<String, Object> ilRow : eltIlMappingData) {
                    if (eltRow.get("Connection_Id").equals(ilRow.get("Connection_Id")) &&
                        eltRow.get("Table_Name").equals(ilRow.get("Source_Table_Name"))) {
                        Map<String, Object> mergedRow = new HashMap<>(eltRow);
                        mergedRow.putAll(ilRow);
                        mergedData.add(mergedRow);
                    }
                }
            }

            // Apply transformation logic
            List<Map<String, Object>> updateData = new ArrayList<>();
            List<Map<String, Object>> outData = new ArrayList<>();

            for (Map<String, Object> row : mergedData) {
                Map<String, Object> updateRow = new HashMap<>();
                updateRow.put("Connection_Id", row.get("Connection_Id"));
                updateRow.put("Table_Schema", row.get("Table_Schema"));
                updateRow.put("Source_Table_Name", row.get("Source_Table_Name"));
                updateRow.put("Source_Column_Name", row.get("Source_Column_Name"));
                updateRow.put("Dimension_Transaction", row.get("Dimension_Transaction"));

                String incrementalColumn = calculateIncrementalColumn(row);
                updateRow.put("Incremental_Column", incrementalColumn);
                updateRow.put("Active_Flag", false);

                updateData.add(updateRow);

                Map<String, Object> outRow = new HashMap<>(row);
                outData.add(outRow);
            }

            // Save updateData to CSV
            String bulkPath = context.get("BULK_PATH") + context.get("CLIENT_ID") + "_" + 
                              context.get("PACKAGE_ID") + "_" + context.get("JOB_NAME") + "_UPDATE_" + 
                              LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")) + ".csv";

            if (!updateData.isEmpty()) {
                saveToCsv(updateData, bulkPath);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("update_df", updateData);
            result.put("OUT", outData);
            return result;
        }

        private static String calculateIncrementalColumn(Map<String, Object> row) {
            String dimensionTransactionY = (String) row.get("Dimension_Transaction");
            String dimensionTransactionX = (String) row.get("Dimension_Transaction");
            String incrementalColumn = (String) row.get("Incremental_Column");

            if (dimensionTransactionY == null) {
                return "N";
            } else if ("T".equals(dimensionTransactionX) && "D".equals(dimensionTransactionY)) {
                return "Y";
            } else {
                return incrementalColumn;
            }
        }

        // Navin 
        // Custome Type is missing  case as well
        public static Map<String, Map<String, String>> fetchSelectiveSourceMetadata(
            Connection connection, 
            String tableNameList, 
            String connectionId, 
            String querySchemaCondition, 
            String customType) {
        
            String baseQuery = "SELECT " +
                            "Connection_Id, Schema_Name, Table_Name, Column_Name, Dimension_Transaction " +
                            "FROM `ELT_Selective_Source_Metadata` " +
                            "WHERE Table_Name IN (?) " +
                            "AND IsFileUpload != '1' ";
            
            // Add conditional part for Custom_Type if a non-empty string is passed
            if (customType != null && !customType.isEmpty()) {
                baseQuery += "AND Custom_Type <> ? ";
            }
            baseQuery += "AND Connection_Id = ? " + querySchemaCondition;
        
            Map<String, Map<String, String>> resultMap = new HashMap<>();
        
            try (PreparedStatement preparedStatement = connection.prepareStatement(baseQuery)) {
                preparedStatement.setString(1, tableNameList);
        
                int paramIndex = 2; // To handle optional customType
                if (customType != null && !customType.isEmpty()) {
                    preparedStatement.setString(paramIndex++, customType);
                }
                preparedStatement.setString(paramIndex, connectionId);
        
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        // Construct the key
                        String key = resultSet.getString("Connection_Id") + "_" +
                                    resultSet.getString("Schema_Name") + "_" +
                                    resultSet.getString("Table_Name") + "_" +
                                    resultSet.getString("Column_Name");
        
                        // Construct the value as a map of column names to their values
                        Map<String, String> rowMap = new HashMap<>();
                        rowMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                        rowMap.put("Schema_Name", resultSet.getString("Schema_Name"));
                        rowMap.put("Table_Name", resultSet.getString("Table_Name"));
                        rowMap.put("Column_Name", resultSet.getString("Column_Name"));
                        rowMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));
        
                        resultMap.put(key, rowMap);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception occurred while fetching selective source metadata: " + e.getMessage());
            }
        
            return resultMap;
        }

        // Navin
        // SAVED UNite part 2
        // Used in SAved FAS also same query

        public static Map<String, Object> getConstantAnvizentFields(
                Connection connection,
                String dataSourceName,
                String tableName,
                String companyId) {

            String query = "SELECT DISTINCT " +
                    "'DataSource_Id' AS IL_Column_Name, " +
                    "'varchar(50)' AS IL_Data_Type, " +
                    "'varchar' AS Source_Data_Type, " +
                    "'PK' AS Constraints, " +
                    "'PK' AS PK_Constraint, " +
                    "'Y' AS Constant_Insert_Column, " +
                    "'" + dataSourceName + "' AS Constant_Insert_Value " +
                    "FROM `ELT_Selective_Source_Metadata` " +
                    "UNION ALL " +
                    "SELECT DISTINCT '" + tableName + "_Key' AS IL_Column_Name, " +
                    "'bigint(32)' AS IL_Data_Type, " +
                    "'bigint' AS Source_Data_Type, " +
                    "'SK' AS Constraints, " +
                    "'SK' AS PK_Constraint, " +
                    "'N' AS Constant_Insert_Column, " +
                    "NULL AS Constant_Insert_Value " +
                    "FROM `ELT_Selective_Source_Metadata` " +
                    "UNION ALL " +
                    "SELECT DISTINCT 'Company_Id' AS IL_Column_Name, " +
                    "'varchar(50)' AS IL_Data_Type, " +
                    "'varchar' AS Source_Data_Type, " +
                    "'PK' AS Constraints, " +
                    "'PK' AS PK_Constraint, " +
                    "'Y' AS Constant_Insert_Column, " +
                    "'" + companyId + "' AS Constant_Insert_Value " +
                    "FROM `ELT_Selective_Source_Metadata`";

            Map<String, Object> resultMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("IL_Column_Name") + "_"
                                + resultSet.getString("Constant_Insert_Column");
                        Map<String, Object> transformedRow = new HashMap<>();

                        // Here, you can apply your transformations
                        transformedRow.put("IL_Column_Name", resultSet.getString("IL_Column_Name"));
                        transformedRow.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                        transformedRow.put("Source_Data_Type", resultSet.getString("Source_Data_Type"));
                        transformedRow.put("Constraints", resultSet.getString("Constraints"));
                        transformedRow.put("PK_Constraint", resultSet.getString("PK_Constraint"));
                        transformedRow.put("Constant_Insert_Column", resultSet.getString("Constant_Insert_Column"));
                        transformedRow.put("Constant_Insert_Value", resultSet.getObject("Constant_Insert_Value"));

                        resultMap.put(key, transformedRow); // Map key can be customized based on your transformation
                                                            // needs
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }

            return resultMap;
        }

        // Navin - Saved_FAS
        // ELT_Custom_Source_mapping
        public static Map<String, Map<String, String>> fetchCustomSourceMetadata(
                Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCondition) {

            String query = "SELECT " +
                    "`ELT_Custom_Source_Metadata_Info`.`Connection_Id`, " +
                    "`ELT_Custom_Source_Metadata_Info`.`Schema_Name`, " +
                    "`ELT_Custom_Source_Metadata_Info`.`Table_Name`, " +
                    "`ELT_Custom_Source_Mapping_Info`.`Column_Name`, " +
                    "CASE WHEN Constraints = 'PK' AND Column_Data_Type LIKE '%bit%' THEN 'tinyint(1)' " +
                    "ELSE Column_Data_Type END AS Column_Data_Type, " +
                    "`ELT_Custom_Source_Mapping_Info`.`Constraints`, " +
                    "CASE WHEN Constraints = 'PK' AND Column_Data_Type LIKE '%bit%' THEN 'tinyint' " +
                    "ELSE LOWER(SUBSTRING_INDEX(Column_Data_Type, '(', 1)) END AS Source_Data_Type, " +
                    "Source_Column_Name AS Source_Column_Name " +
                    "FROM `ELT_Custom_Source_Mapping_Info` " +
                    "JOIN ELT_Custom_Source_Metadata_Info ON ELT_Custom_Source_Mapping_Info.Custom_Id = ELT_Custom_Source_Metadata_Info.Id "
                    +
                    "WHERE Table_Name = ? AND Connection_Id = ? " + querySchemaCondition;

            Map<String, Map<String, String>> resultMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" +
                                resultSet.getString("Schema_Name") + "_" +
                                resultSet.getString("Table_Name") + "_" +
                                resultSet.getString("Column_Name");

                        Map<String, String> rowMap = new HashMap<>();
                        rowMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                        rowMap.put("Schema_Name", resultSet.getString("Schema_Name"));
                        rowMap.put("Table_Name", resultSet.getString("Table_Name"));
                        rowMap.put("Column_Name", resultSet.getString("Column_Name"));
                        rowMap.put("Column_Data_Type", resultSet.getString("Column_Data_Type"));
                        rowMap.put("Constraints", resultSet.getString("Constraints"));
                        rowMap.put("Source_Data_Type", resultSet.getString("Source_Data_Type"));
                        rowMap.put("Source_Column_Name", resultSet.getString("Source_Column_Name"));

                        resultMap.put(key, rowMap);
                    }
                }
            } catch (Exception e) {
                System.err.println("Exception occurred while fetching custom source metadata: " + e.getMessage());
            }

            return resultMap;
        }

        // navin -Saved_FAS
        // row9 - Source_mapping_info_saved
        // first merge 3rd input
        public static Map<String, Map<String, String>> fetchILSourceMappingInfoSaved(
                Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCondition,
                String customType) {

            String query = "SELECT " +
                    "`ELT_IL_Source_Mapping_Info_Saved`.`Connection_Id`, " +
                    "`ELT_IL_Source_Mapping_Info_Saved`.`Table_Schema`, " +
                    "`ELT_IL_Source_Mapping_Info_Saved`.`Source_Table_Name`, " +
                    "`ELT_IL_Source_Mapping_Info_Saved`.`Source_Column_Name`, " +
                    "IL_Column_Name, Updated_Date, Dimension_Transaction, Incremental_Column " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                    "WHERE Source_Table_Name = ? AND Connection_Id = ? " + querySchemaCondition;

            Map<String, Map<String, String>> resultMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        // String sourceColumnName = resultSet.getString("IL_Column_Name");
                        String sourceColumnName = resultSet.getString("Source_Column_Name");

                        // Apply transformation based on Custom_Type
                        if (customType != null &&
                                (customType.equals("web_service") ||
                                        customType.equals("shared_folder") ||
                                        customType.equals("OneDrive") ||
                                        customType.equals("SageIntacct"))) {
                            sourceColumnName = resultSet.getString("IL_Column_Name");
                            // } else {
                            // sourceColumnName = resultSet.getString("Source_Column_Name");
                            // }

                            String key = resultSet.getString("Connection_Id") + "_" +
                                    resultSet.getString("Table_Schema") + "_" +
                                    resultSet.getString("Source_Table_Name") + "_" +
                                    sourceColumnName;

                            Map<String, String> rowMap = new HashMap<>();
                            rowMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                            rowMap.put("Table_Schema", resultSet.getString("Table_Schema"));
                            rowMap.put("Source_Table_Name", resultSet.getString("Source_Table_Name"));
                            rowMap.put("Source_Column_Name", sourceColumnName);
                            rowMap.put("IL_Column_Name", resultSet.getString("IL_Column_Name"));
                            rowMap.put("Updated_Date", resultSet.getString("Updated_Date"));
                            rowMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));
                            rowMap.put("Incremental_Column", resultSet.getString("Incremental_Column"));

                            resultMap.put(key, rowMap);
                        }
                    }
                } catch (Exception e) {
                    System.err.println(
                            "Exception occurred while fetching IL source mapping info saved: " + e.getMessage());
                }
            } catch (Exception e) {
                System.err.println(
                        "Exception occurred while fetching IL source mapping info saved: " + e.getMessage());
            }
            return resultMap;
        }

        // Navin Saved comp1 start
        // For Inner Join + Anti Join
        public Map<String, Map<String, Object>> processSelectiveSourceMetadata(
                Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCond) {

            String query = "SELECT Connection_Id, Schema_Name, Table_Name, Column_Name, Dimension_Transaction" +
                    "FROM ELT_Selective_Source_Metadata " +
                    "WHERE Table_Name = ? AND IsFileUpload != '1' AND Connection_Id = ? " + querySchemaCond;

            // Joining Table data
            Map<String, Map<String, Object>> ilSourceMappingData = getILSourceMappingInfo(connection, query, query, query);

            Map<String, Map<String, Object>> innerJoinResultMap = new HashMap<>(); // Inner Join Result Map; Store into a file
            Map<String, Map<String, Object>> antiInnerJoinResultMap = new HashMap<>(); // Anti-Inner Join Result Map; output for further processing

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = String.join("_",
                                resultSet.getString("Connection_Id"),
                                resultSet.getString("Schema_Name"),
                                resultSet.getString("Table_Name"),
                                resultSet.getString("Column_Name"));

                        // inner join with the ilSourceMappingData map (inner join logic)
                        if (ilSourceMappingData.containsKey(key)) {
                            Map<String, Object> valueMap = new HashMap<>();
                            Map<String, Object> ilmappingValue = ilSourceMappingData.get(key);

                            String icremental_Column = ilmappingValue.get("Dimension_Transaction") == null ? "N"
                                    : ("T".equals(resultSet.getString("Dimension_Transaction"))
                                            && "D".equals(ilmappingValue.get("Dimension_Transaction"))) ? "Y"
                                                    : (String) ilmappingValue.get("Incremental_Column");

                            // components of the key
                            valueMap.putAll(ilSourceMappingData.get(key));
                            // Updated values
                            valueMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));
                            valueMap.put("Incremental_Column", icremental_Column);
                            valueMap.put("Active_Flag", false);

                            innerJoinResultMap.put(key, valueMap);
                        }
                        // anti-join with the ilSourceMappingData map (!ilSourceMappingData.containsKey(key))
                        else {
                            Map<String, Object> valueMap = new HashMap<>();
                            valueMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                            valueMap.put("Schema_Name", resultSet.getString("Schema_Name"));
                            valueMap.put("Table_Name", resultSet.getString("Table_Name"));
                            valueMap.put("Column_Name", resultSet.getString("Column_Name"));
                            valueMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));

                            antiInnerJoinResultMap.put(key, valueMap);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // TODO Save innerJoinResultMap into the table; The data doesn't comprise all the fields
            return antiInnerJoinResultMap;
        }

        public Map<String, Map<String, Object>> getILSourceMappingInfo(
                Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCond) {

            String query = "SELECT Connection_Id, Table_Schema, Source_Table_Name, Source_Column_Name, " +
                    "Dimension_Transaction, Incremental_Column " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Source_Table_Name = ? AND Connection_Id = ? " + querySchemaCond;

            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = String.join("_",
                                resultSet.getString("Connection_Id"),
                                resultSet.getString("Table_Schema"),
                                resultSet.getString("Source_Table_Name"),
                                resultSet.getString("Source_Column_Name"));

                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));
                        valueMap.put("Incremental_Column", resultSet.getString("Incremental_Column"));

                        resultMap.put(key, valueMap);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace(); // Log or handle the exception as needed
            }

            return resultMap;
        }

        public Map<String, Map<String, Object>> getSourceMetadata(Connection connection, String tableName, String connectionId, String querySchemaCond) throws SQLException {
            // Define the query using StringBuilder for compactness
            StringBuilder querySourceMetadata = new StringBuilder();
            querySourceMetadata.append("SELECT ")
                        .append("`Connection_Id`, ").append("`TABLE_SCHEMA`, ").append(".`Table_Name`, ").append(".`Column_Name`, ")
                        .append("LOWER(Data_Type) AS Data_Type, ")
                        .append("`PK_Column_Name`, ").append("`PK_Constraint`, ")
                        .append("`FK_Column_Name`, ").append("`FK_Constraint`, ")
                        .append("`Prefix_Suffix_Flag`, ").append("`Prefix_Suffix`, ")
                        .append("`Added_Date`, ").append("`Added_User`, ").append("`Updated_Date`, ").append("`Updated_User`, ")
                        .append("CASE WHEN Character_Max_Length < 0 THEN `Character_Max_Length` ")
                        .append("WHEN Data_Type LIKE '%char%' AND Character_Max_Length < 7 THEN '7' ELSE `Character_Max_Length` END AS Character_Max_Length, ")
                        .append("`Character_Octet_Length`, ")
                        .append("`Numeric_Precision`, ")
                        .append("CASE WHEN `Numeric_Scale` IS NULL THEN Numeric_Precision_Radix ELSE Numeric_Scale END AS Numeric_Scale ")
                        .append("FROM `ELT_Source_Metadata` ")
                        .append("WHERE Table_Name = ? AND Connection_Id = ? ")
                        .append(querySchemaCond); // Add dynamic schema condition
        
                        // Query for ELT_Source_Metadata from other source
            StringBuilder querySourceMetadataDummy = new StringBuilder();
            querySourceMetadata.append("SELECT Connection_Id, TABLE_SCHEMA, Table_Name, Column_Name, ")
                               .append("LOWER(Data_Type) AS Data_Type, PK_Column_Name, PK_Constraint, ")
                               .append("FK_Column_Name, FK_Constraint, Prefix_Suffix_Flag, Prefix_Suffix, ")
                               .append("Added_Date, Added_User, Updated_Date, Updated_User, ")
                               .append("CASE ")
                               .append("    WHEN Character_Max_Length < 0 THEN Character_Max_Length ")
                               .append("    WHEN Data_Type LIKE '%char%' AND Character_Max_Length < 7 THEN 7 ")
                               .append("    ELSE Character_Max_Length ")
                               .append("END AS Character_Max_Length, ")
                               .append("Character_Octet_Length, Numeric_Precision, ")
                               .append("CASE ")
                               .append("    WHEN Numeric_Scale IS NULL THEN Numeric_Precision_Radix ")
                               .append("    ELSE Numeric_Scale ")
                               .append("END AS Numeric_Scale ")
                               .append("FROM ELT_Source_Metadata ")
                               .append("WHERE Table_Name = ? ")
                               .append("AND Connection_Id = ? ")
                               .append(querySchemaCond);

            try (PreparedStatement preparedStatement = connection.prepareStatement(querySourceMetadata.toString())) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);
        
                ResultSet resultSet = preparedStatement.executeQuery();
                Map<String, Map<String, Object>> resultMap = new HashMap<>();
                
                while (resultSet.next()) {
                    String connectionIdResult = resultSet.getString("Connection_Id");
                    String tableSchema = resultSet.getString("TABLE_SCHEMA");
                    String tableNameResult = resultSet.getString("Table_Name");
                    String columnName = resultSet.getString("Column_Name");
        
                    String key = connectionIdResult + "_" + tableSchema + "_" + tableNameResult + "_" + columnName;
        
                    // Put the result set data into the map
                    Map<String, Object> valueMap = new HashMap<>();
                    
                    String dataType = resultSet.getString("Data_Type");
                    valueMap.put("Source_Data_Type", dataType);
                    String transformedDataType = getTransformedDataType(dataType);
                    valueMap.put("Data_Type", transformedDataType);

                    valueMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                    valueMap.put("TABLE_SCHEMA", resultSet.getString("TABLE_SCHEMA"));
                    valueMap.put("Table_Name", resultSet.getString("Table_Name"));
                    valueMap.put("Column_Name", resultSet.getString("Column_Name"));

                    //valueMap.put("Data_Type", resultSet.getString("LOWER(Data_Type)"));
                    valueMap.put("PK_Column_Name", resultSet.getString("PK_Column_Name"));
                    valueMap.put("PK_Constraint", resultSet.getString("PK_Constraint"));
                    valueMap.put("FK_Column_Name", resultSet.getString("FK_Column_Name"));
                    valueMap.put("FK_Constraint", resultSet.getString("FK_Constraint"));
                    valueMap.put("Prefix_Suffix_Flag", resultSet.getString("Prefix_Suffix_Flag"));
                    valueMap.put("Prefix_Suffix", resultSet.getString("Prefix_Suffix"));
                    valueMap.put("Added_Date", resultSet.getTimestamp("Added_Date"));
                    valueMap.put("Added_User", resultSet.getString("Added_User"));
                    valueMap.put("Updated_Date", resultSet.getTimestamp("Updated_Date"));
                    valueMap.put("Updated_User", resultSet.getString("Updated_User"));
                    valueMap.put("Character_Max_Length", resultSet.getInt("Character_Max_Length"));
                    valueMap.put("Character_Octet_Length", resultSet.getInt("Character_Octet_Length"));
                    valueMap.put("Numeric_Precision", resultSet.getInt("Numeric_Precision"));
                    valueMap.put("Numeric_Scale", resultSet.getInt("Numeric_Scale"));
        
                    // Add the key-value pair to the map
                    resultMap.put(key, valueMap);
                }
                return resultMap;
            }
        }

        // Original but difficult to maintaim

        // private void getTransformedDataType(String dataType) {
        //     dataType = (dataType.equals("smallidentity") || dataType.equals("identity") ||
        //             dataType.equals("bigidentity") || dataType.equals("ubigint"))
        //                     ? "bigint"
        //                     : (dataType.contains("float")) ? "float"
        //                             : (dataType.equals("longvarbinary")
        //                                     || dataType.contains("longvarchar"))
        //                                             ? "blob"
        //                                             : (dataType.contains("integer")) ? "int"
        //                                                     : (dataType.contains("numeric")) ? "numeric"
        //                                                             : (dataType.contains("tinyint"))
        //                                                                     ? "tinyint"
        //                                                                     : (dataType
        //                                                                             .contains("smallint"))
        //                                                                                     ? "smallint"
        //                                                                                     : (dataType.contains("currency")) ? "decimal" : dataType;
        // }
        
        private String getTransformedDataType(String dataType) {
            if (dataType.equals("smallidentity") || dataType.equals("identity") ||
                dataType.equals("bigidentity") || dataType.equals("ubigint")) {
                dataType = "bigint";
            } else if (dataType.contains("float")) {
                dataType = "float";
            } else if (dataType.equals("longvarbinary") || dataType.contains("longvarchar")) {
                dataType = "blob";
            } else if (dataType.contains("integer")) {
                dataType = "int";
            } else if (dataType.contains("numeric")) {
                dataType = "numeric";
            } else if (dataType.contains("tinyint")) {
                dataType = "tinyint";
            } else if (dataType.contains("smallint")) {
                dataType = "smallint";
            } else if (dataType.contains("currency")) {
                dataType = "decimal";
            }

            return dataType;
        }
        
        public Map<String, Map<String, Object>> leftOuterJoin(
                Map<String, Map<String, Object>> mainData,
                Map<String, Map<String, Object>> lookupData) {

            Map<String, Map<String, Object>> result = new HashMap<>();
            Map<String, Object> resultMap = new HashMap<>();

            for (Map.Entry<String, Map<String, Object>> entry : mainData.entrySet()) {
                String mainKey = entry.getKey();
                Map<String, Object> mainValue = entry.getValue();

                Map<String, Object> lookupMetaData = lookupData.getOrDefault(mainKey, new HashMap<>());

                // If a corresponding key exists in lookup data, perform a left outer join
                String dataType = (String) lookupMetaData.get("Data_Type");
                // Map<String, Object> lookupValue = lookupData.getOrDefault(mainKey, new
                // HashMap<>());
                // Retrieve the lookup value
                Map<String, Object> lookupDataTypesCoverssions = lookupData.getOrDefault(dataType, new HashMap<>());

                resultMap.putAll(lookupMetaData);
                resultMap.putAll(mainValue);
                // Put the joined data (main value + lookup value) into the result map
                Map<String, Object> resultValue = new HashMap<>(mainValue);
                resultValue.put("lookup_value", lookupMetaData); // Example key for lookup value

                Map<String, Object> outputMap = new HashMap<>();
                outputMap.put("Connection_Id", mainValue.get("Connection_Id"));
                outputMap.put("Table_Name", mainValue.get("Table_Name"));
                outputMap.put("Column_Name", mainValue.get("Column_Name"));

                Integer maxLength = (Integer) lookupMetaData.get("Character_Max_Length");
                int characterMaxLength = characterMaxLength(maxLength);
                String ilDataType = getIlDataType(mainValue, characterMaxLength);
                ilDataType = (maxLength == null) ? ilDataType
                        : (maxLength == -1) ? "text" : ilDataType;
                outputMap.put("Data_Type", ilDataType);

                outputMap.put("TABLE_SCHEMA", lookupMetaData.get("TABLE_SCHEMA"));
                outputMap.put("IL_Data_Type", lookupMetaData.get("Connection_Id"));
                outputMap.put("PK_Column_Name", lookupMetaData.get("PK_Column_Name"));
                outputMap.put("PK_Constraint", lookupMetaData.get("PK_Constraint"));
                outputMap.put("FK_Column_Name", lookupMetaData.get("FK_Column_Name"));
                outputMap.put("FK_Constraint", lookupMetaData.get("FK_Constraint"));
                outputMap.put("Prefix_Suffix_Flag", lookupMetaData.get("Prefix_Suffix_Flag"));
                outputMap.put("Prefix_Suffix", lookupMetaData.get("Prefix_Suffix"));

                outputMap.put("Schema_Name", mainValue.get("Schema_Name"));
                outputMap.put("Dimension_Transaction", mainValue.get("Dimension_Transaction"));

                outputMap.put("Added_Date", ""); // TODO
                outputMap.put("Added_User", ""); // TODO
                outputMap.put("Updated_Date", ""); // TODO
                outputMap.put("Updated_User", ""); // TODO

                result.put(mainKey, resultValue);
            }

            return result;
        }

        private int characterMaxLength(Integer maxLength) {
            //Integer characterMaxLength = Integer.parseInt(len);
            int length =  (maxLength != null) ? 2 * maxLength : 0;
            return (length > 255) ? 255 : length; 
        }

        private String getIlDataType(Map<String, Object> value, int maxLength) {
            String dataType = (String) value.get("Data_Type");
            String defaultFlag = (String) value.get("Default_Flag");
            Integer numericPrecision = (Integer) value.get("Numeric_Precision");
            Integer numericScale = (Integer) value.get("Numeric_Scale");
            Integer characterMaxLength = (Integer) value.get("Character_Max_Length"); // transformed

            if (defaultFlag == null) {
                return dataType;
            }
            if ("y".equalsIgnoreCase(defaultFlag)) {
                return (String) value.get("IL_Data_Type");
            }
            if ("decimal".equals(dataType) || "numeric".equals(dataType) || "number".equals(dataType) || "double".equals(dataType)) {
                return String.format("decimal(%d,%d)", numericPrecision, numericScale);
            }
            if (dataType.contains("int")) {
                return String.format("%s(%d)", dataType, numericPrecision);
            }
            if ("varchar".equals(dataType) || "char".equals(dataType) || "mediumtext".equals(dataType)) {
                return String.format("%s(%d)", dataType, characterMaxLength); // TODO
            }
            if ("varchar2".equals(dataType) || "nvarchar".equals(dataType)) {
                return String.format("varchar(%d)", maxLength);
            }
            if ("nchar".equals(dataType)) {
                return String.format("char(%d)", maxLength);
            }
            if (dataType.contains("bit")) {
                return "bit(1)";
            }
            if ("float".equals(dataType)) {
                return String.format("float(%d,%d)", numericPrecision, numericScale);
            }
            return dataType;
        }
        // Navin
        // FAS Unite comp 2  2
        // Saved Comp1 - start Query
        // SaVED cOMP2 - 2
        public Map<String, Object> executeQueryAndBuildMapWithTransaction(
                Connection connection,
                String connectionId,
                String tableName,
                String querySchemaCondition) {

            String query = "SELECT DISTINCT " +
                    "`Connection_Id`, " +
                    "`Schema_Name`, " +
                    "`Table_Name`, " +
                    "`Dimension_Transaction` " +
                    "FROM `ELT_Selective_Source_Metadata` " +
                    "WHERE `Table_Name` = ? " +
                    "AND `IsFileUpload` != '1' " +
                    "AND `Connection_Id` = ? " +
                    querySchemaCondition;

            Map<String, Object> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" +
                                resultSet.getString("Schema_Name") + "_" +
                                resultSet.getString("Table_Name");
                        Object value = resultSet.getObject("Dimension_Transaction");
                        resultMap.put(key, value);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }
            return resultMap;
        }

        // Navin FAS comp2 tmap3
        // Settings 
        public static Map<String, Object> getActiveAliasValues(
                Connection connection,
                String connectionId,
                String querySchemaCondition) {

            String query = "SELECT " +
                    "`Connection_Id`, " +
                    "`Schema_Name`, " +
                    "`Setting_Value` " +
                    "FROM `ELT_IL_Settings_Info` " +
                    "WHERE `Settings_Category` = 'Suffix' " +
                    "AND `Active_Flag` = '1' " +
                    "AND `Connection_Id` = ? " +
                    querySchemaCondition;

            Map<String, Object> resultMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" + resultSet.getString("Schema_Name");
                        Object value = resultSet.getObject("Setting_Value");
                        resultMap.put(key, value);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }

            return resultMap;
        }

        // Navin
        // Sharedfolder compoennt
        public static Map<String, Object> executeQueryAndBuildMap(
                Connection connection,
                String connectionId) {

            String query = "SELECT " +
                    "`Connection_Id`, " +
                    "`param_or_schema_name` AS Schema_name, " +
                    "`suffix` " +
                    "FROM `sharedconnections_file_path_info` " +
                    "WHERE `Connection_Id` = ?";

            Map<String, Object> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" + resultSet.getString("Schema_name");
                        Object value = resultSet.getObject("suffix");
                        resultMap.put(key, value);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }
            return resultMap;
        }

        // Navin FAS
        // webservice
        public static Map<String, Object> getWSConnectionData(
                Connection connection,
                String connectionId) {

            String query = "SELECT " +
                    "`id` AS Connection_Id, " +
                    "`suffix` " +
                    "FROM `minidwcs_ws_connections_mst` " +
                    "WHERE `id` = ?";

            Map<String, Object> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id");
                        Object value = resultSet.getObject("suffix");
                        resultMap.put(key, value);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }
            return resultMap;
        }
        // Navin
        // FAS - Data Type Conversions - Outer joined but not effective in FAS
        // SAVED - used and effective
        public Map<String, Map<String, Object>> getDatatypeConversions(Connection connection) {
            String query = "SELECT Source_Data_Type, IL_Data_Type, Default_Flag, Precision_Flag, Default_Length " +
                           "FROM ELT_Datatype_Conversions";
        
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String sourceDataType = resultSet.getString("Source_Data_Type");
        
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                    valueMap.put("Default_Flag", resultSet.getString("Default_Flag"));
                    valueMap.put("Precision_Flag", resultSet.getString("Precision_Flag"));
                    valueMap.put("Default_Length", resultSet.getInt("Default_Length"));
        
                    resultMap.put(sourceDataType, valueMap);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        private static List<Map<String, Object>> joinAndCalculateIlDataType(Connection connection, List<Map<String, Object>> OUT) throws SQLException {
            String tableName = context.get("Table_Name");
            String connectionId = context.get("CONNECTION_ID");
            String querySchemaCondition = context.get("query_schema_cond");

            // Query for datatypes
            String queryDatatypes = "SELECT LOWER(Source_Data_Type) as Source_Data_Type, " +
                                    "IL_Data_Type, Default_Flag, Precision_Flag, Default_Length " +
                                    "FROM ELT_Datatype_Conversions";

            // Query for ELT_Source_Metadata
            StringBuilder querySourceMetadata = new StringBuilder();
            querySourceMetadata.append("SELECT Connection_Id, TABLE_SCHEMA, Table_Name, Column_Name, ")
                               .append("LOWER(Data_Type) AS Data_Type, PK_Column_Name, PK_Constraint, ")
                               .append("FK_Column_Name, FK_Constraint, Prefix_Suffix_Flag, Prefix_Suffix, ")
                               .append("Added_Date, Added_User, Updated_Date, Updated_User, ")
                               .append("CASE ")
                               .append("    WHEN Character_Max_Length < 0 THEN Character_Max_Length ")
                               .append("    WHEN Data_Type LIKE '%char%' AND Character_Max_Length < 7 THEN 7 ")
                               .append("    ELSE Character_Max_Length ")
                               .append("END AS Character_Max_Length, ")
                               .append("Character_Octet_Length, Numeric_Precision, ")
                               .append("CASE ")
                               .append("    WHEN Numeric_Scale IS NULL THEN Numeric_Precision_Radix ")
                               .append("    ELSE Numeric_Scale ")
                               .append("END AS Numeric_Scale ")
                               .append("FROM ELT_Source_Metadata ")
                               .append("WHERE Table_Name = ? ")
                               .append("AND Connection_Id = ? ")
                               .append(querySchemaCondition);

            List<Map<String, Object>> datatypes = new ArrayList<>();
            List<Map<String, Object>> sourceMetadata = new ArrayList<>();

            // Fetch datatypes
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(queryDatatypes)) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Source_Data_Type", rs.getString("Source_Data_Type"));
                    row.put("IL_Data_Type", rs.getString("IL_Data_Type"));
                    row.put("Default_Flag", rs.getString("Default_Flag"));
                    row.put("Precision_Flag", rs.getString("Precision_Flag"));
                    row.put("Default_Length", rs.getInt("Default_Length"));
                    datatypes.add(row);
                }
            }

            // Fetch source metadata
            try (PreparedStatement pstmt = connection.prepareStatement(querySourceMetadata.toString())) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("TABLE_SCHEMA", rs.getString("TABLE_SCHEMA"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Column_Name", rs.getString("Column_Name"));
                        row.put("Data_Type", rs.getString("Data_Type"));
                        row.put("PK_Column_Name", rs.getString("PK_Column_Name"));
                        row.put("PK_Constraint", rs.getString("PK_Constraint"));
                        row.put("FK_Column_Name", rs.getString("FK_Column_Name"));
                        row.put("FK_Constraint", rs.getString("FK_Constraint"));
                        row.put("Prefix_Suffix_Flag", rs.getString("Prefix_Suffix_Flag"));
                        row.put("Prefix_Suffix", rs.getString("Prefix_Suffix"));
                        row.put("Added_Date", rs.getTimestamp("Added_Date"));
                        row.put("Added_User", rs.getString("Added_User"));
                        row.put("Updated_Date", rs.getTimestamp("Updated_Date"));
                        row.put("Updated_User", rs.getString("Updated_User"));
                        row.put("Character_Max_Length", rs.getInt("Character_Max_Length"));
                        row.put("Character_Octet_Length", rs.getInt("Character_Octet_Length"));
                        row.put("Numeric_Precision", rs.getInt("Numeric_Precision"));
                        row.put("Numeric_Scale", rs.getInt("Numeric_Scale"));
                        sourceMetadata.add(row);
                    }
                }
            }

            // Perform left outer join between OUT, sourceMetadata, and datatypes
            List<Map<String, Object>> mergedData = OUT.stream()
                .flatMap(outRow -> sourceMetadata.stream()
                    .filter(metaRow -> outRow.get("Connection_Id").equals(metaRow.get("Connection_Id"))
                                        && outRow.get("Table_Name").equals(metaRow.get("Table_Name"))
                                        && outRow.get("Column_Name").equals(metaRow.get("Column_Name")))
                    .flatMap(metaRow -> datatypes.stream()
                        .filter(dtRow -> metaRow.get("Data_Type").equals(dtRow.get("Source_Data_Type")))
                        .map(dtRow -> {
                            Map<String, Object> mergedRow = new HashMap<>(outRow);
                            mergedRow.putAll(metaRow);
                            mergedRow.putAll(dtRow);
                            return mergedRow;
                        })))
                .collect(Collectors.toList());

            // Calculate Var1 and IL_Data_Type
            return mergedData.stream().map(row -> {
                int var1 = calculateVar1(row);
                String ilDataType = calculateIlDataType(row);
                row.put("Var_var1", var1);
                row.put("IL_Data_Type", ilDataType);
                row.put("Var_length", var1 > 255 ? 255 : var1);
                return row;
            }).collect(Collectors.toList());
        }

        
        private static List<Map<String, Object>> mergeMetadataAndCalculateConstraints(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
            String connectionId = context.get("CONNECTION_ID");
            String tableName = context.get("Table_Name");
            String querySchemaCondition = context.get("query_schema_cond");
            String querySchemaCondition1 = context.get("query_schema_cond1");

            // Query for ELT_Source_Metadata with PK constraint
            StringBuilder queryPkConstraint = new StringBuilder();
            queryPkConstraint.append("SELECT Connection_Id, TABLE_SCHEMA, Table_Name, Column_Name, ")
                             .append("PK_Column_Name AS FK_Column_Name, PK_Constraint AS FK_Constraint ")
                             .append("FROM ELT_Source_Metadata ")
                             .append("WHERE PK_Constraint = 'yes' ")
                             .append("AND Table_Name = ? ")
                             .append("AND Connection_Id = ? ")
                             .append(querySchemaCondition);

            // Query for ELT_Selective_Source_Metadata
            StringBuilder querySelectiveMetadata = new StringBuilder();
            querySelectiveMetadata.append("SELECT Connection_Id, Schema_Name, Table_Name, Column_Name ")
                                  .append("FROM ELT_Selective_Source_Metadata ")
                                  .append("WHERE IsFileUpload != '1' ")
                                  .append("AND Table_Name = ? ")
                                  .append("AND Connection_Id = ? ")
                                  .append(querySchemaCondition1)
                                  .append(" AND Dimension_Transaction = 'T'");

            // Query for ELT_IL_Settings_Info
            StringBuilder queryIlSettings = new StringBuilder();
            queryIlSettings.append("SELECT Connection_Id, Schema_Name, Setting_Value ")
                           .append("FROM ELT_IL_Settings_Info ")
                           .append("WHERE Settings_Category = 'Suffix' ")
                           .append("AND Active_Flag = '1' ")
                           .append("AND Connection_Id = ? ")
                           .append(querySchemaCondition1);

            List<Map<String, Object>> pkConstraintData = new ArrayList<>();
            List<Map<String, Object>> selectiveMetadataData = new ArrayList<>();
            List<Map<String, Object>> ilSettingsData = new ArrayList<>();

            // Fetch PK constraint data
            try (PreparedStatement pstmt = connection.prepareStatement(queryPkConstraint.toString())) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("TABLE_SCHEMA", rs.getString("TABLE_SCHEMA"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Column_Name", rs.getString("Column_Name"));
                        row.put("FK_Column_Name", rs.getString("FK_Column_Name"));
                        row.put("FK_Constraint", rs.getString("FK_Constraint"));
                        pkConstraintData.add(row);
                    }
                }
            }

            // Fetch selective metadata data
            try (PreparedStatement pstmt = connection.prepareStatement(querySelectiveMetadata.toString())) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Column_Name", rs.getString("Column_Name"));
                        selectiveMetadataData.add(row);
                    }
                }
            }

            // Fetch IL settings data
            try (PreparedStatement pstmt = connection.prepareStatement(queryIlSettings.toString())) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Setting_Value", rs.getString("Setting_Value"));
                        ilSettingsData.add(row);
                    }
                }
            }

            // Perform joins and calculate constraints
            List<Map<String, Object>> result = eltSourceMetadataAdd.stream()
                .flatMap(addRow -> pkConstraintData.stream()
                    .filter(pkRow -> addRow.get("Connection_Id").equals(pkRow.get("Connection_Id"))
                                      && addRow.get("TABLE_SCHEMA").equals(pkRow.get("TABLE_SCHEMA"))
                                      && addRow.get("Column_Name").equals(pkRow.get("Column_Name")))
                    .flatMap(pkRow -> selectiveMetadataData.stream()
                        .filter(selRow -> pkRow.get("Connection_Id").equals(selRow.get("Connection_Id"))
                                          && pkRow.get("Table_Name").equals(selRow.get("Table_Name"))
                                          && pkRow.get("Column_Name").equals(selRow.get("Column_Name")))
                        .flatMap(selRow -> ilSettingsData.stream()
                            .filter(ilRow -> selRow.get("Connection_Id").equals(ilRow.get("Connection_Id"))
                                             && selRow.get("Schema_Name").equals(ilRow.get("Schema_Name")))
                            .map(ilRow -> {
                                Map<String, Object> mergedRow = new HashMap<>(addRow);
                                mergedRow.putAll(pkRow);
                                mergedRow.putAll(selRow);
                                mergedRow.putAll(ilRow);
                                return calculateConstraints(mergedRow);
                            }))))
                .collect(Collectors.toList());

            return result;
        }
        
        
        private static List<Map<String, Object>> createMetadataConstants(Connection connection) throws SQLException {
            String connectionId = context.get("CONNECTION_ID");
            String schemaName = context.get("Schema_Name");
            String tableName = context.get("Table_Name");
            String dataSourceName = context.get("DATASOURCENAME");
            String querySchemaCondition1 = context.get("query_schema_cond1");

            // Query for constant metadata
            StringBuilder queryConstantMetadata = new StringBuilder();
            queryConstantMetadata.append("SELECT DISTINCT ")
                                 .append("'DataSource_Id' AS IL_Column_Name, ")
                                 .append("'varchar(50)' AS IL_Data_Type, ")
                                 .append("'varchar' AS Source_Data_Type, ")
                                 .append("'PK' AS Constraints, ")
                                 .append("'PK' AS PK_Constraint, ")
                                 .append("'Y' AS Constant_Insert_Column, ")
                                 .append("? AS Constant_Insert_Value ")
                                 .append("FROM ELT_Selective_Source_Metadata ")
                                 .append("UNION ALL ")
                                 .append("SELECT DISTINCT ")
                                 .append("? AS IL_Column_Name, ")
                                 .append("'bigint(32)' AS IL_Data_Type, ")
                                 .append("'bigint' AS Source_Data_Type, ")
                                 .append("'SK' AS Constraints, ")
                                 .append("'SK' AS PK_Constraint, ")
                                 .append("'N' AS Constant_Insert_Column, ")
                                 .append("NULL AS Constant_Insert_Value ")
                                 .append("FROM ELT_Selective_Source_Metadata ")
                                 .append("UNION ALL ")
                                 .append("SELECT DISTINCT ")
                                 .append("'Company_Id' AS IL_Column_Name, ")
                                 .append("'varchar(50)' AS IL_Data_Type, ")
                                 .append("'varchar' AS Source_Data_Type, ")
                                 .append("'PK' AS Constraints, ")
                                 .append("'PK' AS PK_Constraint, ")
                                 .append("NULL AS Constant_Insert_Column, ")
                                 .append("NULL AS Constant_Insert_Value ")
                                 .append("FROM ELT_Selective_Source_Metadata");

            // Query for ELT_Selective_Source_Metadata
            StringBuilder querySelectiveMetadata = new StringBuilder();
            querySelectiveMetadata.append("SELECT DISTINCT ")
                                  .append("Connection_Id, Schema_Name, Table_Name, Dimension_Transaction ")
                                  .append("FROM ELT_Selective_Source_Metadata ")
                                  .append("WHERE Table_Name = ? ")
                                  .append("AND IsFileUpload != '1' ")
                                  .append("AND Connection_Id = ? ")
                                  .append(querySchemaCondition1);

            // Query for ELT_IL_Settings_Info
            StringBuilder queryIlSettings = new StringBuilder();
            queryIlSettings.append("SELECT ")
                           .append("Connection_Id, Schema_Name, Setting_Value ")
                           .append("FROM ELT_IL_Settings_Info ")
                           .append("WHERE Settings_Category = 'Suffix' ")
                           .append("AND Active_Flag = '1' ")
                           .append("AND Connection_Id = ? ")
                           .append(querySchemaCondition1);

            List<Map<String, Object>> constantMetadata = new ArrayList<>();
            List<Map<String, Object>> selectiveMetadata = new ArrayList<>();
            List<Map<String, Object>> ilSettings = new ArrayList<>();

            // Fetch constant metadata
            try (PreparedStatement pstmt = connection.prepareStatement(queryConstantMetadata.toString())) {
                pstmt.setString(1, dataSourceName);
                pstmt.setString(2, tableName + "_Key");
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("IL_Column_Name", rs.getString("IL_Column_Name"));
                        row.put("IL_Data_Type", rs.getString("IL_Data_Type"));
                        row.put("Source_Data_Type", rs.getString("Source_Data_Type"));
                        row.put("Constraints", rs.getString("Constraints"));
                        row.put("PK_Constraint", rs.getString("PK_Constraint"));
                        row.put("Constant_Insert_Column", rs.getString("Constant_Insert_Column"));
                        row.put("Constant_Insert_Value", rs.getString("Constant_Insert_Value"));
                        constantMetadata.add(row);
                    }
                }
            }

            // Fetch selective metadata
            try (PreparedStatement pstmt = connection.prepareStatement(querySelectiveMetadata.toString())) {
                pstmt.setString(1, tableName);
                pstmt.setString(2, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Dimension_Transaction", rs.getString("Dimension_Transaction"));
                        selectiveMetadata.add(row);
                    }
                }
            }

            // Fetch IL settings
            try (PreparedStatement pstmt = connection.prepareStatement(queryIlSettings.toString())) {
                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", rs.getString("Connection_Id"));
                        row.put("Schema_Name", rs.getString("Schema_Name"));
                        row.put("Setting_Value", rs.getString("Setting_Value"));
                        ilSettings.add(row);
                    }
                }
            }

            // Process and merge the data
            List<Map<String, Object>> result = constantMetadata.stream()
                .map(constRow -> {
                    Map<String, Object> resultRow = new HashMap<>(constRow);
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", schemaName);
                    resultRow.put("Source_Table_Name", tableName);
                    resultRow.put("Active_Flag", true);

                    // Find matching selective metadata
                    selectiveMetadata.stream()
                        .filter(selRow -> selRow.get("Table_Name").equals(tableName))
                        .findFirst()
                        .ifPresent(selRow -> resultRow.put("Dimension_Transaction", selRow.get("Dimension_Transaction")));

                    // Find matching IL setting
                    ilSettings.stream()
                        .filter(ilRow -> ilRow.get("Schema_Name").equals(schemaName))
                        .findFirst()
                        .ifPresent(ilRow -> {
                            String settingValue = (String) ilRow.get("Setting_Value");
                            resultRow.put("IL_Table_Name", settingValue != null ? tableName + "_" + settingValue : tableName);
                        });

                    return resultRow;
                })
                .collect(Collectors.toList());

            // Create the final output
            return result.stream().map(row -> {
                Map<String, Object> finalRow = new HashMap<>();
                finalRow.put("Connection_Id", row.get("Connection_Id"));
                finalRow.put("TABLE_SCHEMA", row.get("TABLE_SCHEMA"));
                finalRow.put("IL_Table_Name", row.get("IL_Table_Name"));
                finalRow.put("IL_Column_Name", row.get("IL_Column_Name"));
                finalRow.put("IL_Data_Type", row.get("IL_Data_Type"));
                finalRow.put("Constraints", row.get("Constraints"));
                finalRow.put("Source_Table_Name", row.get("Source_Table_Name"));
                finalRow.put("Source_Column_Name", row.get("IL_Column_Name"));
                finalRow.put("Source_Data_Type", row.get("Source_Data_Type"));
                finalRow.put("PK_Constraint", row.get("PK_Constraint"));
                finalRow.put("PK_Column_Name", null);
                finalRow.put("FK_Constraint", null);
                finalRow.put("FK_Column_Name", null);
                finalRow.put("Constant_Insert_Column", row.get("Constant_Insert_Column"));
                finalRow.put("Constant_Insert_Value", row.get("Constant_Insert_Value"));
                finalRow.put("Dimension_Transaction", row.get("Dimension_Transaction"));
                finalRow.put("Dimension_Key", null);
                finalRow.put("Dimension_Name", null);
                finalRow.put("Dimension_Join_Condition", null);
                finalRow.put("Incremental_Column", null);
                finalRow.put("Isfileupload", 0);
                finalRow.put("File_Id", 0);
                finalRow.put("Column_Type", "Anvizent");
                finalRow.put("Active_Flag", row.get("Active_Flag"));
                finalRow.put("Added_Date", LocalDateTime.now());
                finalRow.put("Added_User", "ETL Admin");
                finalRow.put("Updated_Date", LocalDateTime.now());
                finalRow.put("Updated_User", "ETL Admin");
                return finalRow;
            }).collect(Collectors.toList());
        }
        
        private static List<Map<String, Object>> uniteResultOut2(List<Map<String, Object>> result, List<Map<String, Object>> out2) {
            // Determine the common set of keys
            Set<String> commonKeys = new HashSet<>(result.get(0).keySet());
            commonKeys.retainAll(out2.get(0).keySet());

            // Combine the two lists, keeping only the common keys
            return Stream.concat(
                    result.stream().map(row -> filterCommonKeys(row, commonKeys)),
                    out2.stream().map(row -> filterCommonKeys(row, commonKeys))
                )
                .collect(Collectors.toList());
        }
// Navin - New
// Used in both the flows 
        public static boolean updateActiveFlag(Connection connection, String connectionId, String querySchemaCondition, boolean activeFlagValue) {
            String query = "UPDATE ELT_IL_Source_Mapping_Info_Saved " +
                           "SET Active_Flag = ? " +
                           "WHERE Connection_Id = ? " + querySchemaCondition;
        
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setBoolean(1, activeFlagValue);
                preparedStatement.setString(2, connectionId);
        
                int rowsUpdated = preparedStatement.executeUpdate();
                System.out.println("Active_Flag Rows updated: " + rowsUpdated);
        
                return true;
            } catch (Exception e) {
                System.err.println("Exception occurred while updating Active_Flag: " + e.getMessage());
                return false;
            }
        }
        
        // Navin
        // Common to Both iterations at the end
        public boolean updateDimentionTransaction(Connection connection, String dimTransType, String dwTableName,
                String connectionId, String querySchemaCond) {
            if (connection == null || dimTransType == null || dwTableName == null || connectionId == null) {
                throw new IllegalArgumentException("Input parameters must not be null");
            }

            String query = "UPDATE ELT_IL_Source_Mapping_Info_Saved " +
                    "SET Dimension_Transaction=? " +
                    "WHERE IL_Table_Name=? AND Connection_Id=? " +
                    (querySchemaCond != null ? querySchemaCond : "");

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, dimTransType);
                preparedStatement.setString(2, dwTableName);
                preparedStatement.setString(3, connectionId);

                int rowsAffected = preparedStatement.executeUpdate();
                return rowsAffected > 0;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Navin
        // Applicable to both iterations - Second delte operation outside

        // Funciton remae needed

        public boolean executeDeleteQuery(Connection connection, String connectionId, String tableName, String querySchemaCond) {
            if (connection == null || connectionId == null || tableName == null) {
                throw new IllegalArgumentException("Input parameters must not be null");
            }
            String query = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                        "WHERE Active_Flag=1 " +
                        "AND (Constant_Insert_Column IS NULL OR " +
                        "     (Constant_Insert_Column <> 'Y' OR IL_Column_Name = 'DataSource_Id') " +
                        "     AND Constraints <> 'SK,PK') " +
                        "AND Connection_Id = ? " +
                        "AND Source_Table_Name = ? " +
                        (querySchemaCond != null ? querySchemaCond : "");

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                preparedStatement.setString(2, tableName);

                int rowsAffected = preparedStatement.executeUpdate();
                return rowsAffected > 0;
            } catch (SQLException e) {
                e.printStackTrace(); // Log the exception or handle it appropriately
                return false; // Return false in case of failure
            }
        }


        private static void bulk(Connection connection, List<Map<String, Object>> row1) throws SQLException, IOException {
            String clientId = context.get("CLIENT_ID");
            String packageId = context.get("PACKAGE_ID");
            String jobName = context.get("JOB_NAME");
            String connectionId = context.get("CONNECTION_ID");
            String tableName = context.get("Table_Name");
            String dwTableName = context.get("DW_Table_Name");
            String bulkPath = context.get("BULK_PATH");
            String querySchemaCondition = context.get("query_schema_cond");
            String tableType = context.get("TableType");

            // Generate bulk file path
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
            String bulkFilePath = bulkPath + clientId + "_" + packageId + "_" + jobName + "_BULK_" + timestamp + ".csv";

            // Save row1 to CSV file
            saveToCsv(row1, bulkFilePath);

            // Perform SQL DELETE operations
            String deleteQuery1 = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved WHERE Column_Type='Anvizent' AND IL_Table_Name=?";
            String deleteQuery2 = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                                  "WHERE Active_Flag=1 " +
                                  "AND (Constant_Insert_Column IS NULL OR " +
                                  "(Constant_Insert_Column <> 'Y' OR IL_Column_Name='DataSource_Id') " +
                                  "AND Constraints <> 'SK,PK') " +
                                  "AND Connection_Id=? " +
                                  "AND Source_Table_Name=? " +
                                  querySchemaCondition;

            try (PreparedStatement pstmt1 = connection.prepareStatement(deleteQuery1);
                 PreparedStatement pstmt2 = connection.prepareStatement(deleteQuery2)) {
                
                pstmt1.setString(1, dwTableName);
                pstmt1.executeUpdate();

                pstmt2.setString(1, connectionId);
                pstmt2.setString(2, tableName);
                pstmt2.executeUpdate();
            }

            // Perform bulk insert
            String bulkInsertQuery = "BULK INSERT ELT_IL_Source_Mapping_Info_Saved " +
                                     "FROM ? " +
                                     "WITH (FORMAT='CSV', FIRSTROW=2)";
            
            try (PreparedStatement pstmt = connection.prepareStatement(bulkInsertQuery)) {
                pstmt.setString(1, bulkFilePath);
                pstmt.executeUpdate();
            }

            // Perform SQL UPDATE operation
            String updateQuery = "UPDATE ELT_IL_Source_Mapping_Info_Saved " +
                                 "SET Dimension_Transaction=? " +
                                 "WHERE IL_Table_Name=? " +
                                 "AND Connection_Id=? " +
                                 querySchemaCondition;
            
            try (PreparedStatement pstmt = connection.prepareStatement(updateQuery)) {
                pstmt.setString(1, tableType);
                pstmt.setString(2, dwTableName);
                pstmt.setString(3, connectionId);
                pstmt.executeUpdate();
            }

            // Handle file paths
            String bulkFilePattern = clientId + "_" + packageId + "_" + jobName + "_BULK_" + timestamp + ".csv";
            String currentFilePath = (String) globalMap.get("tFileList_1_CURRENT_FILEPATH");
            String errorFilePath = context.get("FILE_PATH") + clientId + "_" + packageId + "_" + jobName + "_" +
                                   timestamp + "_error_file.csv";

            System.out.println("Bulk process complete. CSV saved at: " + bulkFilePath);
            System.out.println("Error file would be saved at: " + errorFilePath);
            System.out.println("Processed file: " + currentFilePath);
        }

       
        
        private static Map<String, Object> filterCommonKeys(Map<String, Object> row, Set<String> commonKeys) {
            return row.entrySet().stream()
                .filter(entry -> commonKeys.contains(entry.getKey()))
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> v1,
                    LinkedHashMap::new
                ));
        }

        private static Map<String, Object> calculateConstraints(Map<String, Object> row) {
            String pkConstraint = (row.get("PK_Constraint") != null && "yes".equals(row.get("PK_Constraint"))) ? "PK" : "";
            String pkColumnName = (row.get("PK_Column_Name") != null && !"NULL".equals(row.get("PK_Column_Name"))
                                   && !"".equals(row.get("PK_Column_Name"))) ? (String) row.get("PK_Column_Name") : "";
            
            String ilTableName = (row.get("Setting_Value") != null) 
                                 ? row.get("Table_Name") + "_" + row.get("Setting_Value")
                                 : (String) row.get("Table_Name");
            
            String fkConstraint = (row.get("FK_Constraint") != null && !"".equals(row.get("FK_Constraint"))) ? "FK" : "";
            String fkColumnName = (row.get("FK_Column_Name") != null && !"".equals(row.get("FK_Column_Name"))) 
                                  ? (String) row.get("FK_Column_Name") : "";

            row.put("PK_Constraint", pkConstraint);
            row.put("PK_Column_Name", pkColumnName);
            row.put("IL_Table_Name", ilTableName);
            row.put("FK_Constraint", fkConstraint);
            row.put("FK_Column_Name", fkColumnName);

            return row;
        }

        private static List<Map<String, Object>> createFinalResult(List<Map<String, Object>> mergedData) {
            return mergedData.stream().map(row -> {
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", row.get("Connection_Id"));
                resultRow.put("TABLE_SCHEMA", row.get("TABLE_SCHEMA"));
                resultRow.put("IL_Table_Name", row.get("IL_Table_Name"));
                resultRow.put("IL_Column_Name", row.get("Column_Name"));
                resultRow.put("IL_Data_Type", row.get("IL_Data_Type"));
                resultRow.put("Constraints", row.get("PK_Constraint"));
                resultRow.put("Source_Table_Name", row.get("Table_Name"));
                resultRow.put("Source_Column_Name", row.get("Column_Name"));
                resultRow.put("Source_Data_Type", row.get("Data_Type"));
                resultRow.put("PK_Constraint", row.get("PK_Constraint"));
                resultRow.put("PK_Column_Name", row.get("PK_Column_Name"));
                resultRow.put("FK_Constraint", row.get("FK_Constraint"));
                resultRow.put("FK_Column_Name", row.get("FK_Column_Name"));
                resultRow.put("Constant_Insert_Column", null);
                resultRow.put("Constant_Insert_Value", null);
                resultRow.put("Dimension_Transaction", row.get("Dimension_Transaction"));
                resultRow.put("Dimension_Key", null);
                resultRow.put("Dimension_Name", null);
                resultRow.put("Dimension_Join_Condition", null);
                resultRow.put("Incremental_Column", "T".equals(row.get("Dimension_Transaction")) ? "Y" : "N");
                resultRow.put("Isfileupload", 0);
                resultRow.put("File_Id", 0);
                resultRow.put("Column_Type", "Source");
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", LocalDateTime.now());
                resultRow.put("Added_User", "ETL Admin");
                resultRow.put("Updated_Date", LocalDateTime.now());
                resultRow.put("Updated_User", "ETL Admin");
                return resultRow;
            }).collect(Collectors.toList());
        }
        
        
        private static int calculateVar1(Map<String, Object> row) {
            Integer characterMaxLength = (Integer) row.get("Character_Max_Length");
            return (characterMaxLength != null) ? 2 * characterMaxLength : 0;
        }

        private static String calculateIlDataType(Map<String, Object> row) {
            String dataType = (String) row.get("Data_Type");
            String defaultFlag = (String) row.get("Default_Flag");
            Integer numericPrecision = (Integer) row.get("Numeric_Precision");
            Integer numericScale = (Integer) row.get("Numeric_Scale");
            Integer characterMaxLength = (Integer) row.get("Character_Max_Length");

            if (defaultFlag == null) {
                return dataType;
            }
            if ("y".equalsIgnoreCase(defaultFlag)) {
                return (String) row.get("IL_Data_Type");
            }
            if ("decimal".equals(dataType) || "numeric".equals(dataType) || "number".equals(dataType) || "double".equals(dataType)) {
                return String.format("decimal(%d,%d)", numericPrecision, numericScale);
            }
            if (dataType.contains("int")) {
                return String.format("%s(%d)", dataType, numericPrecision);
            }
            if ("varchar".equals(dataType) || "char".equals(dataType) || "mediumtext".equals(dataType)) {
                return String.format("%s(%d)", dataType, characterMaxLength);
            }
            if ("varchar2".equals(dataType) || "nvarchar".equals(dataType)) {
                return String.format("varchar(%d)", characterMaxLength);
            }
            if ("nchar".equals(dataType)) {
                return String.format("char(%d)", characterMaxLength);
            }
            if (dataType.contains("bit")) {
                return "bit(1)";
            }
            if ("float".equals(dataType)) {
                return String.format("float(%d,%d)", numericPrecision, numericScale);
            }
            return dataType;
        }
    
}
