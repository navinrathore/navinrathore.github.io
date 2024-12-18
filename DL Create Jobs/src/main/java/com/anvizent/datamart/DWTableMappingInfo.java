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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.sql.Timestamp;

import org.json.JSONObject;

/*
 * Table Mapping Info, DW Table Mapping generation mapping 
 */
public class DWTableMappingInfo {

	
    private final Map<String, String> context = new HashMap<>();
    private final Map<String, Object> globalMap = new HashMap<>();
    private Connection dbConnection;
    private final LocalDateTime startTime;
    private String userName = "ETL Admin"; // default user
    private String schemaName;
    private String customType;
    private String companyId;
    private String connectionId;
    private String dataSourceName;
    private int customFlag;
    private String clientId;
    private String selectTables;
    private String querySchemaCondition = ""; // based on TABLE_SCHEMA
    private String querySchemaCondition1 = ""; // based on Schema_Name

    private String dbDetails;

    // default ctor
    public DWTableMappingInfo() {
        startTime = LocalDateTime.now();
        //this.dbDetails = dbDetails;
    }

    public DWTableMappingInfo(String clientId,
                              String schemaName,
                              String connectionId,
                              String dataSourceName,
                              String customFlag,
                              String customType,
                              String companyId,
                              String selectTables,
                              String dbDetails) {
        this.startTime = LocalDateTime.now();
        this.dbDetails = dbDetails;
        this.clientId = clientId;
        this.schemaName = schemaName;
        this.connectionId = connectionId;
        this.dataSourceName = dataSourceName;
        this.customFlag = Integer.parseInt(customFlag);
        this.customType = customType;
        this.companyId = companyId;
        this.selectTables = selectTables;
    }

    public static void main(String[] args) {
        DWTableMappingInfo dWTableMappingInfo = new DWTableMappingInfo(); // TODO
//        DWTableMappingInfo(clientId, schemaName, tableType, connectionId, dataSourceName,
//                 customFlag, customType, companyId, tableString, dbDetails);
        dWTableMappingInfo.loadContext();
        dWTableMappingInfo.init();
        dWTableMappingInfo.mainProcess();
    }

    private void init() {
        // TODO uncomment
//        JSONObject jsonDbDetails = new JSONObject(dbDetails);
//        userName = jsonDbDetails.getString("appdb_username");

        // The Below function shall create two Schema Conditions which are used throughout the application
        Map<String, String> conditions  = createQueryConditions(null, schemaName);
        this.querySchemaCondition = conditions.get("query_schema_cond");
        this.querySchemaCondition1 = conditions.get("query_schema_cond1");
    }
   public void initiateJob(Map<String, String> newContext){
	   if (newContext == null) {
           throw new IllegalArgumentException("Context cannot be null");
       }
       synchronized (context) {
           context.clear();
           context.putAll(newContext);
           mainProcess();
       }
    }
    

    private void loadContext() {
    	 synchronized (context) {
	    	context.put("APP_HOST", "172.25.25.124:4475");
			context.put("APP_DBNAME", "Mysql8_2_1009427_appdb");
			context.put("APP_UN", "Bk16Tt55");
			context.put("APP_PW", "Tt5526");
            // set 1 custom flag is ZERO
//             context.put("Schema_Name", "dbo"); // FGB_1, AbcAnalysis
//             schemaName = context.get("Schema_Name");
//
//             //context.put("Table_Name","SorDetail"); // SorDetail, AbcAnalysis, AbcElement
//             context.put("Table_Name","AbcAnalysis"); // SorDetail, AbcAnalysis, AbcElement
//
//             context.put("CONNECTION_ID", "4"); // 2,3,4
//             connectionId = context.get("CONNECTION_ID");

             // set 2
			context.put("Schema_Name", "FGB_1"); // FGB_1,
             schemaName = context.get("Schema_Name");
			context.put("Table_Name","Finished_Goods_BOM"); // Finished_Goods_BOM
             //tableName = context.get("Table_Name");
			context.put("CONNECTION_ID", "41"); // 2,3,4
             connectionId = context.get("CONNECTION_ID");
             //String customType = "Common";
             context.put("Custom_Type","Common");
             customType = context.get("Custom_Type");
             context.put("DATASOURCENAME","syspro");
             dataSourceName = context.get("DATASOURCENAME");
			context.put("CLIENT_ID", "1009427");
            clientId = context.get("CLIENT_ID");
			context.put("TableType", "D");
            //tableType = context.get("TableType");
            //context.put("Custom_Flag", "0"); // Custom flag should be set to 0 or 1
             context.put("Custom_Flag", "1");
             customFlag = Integer.parseInt(context.get("Custom_Flag"));
             context.put("Selective_Tables","'Finished_Goods_BOM'");
             selectTables = context.get("Selective_Tables");
             context.put("Company_Id","syspro");
             companyId = context.get("Company_Id");
    	 }
    }

    private Connection getDbConnection() {
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
    // Navin - redundant another functio is written
//    private void createQueryConditions() {
//        String schemaName = context.get("Schema_Name");
//        String querySchemaCondition = "";
//        String querySchemaCondition1 = "";
//
//        if (schemaName == null || schemaName.equalsIgnoreCase("NULL") || schemaName.isEmpty()) {
//            querySchemaCondition = "";
//            querySchemaCondition1 = "";
//        } else {
//            querySchemaCondition = " AND TABLE_SCHEMA = '" + schemaName + "'";
//            querySchemaCondition1 = " AND Schema_Name = '" + schemaName + "'";
//        }
//
//        context.put("query_schema_cond", querySchemaCondition);
//        context.put("query_schema_cond1", querySchemaCondition1);
//    }


    /**
     * Generates query conditions based on the provided schema name and optional table alias.
     *
     * If a table alias is provided, it prefixes the schema references in the generated conditions.
     * Otherwise, the conditions are generated without any prefix.
     *
     * @param tableAlias Optional table alias to prefix schema references.
     * @param schemaName Name of the schema to use in query conditions.
     * @return Map containing the generated query conditions.
     */
    private Map<String, String> createQueryConditions(String tableAlias, String schemaName) {
        Map<String, String> queryConditions = new HashMap<>();
        String querySchemaCondition = "";
        String querySchemaCondition1 = "";

        // Determine prefix if table alias is provided
        String prefix = (tableAlias != null && !tableAlias.isEmpty()) ? tableAlias + "." : "";

        if (schemaName != null && !schemaName.equalsIgnoreCase("NULL") && !schemaName.isEmpty()) {
            querySchemaCondition = " AND " + prefix + "TABLE_SCHEMA = '" + schemaName + "'";
            querySchemaCondition1 = " AND " + prefix + "Schema_Name = '" + schemaName + "'";
        }

        queryConditions.put("query_schema_cond", querySchemaCondition);
        queryConditions.put("query_schema_cond1", querySchemaCondition1);
        return queryConditions;
    }

    /**
     * Executes the delete query on the database.
     *
     * @param connection       The database connection.
     * @param connectionId     The connection ID to filter the query.
     * @param sourceTableName  The source table name to filter the query.
     * @param querySchemaCond  The additional schema condition for the query.
     * @return true if the query executes successfully, false otherwise.
     */
    public boolean deleteExistingRecordsForSourceTableName(Connection connection, String connectionId, String sourceTableName, String querySchemaCond) {
        String sql = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Active_Flag = 1 " +
                    "AND (Constant_Insert_Column IS NULL " +
                    "OR (Constant_Insert_Column <> 'Y' OR IL_Column_Name = 'DataSource_Id') " +
                    "AND Constraints <> 'SK,PK') " +
                    "AND Connection_Id = ? " +
                    "AND Source_Table_Name = ? " +
                    querySchemaCond;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, connectionId);
            preparedStatement.setString(2, sourceTableName);

            int rowsAffected = preparedStatement.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.err.println("SQL Exception while executing query: " + e.getMessage());
            return false;
        }
    }

    /**
     * Executes the delete query for rows with a specific Column_Type (Anvizent) and IL_Table_Name.
     *
     * @param connection  The database connection.
     * @param tableName   The base table name to filter the query.
     * @param suffixValue The suffix to append to the table name in the query.
     * @return true if the query executes successfully, false otherwise.
     */
    public boolean deleteRecordsForColumnTypeAnvizent(Connection connection, String tableName, String suffixValue) {
        String sql = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Column_Type = 'Anvizent' " +
                    "AND IL_Table_Name = ?";
        // TODO check if suffix value is not present. Other places this check is done
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (suffixValue != null && !suffixValue.isEmpty())
                preparedStatement.setString(1, tableName + "_" + suffixValue);
            else 
                preparedStatement.setString(1, tableName);


            int rowsAffected = preparedStatement.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.err.println("SQL Exception while executing query: " + e.getMessage());
            return false;
        }
    }
// TODO for SAved FAS
    public boolean deleteRecordsForColumnTypeAnvizent(Connection connection, String tableName) {
        return deleteRecordsForColumnTypeAnvizent(connection, tableName, "");
    }

    /**
     * Deletes records from the ELT_IL_Source_Mapping_Info_Saved table.
     * The delete operation done on records of `ELT_IL_Source_Mapping_Info_Saved` inner-joined with
     * the records got from Anti-Joined from ELT_IL_Source_Mapping_Info_History and ELT_Selective_Source_Metadata
     *
     * @param connection The active database connection
     */
    private int deleteRecordsFromMappingInfoSaved(Connection connection) {
        int rowsAffected = 0;
        try (Statement stmt = connection.createStatement()) {
            String connectionId = context.get("CONNECTION_ID");

            String deleteQuery = "DELETE FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE IL_Table_Name IN (" +
                    "    SELECT DISTINCT IL_Table_Name" +
                    "    FROM ELT_IL_Source_Mapping_Info_History" +
                    "    WHERE Source_Table_Name NOT IN (" +
                    "        SELECT DISTINCT Table_Name" +
                    "        FROM ELT_Selective_Source_Metadata" +
                    "        WHERE Isfileupload != '1'" +
                    "        AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition1 +
                    "    )" +
                    "    AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition +
                    ")" +
                    "AND Connection_Id = '" + connectionId + "'" +
                    querySchemaCondition;

            rowsAffected = stmt.executeUpdate(deleteQuery);
            System.out.println("Deleted " + rowsAffected + " rows from ELT_IL_Source_Mapping_Info_Saved.");
        } catch (SQLException e) {
            System.out.println("Error executing delete query: " + e.getMessage());
        }
        return rowsAffected;
    }

    private void mainProcess() {
        dbConnection = getDbConnection(); // initialize the member variable // TODO should be init()
        // dbConnection= DBHelper.getConnection(DataSourceType.MYSQL, dbDetails); // TODO this one to be used
        if (dbConnection == null) {
            System.out.println("Failed to establish database connection. Exiting.");
            return;
        }

        try {
            // Query Schema Conditions have been initialized in init() function

            deleteRecordsFromMappingInfoSaved(dbConnection);

            if (customFlag == 0) { //DB,Metadata file
                System.out.println("Running process when Custom_Flag = 0");
                mainProcessForFlagZero();
            } else if (customFlag == 1) {  //SHARED_FOLDER, FLAT_FILE, WEB_SERVICE , ONEDRIVE, SAGE_INTACCT, SALESFORCESOAP
                System.out.println("Running process when Custom_Flag = 1");
                mainProcessForFlagOne();
            } else {
                System.out.println("Invalid Custom_Flag value. Exiting.");
            }
        } finally {
            try {
                dbConnection.close();
                System.out.println("Main database connection closed.");
            } catch (SQLException e) {
                System.out.println("Error closing database connection: " + e.getMessage());
            }
        }
    }
        // the main processing logic when custom flag is set to 0.
        public void mainProcessForFlagZero() {
            try {
                int rowsDeleted = deleteRecordsFromMappingInfoSaved(dbConnection);

                List<Map<String, Object>> metadata = processSelectiveTables(dbConnection); //Form Table Name with Suffix
                System.out.println("Selective tables processing completed successfully.");

                // Step 2 - iteration
                System.out.println("Size of metadata (custom type 0): " + metadata.size());
                for (Map<String, Object> record : metadata) {

                    String tableName = (String) record.get("Table_Name");
                    String tableType = (String) record.get("Dimension_Transaction");
                    String suffixValue = (String) record.get("Setting_Value");
                    String dwTableName = (String) record.get("DW_Table_Name");
                    System.out.println("Table_Name: " + tableName + ", Dimension_Transaction: " + tableType + ", Setting_Value: " + suffixValue + ", dwTableName: " + dwTableName );

                    boolean activeFlagValue = true; // Type in DB is bit(1)
                    boolean updated = updateActiveFlag(this.dbConnection , connectionId, querySchemaCondition, activeFlagValue);

                    Map<String, Map<String, Object>> resultMetadata = processSelectiveSourceMetadata(dbConnection, tableName, connectionId, querySchemaCondition);

                    Map<String, Map<String, Object>> y = getSourceMetadata(dbConnection, tableName, connectionId, querySchemaCondition);
                    Map<String, Map<String, Object>> z = getDatatypeConversions(dbConnection);
                    List<Map<String, Object>> x = JoinWithMetadataAndDataTypeConversions(resultMetadata, y, z);

                    Map<String, String> activeAliasValuesTables = getActiveAliasValuesTables(dbConnection, connectionId, querySchemaCondition1);
                    Map<String, Map<String, Object>> fkConstraintLookup = getAggregatedFKConstraint(dbConnection, tableName, connectionId, querySchemaCondition, querySchemaCondition1);
                    List<Map<String, Object>> tMap2Output = processJoinedData(x, activeAliasValuesTables, fkConstraintLookup);
                    // End of first part of UNITE

                    // Start of second part of UNITE
                    List<Map<String, Object>> tMap9Output = processConstantFieldsWithMetadataAndAliases (dbConnection, dataSourceName,
                            tableName, companyId, connectionId, querySchemaCondition);

                    // Unite the records from first and second parts
                    List<Map<String, Object>> unitedResult = uniteResultOut2(tMap2Output,  tMap9Output);

                    // Delete the records from the table
                    boolean rowsDeleted1 = deleteRecordsForColumnTypeAnvizent(dbConnection, tableName, suffixValue);
                    boolean rowsDeleted2 = deleteRecordsFromILSourceMappingInfoSaved(dbConnection, connectionId, tableName, querySchemaCondition);

                    // Save into DB
                    String dbTable = "ELT_IL_Source_Mapping_Info_Saved";
                    int rowsAdded = saveDataSourceMappingInfoIntoDB(dbConnection, dbTable, unitedResult);

                    // Set Dimension_Transaction for given IL_Table_name
                    updateDimensionTransaction(dbConnection, tableType, dwTableName, connectionId, querySchemaCondition);
                }
                System.out.println("Processing for custom_flag == 0 completed successfully.");
            } catch (Exception e) {
                System.out.println("An error occurred during processing for custom_flag == 0: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // the main processing logic when custom flag is set to 1.
        private void mainProcessForFlagOne() {
            try {
                int rowsDeleted = deleteRecordsFromMappingInfoSaved(dbConnection);

                List<Map<String, Object>> metadata = getSelectiveSourceMetadataWithSettingsSharedFolderAndWS(selectTables, connectionId, dbConnection);
                System.out.println("Selective tables processing completed successfully.");

                // Step 2 - iteration
                System.out.println("Size of metadata (custom type 1): " + metadata.size());
                for (Map<String, Object> record : metadata) {

                    String tableName = (String) record.get("Table_Name");
                    String tableType = (String) record.get("Dimension_Transaction");
                    String suffixValue = (String) record.get("Setting_Value");
                    String dwTableName = (String) record.get("DW_Table_Name");
                    System.out.println("Table_Name: " + tableName + ", Dimension_Transaction: " + tableType + ", Setting_Value: " + suffixValue + ", dwTableName: " + dwTableName );

                    boolean activeFlagValue = true; // Type in DB is bit(1)
                    boolean updated = updateActiveFlag(this.dbConnection, connectionId, querySchemaCondition, activeFlagValue);

                    String customTypeExcluded = "Common"; // Excluding this custom type
                    Map<String, Map<String, Object>> tMap1Output = processSelectiveSourceMetadataForFlagOne(tableName, connectionId, querySchemaCondition, querySchemaCondition1, customTypeExcluded, tableName);

                    Map<String, Map<String, String>> customMappingData = getCustomSourceMappingInfo(dbConnection, tableName, connectionId, querySchemaCondition1);
                    // ELT_Datatype_conversion
                    Map<String, Map<String, Object>> conversionDataTypesMap = getDatatypeConversions(dbConnection);

                    // tmap2
                    List<Map<String, Object>> tMap2Output = JoinWithMetadataAndDataTypeConversionsForFlagOne(
                            tMap1Output,
                            customMappingData,
                            conversionDataTypesMap);

                    // tmap3
                    List<Map<String, Object>> tMap3Output = processJoinedDataForFlagOne(tMap2Output, dbConnection, connectionId, querySchemaCondition1);
                    // End of first part of UNITE

                    // Start of second part of UNITE
                    List<Map<String, Object>> tMap7Output = processConstantFieldsWithMetadataSharedFolderWebServiceAndAliases(
                            dbConnection, dataSourceName, tableName,
                            companyId, connectionId, querySchemaCondition);

                    // Unite the records from first and second parts
                    List<Map<String, Object>> unitedResult = uniteResultOut2(tMap3Output, tMap7Output);

                    // Delete the records from the table
                    boolean rowsDeleted1 = deleteRecordsForColumnTypeAnvizent(dbConnection, tableName, suffixValue);
                    boolean rowsDeleted2 = deleteRecordsFromILSourceMappingInfoSaved(dbConnection, connectionId, tableName, querySchemaCondition);

                    // Save into DB
                    String dbTable = "ELT_IL_Source_Mapping_Info_Saved";
                    int rowsAdded = saveDataSourceMappingInfoIntoDB(dbConnection, dbTable, unitedResult);

                    // Set Dimension_Transaction for given IL_Table_name
                     updateDimensionTransaction(dbConnection, tableType, dwTableName, connectionId, querySchemaCondition);
                }
                System.out.println("Processing for custom_flag == 1 completed successfully.");
            } catch (Exception e) {
                System.out.println("An error occurred during processing for custom_flag == 1: " + e.getMessage());
                e.printStackTrace();
            }
        }

    /**
     * Processes selective source metadata for entries with Isfileupload flag set to '1'.
     *
     * This method filters and organizes metadata information from the source based on provided
     * table names, connection ID, and schema query conditions. It returns a nested map structure
     * where keys represent specific metadata categories and corresponding details.
     *
     * @param tableNameList      Comma-separated list of table names to process.
     * @param connectionId       Identifier for the database connection.
     * @param customType         Custom type
     * @param tableName          Specific table name being processed.
     * @return A nested map of metadata details organized by categories and properties.
     */
    private Map<String, Map<String, Object>> processSelectiveSourceMetadataForFlagOne(String tableNameList, String connectionId, String querySchemaCond, String querySchemaCond1, String customType, String tableName) throws SQLException {
        // FAS - main
        Map<String, Map<String, String>> sourceMetadata = fetchSelectiveSourceMetadata(
                dbConnection,
                tableNameList,
                connectionId,
                querySchemaCond1,
                customType);
        // FAS - Lookup 1 - row6
        Map<String, Map<String, String>> lookup1 = fetchCustomSourceMetadata(
                dbConnection,
                tableName,
                connectionId,
                querySchemaCond1);
        // FAS - Lookup 2 - row11 (+row9)
        Map<String, Map<String, String>> lookup2 = fetchILSourceMappingInfoSaved(
                dbConnection,
                tableName,
                connectionId,
                querySchemaCond,
                customType);

        List<Map<String, Object>> innerJoinResultList = new ArrayList<>(); // Inner Join Result List; Store into a file
        Map<String, Map<String, Object>> antiInnerJoinResultMap = new HashMap<>(); // Anti-Inner Join Result Map; output for further processing

        for (Map.Entry<String, Map<String, String>> entry : sourceMetadata.entrySet()) {
            String sourceKey = entry.getKey();
            Map<String, String> sourceMap = (Map<String, String>) entry.getValue();

            // inner join with the ilSourceMappingData map (inner join logic)
            if (lookup2.containsKey(sourceKey)) { // row 11 inner join
                Map<String, Object> valueMap = new HashMap<>();

                Map<String, String> ilmappingValue = lookup2.get(sourceKey);

                boolean ilColumnNamechange;
                Map<String, String> customSourceMappingInfo = lookup1.getOrDefault(sourceKey, new HashMap<>());
                if (ilmappingValue.get("IL_Column_Name") == null || customSourceMappingInfo.get("Source_Column_Name") == null) {
                    ilColumnNamechange = false;
                } else if (!ilmappingValue.get("IL_Column_Name").equals(customSourceMappingInfo.get("MappingColumnTo"))) {
                    ilColumnNamechange = true;
                } else {
                    ilColumnNamechange = false;
                }

                String updatedDate;
                if (ilColumnNamechange) {
                    updatedDate = startTime.toString();
                } else {
                    updatedDate = ilmappingValue.get("Updated_Date");
                }
                String icrementalColumn = null;
                if (ilmappingValue.get("Dimension_Transaction") == null) {
                    icrementalColumn = "N";
                } else if (sourceMap.get("Dimension_Transaction").equals("T") &&
                        ilmappingValue.get("Dimension_Transaction").equals("D")) {
                    icrementalColumn = "Y";
                } else {
                    icrementalColumn = ilmappingValue.get("Dimension_Transaction");
                }

                // Output values
                valueMap.put("Connection_Id", ilmappingValue.get("Connection_Id"));
                valueMap.put("Table_Schema", ilmappingValue.get("Table_Schema"));
                valueMap.put("Source_Table_Name", ilmappingValue.get("Source_Table_Name"));
                valueMap.put("IL_Column_Name", sourceMap.get("Column_Name"));

                valueMap.put("Dimension_Transaction", sourceMap.get("Dimension_Transaction"));
                valueMap.put("Incremental_Column", icrementalColumn);
                valueMap.put("Active_Flag", false);
                valueMap.put("Updated_Date", updatedDate);

                innerJoinResultList.add(valueMap);

            } else { // anti-join with the ilSourceMappingData map (!lookup2.containsKey(sourceKey))
                Map<String, Object> valueMap = new HashMap<>();
                valueMap.put("Connection_Id", sourceMap.get("Connection_Id"));
                valueMap.put("Schema_Name", sourceMap.get("Schema_Name"));
                valueMap.put("Table_Name", sourceMap.get("Table_Name"));
                valueMap.put("Column_Name", sourceMap.get("Column_Name"));
                valueMap.put("Dimension_Transaction", sourceMap.get("Dimension_Transaction"));

                antiInnerJoinResultMap.put(sourceKey, valueMap);
            }
        }
        // Save innerJoinResultMap into the table
        saveInnerJoinedData(innerJoinResultList);

        return antiInnerJoinResultMap;
    }
    // Save Inner join data into the table
    private void saveInnerJoinedData(List<Map<String, Object>> rowsData) throws SQLException {
        String dbTable = "ELT_IL_Source_Mapping_Info_Saved";
        if (rowsData != null && !rowsData.isEmpty()) {
            int rowsAdded = saveDataSourceMappingInfoIntoDB(dbConnection, dbTable, rowsData);
            System.out.println("InnerJoinedData - Number of rows inserted into DB: " + rowsAdded);
        }
        else {
            System.out.println("rowsData list is null or empty");
        }
    }

    private List<Map<String, Object>> fetchEltMetadata(Connection connection) throws SQLException {
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

        private List<Map<String, Object>> mergeSettingsAndCalculateIlTableName(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
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

        private Map<String, Object> calculateIlTableName(Map<String, Object> row, String customType) {
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

        private List<Map<String, Object>> createConstantColumnsAndIlTableName(Connection connection) throws SQLException {
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

        private Map<String, Object> processConstantRow(
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
        
        private Map<String, Object> createFinalConstantRow(Map<String, Object> row) {
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


        private List<Map<String, Object>> fetchConstantMetadata(
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

        private List<Map<String, Object>> fetchSelectiveMetadata(
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

        private List<Map<String, Object>> fetchIlSettings(
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


        private List<Map<String, Object>> uniteDataframes(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
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

        private void saveToCsv(List<Map<String, Object>> data, String filePath) throws IOException {
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


        private List<Map<String, Object>> processSelectiveTables(Connection connection) throws SQLException {
            //String selectTables = context.get("Selective_Tables");
            //String connectionId = context.get("CONNECTION_ID");
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

            List<Map<String, Object>> dfOut = getSelectiveSourceMetadataJoinedWithSettings(selectTables, connectionId, connection);

            //List<Map<String, Object>> dfOut = mergeData(dfEltSelective, dfEltIlSettings);

            // Done inside the function

            // for (Map<String, Object> row : dfOut) {
            //     String tableName = (String) row.get("Table_Name");
            //     String settingValue = (String) row.get("Setting_Value");
            //     String dwTableName = settingValue == null ? tableName : tableName + "_" + settingValue;
            //     globalMap.put("DW_Table_Name", dwTableName);
            // }

            System.out.println(dfOut);
            return dfOut;
        }
        
        private List<Map<String, Object>> fetchDataFromDb(StringBuilder queryEltIlSettings, String connectionId, Connection con) throws SQLException {
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

        // FAS - USED before iterative flow
    // TODO return data is used where? how? Table_Name, TableType, SuffixValue, DW_Table_Name?
        public List<Map<String, Object>> getSelectiveSourceMetadataWithSettingsSharedFolderAndWS(
                String selectiveTables, String connectionId, Connection con) throws SQLException {

            // The Below function shall create two Schema Conditions which are used only in this function
            Map<String, String> conditions  = createQueryConditions("main", schemaName);
            String querySchemaCond1 = conditions.get("query_schema_cond1");
            System.out.println("query_schema_cond1 with alias : " + querySchemaCond1);

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
                            row.put(metaData.getColumnLabel(i), rs.getObject(i));
                        }

                        // private Map<String, Object> getDataForIteration(ResultSet rs) throws SQLException{
                        //     Map<String, Object> row = new HashMap<>();

                        String schemaName = rs.getString("Schema_Name");

                        String settingValue;
                                        //String customType = ""; // or null // TO Context variable ; intialize in context
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
                        String dimensionTransaction = (String) row.get("Dimension_Transaction");
                        String dwTableName = (settingValue != null) ? tableName + "_" + settingValue : tableName;

                        // Data used for Iterative job
                        row.put("Table_Name", tableName);
                        row.put("Dimension_Transaction", dimensionTransaction);
                        row.put("Setting_Value", settingValue);
                        row.put("DW_Table_Name", dwTableName);
                        results.add(row);
                    }
                }
            }
            return results;
        }

    // Navin
    // FAS - tmap2 - top part input
    private Map<String, Map<String, String>> getCustomSourceMappingInfo(Connection connection, String tableName, String connectionId, String querySchemaCond1) throws SQLException {
        String query = "SELECT " +
                "`Connection_Id`, " +
                "`Schema_Name`, " +
                "`Table_Name`, " +
                "`Column_Name`, " +
                "CASE WHEN Constraints='PK' AND Column_Data_Type LIKE '%bit%' THEN 'bit(1)' " +
                "ELSE Column_Data_Type END AS Column_Data_Type, " +
                "`Constraints`, " +
                "LOWER(SUBSTRING_INDEX(Column_Data_Type, '(', 1)) AS Source_Data_Type, " +
                "`Source_Column_Name` " +
                "FROM `ELT_Custom_Source_Mapping_Info` " +
                "JOIN `ELT_Custom_Source_Metadata_Info` " +
                "ON `ELT_Custom_Source_Mapping_Info`.`Custom_Id` = `ELT_Custom_Source_Metadata_Info`.`Id` " +
                "WHERE `Table_Name` = ? AND `Connection_Id` = ? " + querySchemaCond1;
        Map<String, Map<String, String>> resultMap = new HashMap<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, tableName);       // Setting Table Name
            preparedStatement.setString(2, connectionId);    // Setting Connection ID

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String key = resultSet.getString("Connection_Id") + "_" +
                            resultSet.getString("Schema_Name") + "_" +
                            resultSet.getString("Table_Name") + "_" +
                            resultSet.getString("Column_Name");
                    Map<String, String> rowMap = new HashMap<>();
                    rowMap.put("Connection_Id", resultSet.getString("Connection_Id"));
                    rowMap.put("TABLE_SCHEMA", resultSet.getString("Schema_Name"));
                    rowMap.put("Table_Name", resultSet.getString("Table_Name"));
                    rowMap.put("Column_Name", resultSet.getString("Column_Name"));
                    rowMap.put("Data_Type", resultSet.getString("Column_Data_Type"));
                    rowMap.put("Constraints", resultSet.getString("Constraints"));
                    rowMap.put("Source_Data_Type", resultSet.getString("Source_Data_Type"));
                    rowMap.put("Source_Column_Name", resultSet.getString("Source_Column_Name"));

                    resultMap.put(key, rowMap);
                }

            } catch (Exception e) {
                System.err.println("Exception occurred while fetching custom source metadata: " + e.getMessage());
            }
        }
        return resultMap;
    }

        private List<Map<String, Object>> mergeData(List<Map<String, Object>> df1, List<Map<String, Object>> df2) {
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
        public List<Map<String, Object>> getSelectiveSourceMetadataJoinedWithSettings(String selectiveTables, String connectionId, Connection con) throws SQLException {

            // The Below function shall create two Schema Conditions which are used only in this function
            Map<String, String> conditions  = createQueryConditions("src", schemaName);
            String querySchemaCond1 = conditions.get("query_schema_cond1");
            System.out.println("query_schema_cond1 with alias : " + querySchemaCond1);

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
                        "    AND il.Active_Flag = '1' " +
                        "WHERE " +
                    "    src.Table_Name IN (" + selectiveTables + ") " +
                        "    AND src.Isfileupload != '1' " +
                        "    AND src.Connection_Id = " + connectionId +
                         querySchemaCond1 + ";";
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

        private Map<String, Object> updateIncrementalColumnAndPrepareBulk(Connection connection) throws SQLException, IOException {
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

        private String calculateIncrementalColumn(Map<String, Object> row) {
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
        // FAS - main flow ELT_Selective_Source_Metadata )main  - Used
        // Custome Type is missing  case as well
        public Map<String, Map<String, String>> fetchSelectiveSourceMetadata(
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
        // Used in SAved FAS also same query - USED

        public List <Map<String, Object>> getConstantAnvizentFields(
                Connection connection,
                String dataSourceName,
                String tableName,
                String companyId) {
                    // ADD TODO schemaName and connectionID as input

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
                    "SELECT DISTINCT '" + tableName + "_Key' AS IL_Column_Name, " + //TODO distinct needed ot not SAved
                    "'bigint(32)' AS IL_Data_Type, " + // TODO bigint(32) not needed SAved
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
                    "'Y' AS Constant_Insert_Column, " + // TODO NULL SAved
                    "'" + companyId + "' AS Constant_Insert_Value " + // TODO NULL SAved
                    "FROM `ELT_Selective_Source_Metadata`";

            List<Map<String, Object>> resultList = new ArrayList<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        // String key = resultSet.getString("IL_Column_Name") + "_"
                        //         + resultSet.getString("Constant_Insert_Column");

                        String key = context.get("CONNECTION_ID") + "_"
                                + context.get("Schema_Name") + "_"
                                + tableName;

                        Map<String, Object> transformedRow = new HashMap<>();

                        // Here, you can apply your transformations
                        transformedRow.put("Connection_Id", context.get("CONNECTION_ID")); // TODO, checked in fas
                        transformedRow.put("TABLE_SCHEMA", context.get("Schema_Name") ); // TODO, checked in fas
                        transformedRow.put("Source_Table_Name", tableName); //TODO, checked in fas
                        transformedRow.put("Active_Flag", true); //TODO, checked in fas
            // From Query
                        transformedRow.put("IL_Column_Name", resultSet.getString("IL_Column_Name"));
                        transformedRow.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                        transformedRow.put("Source_Data_Type", resultSet.getString("Source_Data_Type"));
                        transformedRow.put("Constraints", resultSet.getString("Constraints"));
                        transformedRow.put("PK_Constraint", resultSet.getString("PK_Constraint"));
                        transformedRow.put("Constant_Insert_Column", resultSet.getString("Constant_Insert_Column"));
                        transformedRow.put("Constant_Insert_Value", resultSet.getObject("Constant_Insert_Value"));

                        resultList.add(transformedRow); // Map key can be customized based on your transformation
                                                            // needs
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exception appropriately (logging, rethrowing, etc.)
            }
            return resultList;
        }

        // Navin - Saved_FAS
        // row6 1st (top) lookup - USED
        // ELT_Custom_Source_mapping
        public Map<String, Map<String, String>> fetchCustomSourceMetadata(
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
                        rowMap.put("Schema_Name", resultSet.getString("Schema_Name")); // TODO Or TABLE_SCHEMA
                        rowMap.put("Table_Name", resultSet.getString("Table_Name"));
                        rowMap.put("Column_Name", resultSet.getString("Column_Name"));
                        rowMap.put("Column_Data_Type", resultSet.getString("Column_Data_Type")); // TODO check it Data_Type
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
        public Map<String, Map<String, String>> fetchILSourceMappingInfoSaved(
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
        public Map<String, Map<String, Object>> processSelectiveSourceMetadata (
                Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCond) throws SQLException {

            String query = "SELECT Connection_Id, Schema_Name, Table_Name, Column_Name, Dimension_Transaction " +
                    "FROM ELT_Selective_Source_Metadata " +
                    "WHERE Table_Name = ? AND IsFileUpload != '1' AND Connection_Id = ? " + querySchemaCond;

            // Joining Table data
            Map<String, Map<String, Object>> ilSourceMappingData = getILSourceMappingInfo(connection, tableName, connectionId, querySchemaCond);

            List<Map<String, Object>> innerJoinResultList = new ArrayList<>(); // Inner Join Result List; Store into a file
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
                            Map<String, Object> ilmappingValue = ilSourceMappingData.get(key); // TODO Its value only

                            String incrementalColumn = ilmappingValue.get("Dimension_Transaction") == null ? "N"
                                    : ("T".equals(resultSet.getString("Dimension_Transaction"))
                                            && "D".equals(ilmappingValue.get("Dimension_Transaction"))) ? "Y"
                                                    : (String) ilmappingValue.get("Incremental_Column");

                            // components of the key
                            valueMap.putAll(ilSourceMappingData.get(key)); // TODO check that the output fields name are same
                            // Updated values
                            valueMap.put("Dimension_Transaction", resultSet.getString("Dimension_Transaction"));
                            valueMap.put("Incremental_Column", incrementalColumn);
                            valueMap.put("Active_Flag", false);
                            // TODO                 valueMap.put("Updated_Date", updatedDate);

                            innerJoinResultList.add(valueMap);
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

            saveInnerJoinedData(innerJoinResultList);

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

        // Navin Saved
        // for tMap_1 t (lookup)
        public Map<String, Map<String, Object>> getSourceMetadata(Connection connection, String tableName, String connectionId, String querySchemaCond) throws SQLException {
            // Define the query using StringBuilder for compactness
            StringBuilder querySourceMetadata = new StringBuilder();
            querySourceMetadata.append("SELECT ")
                        .append("`Connection_Id`, ").append("`TABLE_SCHEMA`, ").append("`Table_Name`, ").append("`Column_Name`, ")
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
        // TODO check if '7' or just 7 make difference just 7 is recommended
                        // Query for ELT_Source_Metadata from other source TBD deleted
//            StringBuilder querySourceMetadataDummy = new StringBuilder();
//            querySourceMetadata.append("SELECT Connection_Id, TABLE_SCHEMA, Table_Name, Column_Name, ")
//                               .append("LOWER(Data_Type) AS Data_Type, PK_Column_Name, PK_Constraint, ")
//                               .append("FK_Column_Name, FK_Constraint, Prefix_Suffix_Flag, Prefix_Suffix, ")
//                               .append("Added_Date, Added_User, Updated_Date, Updated_User, ")
//                               .append("CASE ")
//                               .append("    WHEN Character_Max_Length < 0 THEN Character_Max_Length ")
//                               .append("    WHEN Data_Type LIKE '%char%' AND Character_Max_Length < 7 THEN 7 ")
//                               .append("    ELSE Character_Max_Length ")
//                               .append("END AS Character_Max_Length, ")
//                               .append("Character_Octet_Length, Numeric_Precision, ")
//                               .append("CASE ")
//                               .append("    WHEN Numeric_Scale IS NULL THEN Numeric_Precision_Radix ")
//                               .append("    ELSE Numeric_Scale ")
//                               .append("END AS Numeric_Scale ")
//                               .append("FROM ELT_Source_Metadata ")
//                               .append("WHERE Table_Name = ? ")
//                               .append("AND Connection_Id = ? ")
//                               .append(querySchemaCond);

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
        // navin internal call above
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
        
        // Navin SAVED
        // main flow - tMAp1;
        // nOT CALLED YET - called into saved
        public List<Map<String, Object>> JoinWithMetadataAndDataTypeConversions(
                Map<String, Map<String, Object>> mainDataMap,
                Map<String, Map<String, Object>> lookupDataMap,
                Map<String, Map<String, Object>> conversionDataTypesMap) {

            //Map<String, Map<String, Object>> result = new HashMap<>();
            //Map<String, Object> resultMap = new HashMap<>();
            List<Map<String, Object>> resultList = new ArrayList<>();

            for (Map.Entry<String, Map<String, Object>> entry : mainDataMap.entrySet()) {
                String mainKey = entry.getKey();
                Map<String, Object> mainValue = entry.getValue();

                Map<String, Object> lookupMetaData = lookupDataMap.getOrDefault(mainKey, new HashMap<>());

                // If a corresponding key exists in lookup data, perform a left outer join
                String dataType = (String) lookupMetaData.get("Data_Type");
                // Map<String, Object> lookupValue = lookupData.getOrDefault(mainKey, new
                // HashMap<>());
                // Retrieve the lookup value
                Map<String, Object> dataTypesConversions = conversionDataTypesMap.getOrDefault(dataType, new HashMap<>());

                // resultMap.putAll(lookupMetaData);
                // resultMap.putAll(mainValue);
                // Put the joined data (main value + lookup value) into the result map
                Map<String, Object> resultValue = new HashMap<>(mainValue);
                resultValue.put("lookup_value", lookupMetaData); // Example key for lookup value

                Map<String, Object> outputMap = new HashMap<>();
                outputMap.put("Connection_Id", mainValue.get("Connection_Id"));
                outputMap.put("Table_Name", mainValue.get("Table_Name"));
                outputMap.put("Column_Name", mainValue.get("Column_Name"));

                Integer maxLength = (Integer) lookupMetaData.get("Character_Max_Length");
                int characterMaxLength = characterMaxLength(maxLength);

                String ilDataType = getIlDataType(lookupMetaData, dataTypesConversions, characterMaxLength);
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

                outputMap.put("Added_Date", Timestamp.valueOf(startTime));
                outputMap.put("Added_User", userName);
                outputMap.put("Updated_Date", Timestamp.valueOf(startTime));
                outputMap.put("Updated_User", userName);

                resultList.add(outputMap);
                //result.put(mainKey, resultValue);
            }

            return resultList;
        }

        // Navin FAS
        // main flow - tMAp2;
        public List<Map<String, Object>> JoinWithMetadataAndDataTypeConversionsForFlagOne(
                Map<String, Map<String, Object>> mainDataMap,
                Map<String, Map<String, String>> lookupDataMap,
                Map<String, Map<String, Object>> conversionDataTypesMap) {

            List<Map<String, Object>> resultList = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : mainDataMap.entrySet()) {
                String mainKey = entry.getKey();
                Map<String, Object> mainValue = entry.getValue();

                Map<String, String> lookupMetaData = lookupDataMap.getOrDefault(mainKey, new HashMap<>());

                // If a corresponding key exists in lookup data, perform a left outer join
                String dataType = (String) lookupMetaData.get("Data_Type");
                // Map<String, Object> lookupValue = lookupData.getOrDefault(mainKey, new
                // HashMap<>());
                // Retrieve the lookup value
                Map<String, Object> dataTypesConversions = conversionDataTypesMap.getOrDefault(dataType, new HashMap<>());

                // resultMap.putAll(lookupMetaData);
                // resultMap.putAll(mainValue);
                // Put the joined data (main value + lookup value) into the result map
                Map<String, Object> resultValue = new HashMap<>(mainValue);
                resultValue.put("lookup_value", lookupMetaData); // Example key for lookup value

                // Output data
                Map<String, Object> outputMap = new HashMap<>();
                outputMap.put("Connection_Id", mainValue.get("Connection_Id"));
                outputMap.put("TABLE_SCHEMA", mainValue.get("Schema_Name"));
                outputMap.put("Table_Name", mainValue.get("Table_Name"));
                outputMap.put("Column_Name", mainValue.get("Column_Name"));

                outputMap.put("Source_Column_Name", lookupMetaData.get("Source_Column_Name"));
                outputMap.put("Constraints", lookupMetaData.get("Constraints") == null ? "" : lookupMetaData.get("Constraints"));
                outputMap.put("Data_Type", lookupMetaData.get("Source_Data_Type"));
                outputMap.put("IL_Data_Type", lookupMetaData.get("Data_Type"));  // TO DO Check in saved flow for Connection_Id wrong value there
                outputMap.put("PK_Column_Name", null);
                outputMap.put("PK_Constraint", null);
                outputMap.put("FK_Column_Name", null);
                outputMap.put("FK_Constraint", null);
                outputMap.put("Prefix_Suffix_Flag", null);
                outputMap.put("Prefix_Suffix", null);

                outputMap.put("Schema_Name", mainValue.get("Schema_Name"));
                outputMap.put("Dimension_Transaction", mainValue.get("Dimension_Transaction"));
                
                String userName = context.get("APP_UN");
                outputMap.put("Added_Date", Timestamp.valueOf(startTime));
                outputMap.put("Added_User", userName);
                outputMap.put("Updated_Date", Timestamp.valueOf(startTime));
                outputMap.put("Updated_User", userName);

                resultList.add(outputMap);
                //result.put(mainKey, resultValue);
            }
            return resultList;
        }

        private int characterMaxLength(Integer maxLength) {
            //Integer characterMaxLength = Integer.parseInt(len);
            int length =  (maxLength != null) ? 2 * maxLength : 0;
            return (length > 255) ? 255 : length; 
        }

        // Navin t_map 1 
        // Saved
        private String getIlDataType(Map<String, Object> metadata, Map<String, Object> dataTypes, int maxLength) {
            String dataType = (String) metadata.get("Data_Type");
            Integer numericPrecision = (Integer) metadata.get("Numeric_Precision");
            Integer numericScale = (Integer) metadata.get("Numeric_Scale");
            Integer characterMaxLength = (Integer) metadata.get("Character_Max_Length"); // transformed

            String defaultFlag = (String) dataTypes.get("Default_Flag");

            if (defaultFlag == null) {
                return dataType;
            }
            if ("y".equalsIgnoreCase(defaultFlag)) {
                return (String) dataTypes.get("IL_Data_Type");
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
        // SaVED cOMP2 - 2  (Differnce is input querySchemaCondition vs querySchemaCondition1)
        public Map<String, String> executeQueryAndBuildMapWithTransaction(
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

            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" +
                                resultSet.getString("Schema_Name") + "_" +
                                resultSet.getString("Table_Name");
                        String value = (String) resultSet.getObject("Dimension_Transaction");
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
        // Saved Comp2 part 3 (querySchemaCondition1 is called )
        public Map<String, String> getActiveAliasValues(
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

            Map<String, String> resultMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("Connection_Id") + "_" + resultSet.getString("Schema_Name");
                        String value = (String) resultSet.getObject("Setting_Value");
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
        // FAS
        // Sharedfolder compoennt
        public Map<String, Object> executeQueryAndBuildMap(
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
        public Map<String, Object> getWSConnectionData(
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
        // TODO Source_Data_Type to be lower()
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                //int i = 1;
                while (resultSet.next()) {
                    String sourceDataType = resultSet.getString("Source_Data_Type");
        
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                    valueMap.put("Default_Flag", resultSet.getString("Default_Flag"));
                    valueMap.put("Precision_Flag", resultSet.getString("Precision_Flag"));
                    valueMap.put("Default_Length", resultSet.getInt("Default_Length"));
                   // System.out.println(i + ": " + sourceDataType);
                    //++i;
                    resultMap.put(sourceDataType, valueMap);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        private List<Map<String, Object>> joinAndCalculateIlDataType(Connection connection, List<Map<String, Object>> OUT) throws SQLException {
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
        // Navin
        // Saved -> Settings comp 1
        public Map<String, String> getActiveAliasValuesTables(Connection connection, String connectionId, String querySchemaCond1) throws SQLException {
            // Define the SQL query using StringBuilder for readability
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("SELECT ")
                        .append("Connection_Id, ")
                        .append("Schema_Name, ")
                        .append("Setting_Value ")
                        .append("FROM ELT_IL_Settings_Info ")
                        .append("WHERE Settings_Category = 'Suffix' ")
                        .append("AND Active_Flag = '1' ")
                        .append("AND Connection_Id = '").append(connectionId).append("' ")
                        .append(querySchemaCond1);
            String query = queryBuilder.toString();
        
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
        
                while (resultSet.next()) {
                    // Construct the key using Connection_Id and Schema_Name
                    String key = resultSet.getString("Connection_Id") + "_" + resultSet.getString("Schema_Name");
                    // Get the Setting_Value
                    String value = resultSet.getString("Setting_Value");
        
                    // Store in the map
                    resultMap.put(key, value);
                }
            }
            return resultMap;
        }

        
        // NAVIN
        // FK_Constraint_LKP  Saved aggreagtion logic
        public Map<String, Map<String, Object>> getAggregatedFKConstraint(Connection connection,
                String tableName,
                String connectionId,
                String querySchemaCond,
                String querySchemaCond1) throws SQLException {
            StringBuilder query = new StringBuilder();
            query.append("SELECT ")
                    .append("src.Connection_Id, ")
                    .append("src.TABLE_SCHEMA, ")
                    .append("src.Table_Name, ")
                    .append("src.Column_Name, ")
                    .append("src.PK_Column_Name AS FK_Column_Name, ")
                    .append("src.PK_Constraint AS FK_Constraint ")
                    .append("FROM ELT_Source_Metadata src ")
                    .append("INNER JOIN ELT_Selective_Source_Metadata sel ")
                    .append("ON src.Connection_Id = sel.Connection_Id ")
                    .append("AND src.TABLE_SCHEMA = sel.Schema_Name ")
                    .append("AND src.Table_Name = sel.Table_Name ")
                    .append("AND src.Column_Name = sel.Column_Name ")
                    .append("WHERE src.PK_Constraint = 'yes' ")
                    .append("AND src.Table_Name = '").append(tableName).append("' ")
                    .append("AND src.Connection_Id = '").append(connectionId).append("' ")
                    .append(querySchemaCond).append(" ")
                    .append("AND sel.IsFileUpload != '1' ")
                    .append("AND sel.Table_Name = '").append(tableName).append("' ")
                    .append("AND sel.Connection_Id = '").append(connectionId).append("' ")
                    .append(querySchemaCond1).append(" ")
                    .append("AND sel.Dimension_Transaction = 'T'");

            Map<String, Map<String, Object>> aggregatedResults = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
                    ResultSet resultSet = preparedStatement.executeQuery()) {

                while (resultSet.next()) {
                    String connectionIdResult = resultSet.getString("Connection_Id");
                    String tableSchema = resultSet.getString("TABLE_SCHEMA");
                    String columnName = resultSet.getString("Column_Name");
                    String tableNameResult = resultSet.getString("Table_Name");
                    String fkColumnName = resultSet.getString("FK_Column_Name");
                    String fkConstraint = resultSet.getString("FK_Constraint");

                    String key = connectionIdResult + "_" + tableSchema + "_" + columnName;

                    Map<String, Object> groupData = aggregatedResults.getOrDefault(key, new HashMap<>());

                    // Aggregate Table_Name as a comma-separated value
                    String aggregatedTableNames = (String) groupData.getOrDefault("Table_Name", "");
                    if (!aggregatedTableNames.isEmpty()) {
                        aggregatedTableNames += ", ";
                    }
                    aggregatedTableNames += tableNameResult;

                    // Update the group data with the last FK_Column_Name and FK_Constraint
                    groupData.put("Table_Name", aggregatedTableNames);
                    groupData.put("FK_Column_Name", fkColumnName);
                    groupData.put("FK_Constraint", fkConstraint);

                    // Put the updated group data back into the aggregated results map
                    aggregatedResults.put(key, groupData);
                }
            }
            return aggregatedResults;
        }

        // Navin - tmap 2
        // Saved main flow comp1 -  output
        public List<Map<String, Object>> processJoinedData(
                List<Map<String, Object>> metadataDataTypeList,
                Map<String, String> aliasValuesTables,
                Map<String, Map<String, Object>> aggregatedQueryResults) {

            // Result map to store final joined data
            //Map<String, Map<String, Object>> resultMap = new HashMap<>();
            List<Map<String, Object>> result = new ArrayList<>();
            // Iterate over the metadata and data type conversions list
            for (Map<String, Object> metadataEntry : metadataDataTypeList) {
                // Extract the keys from metadataEntry
                String connectionId = (String) metadataEntry.get("Connection_Id");
                String schemaName = (String) metadataEntry.get("Table_Schema");
                String tableName = (String) metadataEntry.get("Table_Name");
                String columnName = (String) metadataEntry.get("Column_Name");

                // Form the key for joining with aliasValuesTables
                String aliasKey = connectionId + "_" + schemaName;

                // Form the key for joining with aggregatedQueryResults
                // Navin tableName removed
                String aggregateKey = connectionId + "_" + schemaName + "_" + columnName;
                // String aggregateKey = connectionId + "_" + schemaName + "_" + tableName + "_" + columnName;

                // Create a map to hold joined data
                Map<String, Object> joinedData = new HashMap<>(metadataEntry);

                // LEFT OUTER JOIN with aliasValuesTables using getOrDefault
                // joinedData.put("Alias_Setting_Value", aliasValuesTables.getOrDefault(aliasKey, null));
                String aliasSettingValue = aliasValuesTables.getOrDefault(aliasKey, null);

                // LEFT OUTER JOIN with aggregatedQueryResults using getOrDefault
                Map<String, Object> aggregateData = aggregatedQueryResults.getOrDefault(aggregateKey, new HashMap<>());
                for (Map.Entry<String, Object> entry : aggregateData.entrySet()) {
                    joinedData.put(entry.getKey(), entry.getValue());
                }

                // Transformations
                String pkConstraint = metadataEntry.get("PK_Constraint") == null ? ""
                        : metadataEntry.get("PK_Constraint").equals("yes") ? "PK" : "";

                
                String col = (String) metadataEntry.get("PK_Column_Name");
                String pkColumnName = col == null || col.equals("NULL") || col.isEmpty()
                        ? "" : col;

                String ilTableName = aliasSettingValue == null
                        ? metadataEntry.get("Table_Name").toString()
                        : metadataEntry.get("Table_Name") + "_" + aliasSettingValue; // TODO review Check

                String ilColumnName = metadataEntry.get("Column_Name").toString();

                String fkConstraint = aggregateData.getOrDefault("FK_Constraint", "").toString().isEmpty() ? "" : "FK";

                String fkColumnName = aggregateData.getOrDefault("FK_Column_Name", "").toString().isEmpty()
                        ? "" : aggregatedQueryResults.get("FK_Column_Name").toString();


                //Map<String, Object> joinedData = new HashMap<>();
                // TODO first four ar the keys ...
                joinedData.put("Connection_Id", metadataEntry.get("Connection_Id"));
                joinedData.put("TABLE_SCHEMA", metadataEntry.get("TABLE_SCHEMA"));
                joinedData.put("IL_Table_Name", ilTableName); // Transformed from earlier step
                joinedData.put("IL_Column_Name", ilColumnName); // Transformed from earlier step
                joinedData.put("IL_Data_Type", metadataEntry.get("IL_Data_Type"));
                joinedData.put("Constraints", pkConstraint); // Transformed from earlier step
                joinedData.put("Source_Table_Name", metadataEntry.get("Table_Name"));
                joinedData.put("Source_Column_Name", metadataEntry.get("Column_Name"));
                joinedData.put("Source_Data_Type", metadataEntry.get("Data_Type"));
                joinedData.put("PK_Constraint", pkConstraint); // Transformed from earlier step
                joinedData.put("PK_Column_Name", pkColumnName); // Transformed from earlier step
                joinedData.put("FK_Constraint", fkConstraint); // Transformed from earlier step
                joinedData.put("FK_Column_Name", fkColumnName); // Transformed from earlier step
                joinedData.put("Constant_Insert_Column", null);
                joinedData.put("Constant_Insert_Value", null);
                joinedData.put("Dimension_Transaction", metadataEntry.get("Dimension_Transaction"));
                joinedData.put("Dimension_Key", null);
                joinedData.put("Dimension_Name", null);
                joinedData.put("Dimension_Join_Condition", null);
                joinedData.put("Incremental_Column", 
                    metadataEntry.get("Dimension_Transaction").equals("T") ? "Y" : null);
                joinedData.put("Isfileupload", 0);
                joinedData.put("File_Id", 0);
                joinedData.put("Column_Type", "Source");
                joinedData.put("Active_Flag", true);
                String userName = context.get("APP_UN");
                joinedData.put("Added_Date", Timestamp.valueOf(startTime));
                joinedData.put("Added_User", userName);
                joinedData.put("Updated_Date", Timestamp.valueOf(startTime));
                joinedData.put("Updated_User", userName);
                        
                // // Perform LEFT OUTER JOIN with aliasValuesTables
                // if (aliasValuesTables.containsKey(aliasKey)) {
                //     joinedData.put("Alias_Setting_Value", aliasValuesTables.get(aliasKey));
                // } else {
                //     joinedData.put("Alias_Setting_Value", null); // No match, add null
                // }

                // // Perform LEFT OUTER JOIN with aggregatedQueryResults
                // if (aggregatedQueryResults.containsKey(aggregateKey)) {
                //     joinedData.putAll(aggregatedQueryResults.get(aggregateKey)); // Add all matching fields
                // }

                // Add the joined data to the result map
                //resultMap.put(aggregateKey, joinedData);
                result.add(joinedData);
            }

            return result;
        }

        // Navin - tmap 3
        // FAS main flow comp1 -  output
        public List<Map<String, Object>> processJoinedDataForFlagOne(
                List<Map<String, Object>> mainFlowData,
                Connection dbConnection,
                String connectionId,
                String querySchemaCondition) {

            // Settings
            // querySchemaCondition1
            Map<String, String> aliasValues = getActiveAliasValues(dbConnection, connectionId, querySchemaCondition);
            // 3. row2
            Map<String, Object> sharedFolderData = executeQueryAndBuildMap(dbConnection, connectionId);
            // 4. row4
            Map<String, Object> webserviceData = getWSConnectionData(dbConnection, connectionId);

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map<String, Object> metadataEntry : mainFlowData) {
                // Extract the keys from metadataEntry
                //String connectionId = (String) metadataEntry.get("Connection_Id");
                String schemaName = (String) metadataEntry.get("Table_Schema");
                String tableName = (String) metadataEntry.get("Table_Name");
                String columnName = (String) metadataEntry.get("Column_Name");

                // Form the key for joining with aliasValuesTables
                String aliasKey = connectionId + "_" + schemaName;

                // Create a map to hold joined data
                Map<String, Object> joinedData = new HashMap<>(metadataEntry);

                // LEFT OUTER JOIN with aliasValuesTables
                String aliasSettingValue = aliasValues.getOrDefault(aliasKey, null);
                String row2Suffix = (String) sharedFolderData.getOrDefault(aliasKey, ""); // TODO "" or NULL
                String row4Suffix = (String) webserviceData.getOrDefault(connectionId, null); // TODO look at DB

                // Transformations
                String settingValue;
                String customType = context.get("Custom_Type");
                if (customType.equals("shared_folder")) {
                    settingValue = row2Suffix;
                } else if (customType.equals("web_service") ||
                        customType.equals("OneDrive") ||
                        customType.equals("SageIntacct")) {
                    settingValue = row4Suffix;
                } else {
                    settingValue = aliasSettingValue;
                }

                String ilTableName = aliasSettingValue == null
                        ? metadataEntry.get("Table_Name").toString()
                        : metadataEntry.get("Table_Name") + "_" + settingValue; // TODO review Check in counterpart too

                String ilColumnName = metadataEntry.get("Column_Name").toString();

                //Map<String, Object> joinedData = new HashMap<>();
                // TODO first four ar the keys ...
                joinedData.put("Connection_Id", metadataEntry.get("Connection_Id"));
                joinedData.put("TABLE_SCHEMA", metadataEntry.get("TABLE_SCHEMA"));
                joinedData.put("IL_Table_Name", ilTableName); // Transformed from earlier step
                joinedData.put("IL_Column_Name", ilColumnName); // Transformed from earlier step
                joinedData.put("IL_Data_Type", metadataEntry.get("IL_Data_Type"));
                joinedData.put("Constraints", metadataEntry.get("Constraints"));
                joinedData.put("Source_Table_Name", metadataEntry.get("Table_Name"));
                joinedData.put("Source_Column_Name", metadataEntry.get("Source_Column_Name")); // TODO check counterpart
                joinedData.put("Source_Data_Type", metadataEntry.get("Data_Type"));
                joinedData.put("PK_Constraint", null);
                joinedData.put("PK_Column_Name", null);
                joinedData.put("FK_Constraint", null);
                joinedData.put("FK_Column_Name", null);
                joinedData.put("Constant_Insert_Column", null);
                joinedData.put("Constant_Insert_Value", null);
                joinedData.put("Dimension_Transaction", metadataEntry.get("Dimension_Transaction"));
                joinedData.put("Dimension_Key", null);
                joinedData.put("Dimension_Name", null);
                joinedData.put("Dimension_Join_Condition", null);
                joinedData.put("Incremental_Column",
                        metadataEntry.get("Dimension_Transaction").equals("T") ? "Y" : null);
                joinedData.put("Isfileupload", 0);
                joinedData.put("File_Id", 0);
                joinedData.put("Column_Type", "Source");
                joinedData.put("Active_Flag", true);
                String userName = context.get("APP_UN");
                joinedData.put("Added_Date", Timestamp.valueOf(startTime));
                joinedData.put("Added_User", userName);
                joinedData.put("Updated_Date", Timestamp.valueOf(startTime));
                joinedData.put("Updated_User", userName);

                // Add the joined data to the result map
                //resultMap.put(aggregateKey, joinedData);
                result.add(joinedData);
            }

            return result;
        }

        // Navin
        // FAS Comp2 output
        public List<Map<String, Object>> processConstantFieldsWithMetadataAndAliases (
                Connection connection,
                String dataSourceName,
                String tableName,
                String companyId,
                String connectionId,
                String querySchemaCondition) {

            // Step 1: Call the functions and retrieve their results
            List<Map<String, Object>> constantFields = getConstantAnvizentFields(connection, dataSourceName, tableName,
                    companyId);
            Map<String, String> transactionData = executeQueryAndBuildMapWithTransaction(connection, connectionId,
                    tableName, querySchemaCondition);
            Map<String, String> aliasValues = getActiveAliasValues(connection, connectionId, querySchemaCondition);

            // Step 2: Create the result map
            List<Map<String, Object>> result = new ArrayList<>();

            // Step 3: Iterate over constantFields data and perform LEFT OUTER JOINs
            for (Map<String, Object> constantEntry : constantFields) {
                // String key = constantEntry.getKey();

                String sourceTablename = (String) constantEntry.get("Source_Table_Name");
                String key1 = (String) constantEntry.get("Connection_Id") + "_" // TO connectionID is long
                + (String) constantEntry.get("TABLE_SCHEMA") + "_"
                + sourceTablename;

                String key2 = (String) constantEntry.get("Connection_Id") + "_" // TO connectionID is long
                + (String) constantEntry.get("TABLE_SCHEMA");

                // Map<String, Object> constantData = (Map<String, Object>) constantEntry.getValue();

                // Fetch lookup data using keys, defaulting to empty map if key not found
                String dimensionTransaction = transactionData.getOrDefault(key1, "");
                String settingValue = aliasValues.getOrDefault(key2, "");


                String ilTableName = (settingValue == null) || settingValue.equals("") ? sourceTablename : sourceTablename + "_" + settingValue;

                // Create a combined map for the current key
                //Map<String, Object> combinedData = new HashMap<>(constantData);

                Map<String, Object> combinedData = new HashMap<>();

                combinedData.put("Connection_Id", constantEntry.get("Connection_Id"));
                combinedData.put("TABLE_SCHEMA", constantEntry.get("TABLE_SCHEMA"));
                combinedData.put("IL_Table_Name", ilTableName); // from the earlier transformation
                combinedData.put("IL_Column_Name", constantEntry.get("IL_Column_Name"));
                combinedData.put("IL_Data_Type", constantEntry.get("IL_Data_Type"));
                combinedData.put("Constraints", constantEntry.get("Constraints"));
                combinedData.put("Source_Table_Name", null);
                combinedData.put("Source_Column_Name", null);
                combinedData.put("Source_Data_Type", constantEntry.get("Source_Data_Type"));
                combinedData.put("PK_Constraint", constantEntry.get("PK_Constraint"));
                combinedData.put("PK_Column_Name", null);
                combinedData.put("FK_Constraint", null);
                combinedData.put("FK_Column_Name", null);
                combinedData.put("Constant_Insert_Column", constantEntry.get("Constant_Insert_Column"));
                combinedData.put("Constant_Insert_Value", constantEntry.get("Constant_Insert_Value"));
                combinedData.put("Dimension_Transaction", dimensionTransaction); // assuming sm is a valid object
                combinedData.put("Dimension_Key", null);
                combinedData.put("Dimension_Name", null);
                combinedData.put("Dimension_Join_Condition", null);
                combinedData.put("Incremental_Column", null);
                combinedData.put("Isfileupload", 0);
                combinedData.put("File_Id", 0);
                combinedData.put("Column_Type", "Anvizent");
                combinedData.put("Active_Flag", constantEntry.get("Active_Flag"));
                String userName = context.get("APP_UN");
                combinedData.put("Added_Date", Timestamp.valueOf(startTime));
                combinedData.put("Added_User", userName);
                combinedData.put("Updated_Date", Timestamp.valueOf(startTime));
                combinedData.put("Updated_User", userName);

                // combinedData.putAll(transactionLookup); // Merge transaction data
                // combinedData.putAll(aliasLookup); // Merge alias data

                // Add combined data to the result list of maps
                result.add(combinedData);
            }

            return result;
        }

        // Navin FAS
        // SAVED FAS Comp2 output - COpiED from SAVED - Completed
        public List<Map<String, Object>> processConstantFieldsWithMetadataSharedFolderWebServiceAndAliases (
                Connection connection,
                String dataSourceName,
                String tableName,
                String companyId,
                String connectionId,
                String querySchemaCondition) {

            // Step 1: Call the functions and retrieve their results
            // TODO same as SAVED
            List<Map<String, Object>> constantFields = getConstantAnvizentFields(connection, dataSourceName, tableName,
                    companyId);
            // TODO same as SAVED - uses querySchemaCondition1
            // 2. sm
            Map<String, String> transactionData = executeQueryAndBuildMapWithTransaction(connection, connectionId,
                    tableName, querySchemaCondition1);
            // 3. row3
            Map<String, Object> sharedFolderData = executeQueryAndBuildMap(
                    dbConnection, connectionId);
            // 4. row5
            Map<String, Object> webserviceData = getWSConnectionData(
                    dbConnection, connectionId);
            // TODO check the sequence and all the aliasea up and down
            // 5. ELT_Active_aliases querySchemaCondition1
            // cp
            Map<String, String> aliasValues = getActiveAliasValues(connection, connectionId, querySchemaCondition1);

            // Step 2: Create the result map
            List<Map<String, Object>> result = new ArrayList<>();

            // Step 3: Iterate over constantFields data and perform LEFT OUTER JOINs
            for (Map<String, Object> constantEntry : constantFields) {
                // String key = constantEntry.getKey();

                String sourceTablename = (String) constantEntry.get("Source_Table_Name");
                // TODO for sm
                String smKey1 = (String) constantEntry.get("Connection_Id") + "_" // TO connectionID is long
                        + (String) constantEntry.get("TABLE_SCHEMA") + "_"
                        + sourceTablename;
                String dimensionTransaction = transactionData.getOrDefault(smKey1, ""); // SM

                // TODO sm.connection_ID for cp, row3
                // sm.connectionId and that is from constantEntry are same.
                String cpKey2 = (String) constantEntry.get("Connection_Id") + "_" // TO connectionID is long
                        + (String) constantEntry.get("TABLE_SCHEMA");
                String row3Suffix = (String) sharedFolderData.getOrDefault(cpKey2, ""); // TODO "" or NULL
                String row5Suffix = (String) webserviceData.getOrDefault(cpKey2, "");

                // TODO sm.connectionID for row5
                String smConnectionId = (String) constantEntry.get("Connection_Id");
                // Map<String, Object> constantData = (Map<String, Object>) constantEntry.getValue();
                String cpSettingValue = aliasValues.getOrDefault(smConnectionId, "");

//                // Fetch lookup data using keys, defaulting to empty map if key not found
//                String settingValue = aliasValues.getOrDefault(cpKey2, ""); // CP // TODO NULL or ""
//                String sharedFolder = sharedFolderData.getOrDefault(key2, "");  // TODO "" or NULL
                String customType = context.get("Custom_Type");
                String settingValue = null;
                // Expressions
                if (customType.equals("shared_folder")) {
                    settingValue = row3Suffix;
                } else if (customType.equals("web_service") ||
                        customType.equals("OneDrive") || customType.equals("SageIntacct")) {
                    settingValue = row5Suffix;
                } else {
                    settingValue = cpSettingValue;
                }
                String ilTableName = (settingValue == null) || settingValue.equals("") ? sourceTablename : sourceTablename + "_" + settingValue;

                // Create a combined map for the current key
                //Map<String, Object> combinedData = new HashMap<>(constantData);

                Map<String, Object> combinedData = new HashMap<>();

                combinedData.put("Connection_Id", constantEntry.get("Connection_Id"));
                combinedData.put("TABLE_SCHEMA", constantEntry.get("TABLE_SCHEMA"));
                combinedData.put("IL_Table_Name", ilTableName); // from the earlier transformation
                combinedData.put("IL_Column_Name", constantEntry.get("IL_Column_Name"));
                combinedData.put("IL_Data_Type", constantEntry.get("IL_Data_Type"));
                combinedData.put("Constraints", constantEntry.get("Constraints"));
                combinedData.put("Source_Table_Name", null);
                combinedData.put("Source_Column_Name", null);
                combinedData.put("Source_Data_Type", constantEntry.get("Source_Data_Type"));
                combinedData.put("PK_Constraint", constantEntry.get("PK_Constraint"));
                combinedData.put("PK_Column_Name", null);
                combinedData.put("FK_Constraint", null);
                combinedData.put("FK_Column_Name", null);
                combinedData.put("Constant_Insert_Column", constantEntry.get("Constant_Insert_Column"));
                combinedData.put("Constant_Insert_Value", constantEntry.get("Constant_Insert_Value"));
                combinedData.put("Dimension_Transaction", dimensionTransaction); // assuming sm is a valid object
                combinedData.put("Dimension_Key", null);
                combinedData.put("Dimension_Name", null);
                combinedData.put("Dimension_Join_Condition", null);
                combinedData.put("Incremental_Column", null);
                combinedData.put("Isfileupload", 0);
                combinedData.put("File_Id", 0);
                combinedData.put("Column_Type", "Anvizent"); // Fixed string
                combinedData.put("Active_Flag", constantEntry.get("Active_Flag"));
                combinedData.put("Added_Date", Timestamp.valueOf(startTime));
                combinedData.put("Added_User", userName);
                combinedData.put("Updated_Date", Timestamp.valueOf(startTime));
                combinedData.put("Updated_User", userName);

                result.add(combinedData);
            }

            return result;
        }

        private List<Map<String, Object>> mergeMetadataAndCalculateConstraints(Connection connection, List<Map<String, Object>> eltSourceMetadataAdd) throws SQLException {
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
        
        
        private List<Map<String, Object>> createMetadataConstants(Connection connection) throws SQLException {
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
        
        // Navin - reused existing
        // Uniting the two result sets
        private List<Map<String, Object>> uniteResultOut2(List<Map<String, Object>> list1, List<Map<String, Object>> list2) {
            // Determine the common set of keys
            Set<String> keysList1 = new HashSet<>();
            Set<String> keysList2 = new HashSet<>();

            if (!list1.isEmpty()) {
                keysList1 = new HashSet<>(list1.get(0).keySet());
            }
            if (!list2.isEmpty()) {
                keysList2 = new HashSet<>(list2.get(0).keySet());
            }

            Set<String> commonKeys = getCommonKeys(keysList1, keysList2);
//            List<Map<String, Object>> list1out = list1.stream().map(row -> filterCommonKeys(row, commonKeys)).collect(Collectors.toList());
//            List<Map<String, Object>> list2out = list2.stream().map(row -> filterCommonKeys(row, commonKeys)).collect(Collectors.toList());

            List<Map<String, Object>> list1out = new ArrayList<>();

            for (Map<String, Object> row : list1) {
                Map<String, Object> filteredRow = filterCommonKeys(row, commonKeys);
                list1out.add(filteredRow);
            }


            List<Map<String, Object>> list2out = new ArrayList<>();

            for (Map<String, Object> row : list2) {
                Map<String, Object> filteredRow = filterCommonKeys(row, commonKeys);
                list2out.add(filteredRow);
            }


            List<Map<String, Object>> combinedList = Stream.concat(list1out.stream(), list2out.stream())
                    .collect(Collectors.toList());

            return combinedList;
            // Combine the two lists, keeping only the common keys
//            return Stream.concat(
//                    list1.stream().map(row -> filterCommonKeys(row, commonKeys)),
//                    list2.stream().map(row -> filterCommonKeys(row, commonKeys))
//                )
//                .collect(Collectors.toList());
        }

        private Set<String> getCommonKeys(Set<String> set1, Set<String> set2) {
            if (set1.isEmpty() && set2.isEmpty()) {
                return Collections.emptySet(); // if both are empty
            } else if (set1.isEmpty()) {
                return new HashSet<>(set2); // if set1 is empty
            } else if (set2.isEmpty()) {
                return new HashSet<>(set1); // if set2 is empty
            }

            // if both sets are non-empty, retain only the common elements
            Set<String> commonKeys = new HashSet<>(set1);
            commonKeys.retainAll(set2);

            return commonKeys;
        }
// Navin - New
// Used in both the flows - checkd in FAS,
        public boolean updateActiveFlag(Connection connection, String connectionId, String querySchemaCond, boolean activeFlagValue) {
            String query = "UPDATE ELT_IL_Source_Mapping_Info_Saved " +
                           "SET Active_Flag = ? " +
                           "WHERE Connection_Id = ? " + querySchemaCond;
        
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

        /**
         * Updates the Dimension_Transaction field in the ELT_IL_Source_Mapping_Info_Saved table.
         *
         * This method sets the Dimension_Transaction column for records matching the specified
         * Data Warehouse table name (IL_Table_Name) and connection ID (Connection_Id).
         * Additional conditions can be applied using the querySchemaCond parameter.
         *
         * @param connection       The database connection
         * @param dimTransType     The type of dimension transaction to set
         * @param dwTableName      The name of the Data Warehouse table.
         * @param connectionId     The ID of the database connection.
         * @param querySchemaCond  Additional query conditions to apply, can be null.
         * @return true if records were updated successfully, false otherwise.
         * @throws IllegalArgumentException if connection, dimTransType, dwTableName, or connectionId is null.
         */
        public boolean updateDimensionTransaction(Connection connection, String dimTransType, String dwTableName,
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
        // Applicable to both iterations - Second delete operation outside

// Duplicate. get rid of another one - deleteExistingRecordsForSourceTableName
        /**
         * Deletes records from the ELT_IL_Source_Mapping_Info_Saved table based on specific conditions.
         *
         * This method removes records where the following conditions are met:
         * - The record is active (Active_Flag=1).
         * - The Constant_Insert_Column is NULL, or not equal to 'Y' unless the IL_Column_Name is 'DataSource_Id'.
         * - The Constraints column does not contain 'SK,PK'.
         * - The Connection_Id and Source_Table_Name match the provided values.
         * - Additional conditions specified by the querySchemaCond parameter (if provided).
         *
         * @param connection       The database connection
         * @param connectionId     The ID of the database connection.
         * @param tableName        The name of the source table.
         * @param querySchemaCond  Additional query conditions to apply, can be null.
         * @return true if records were deleted successfully, false otherwise.
         * @throws IllegalArgumentException if connection, connectionId, or tableName is null.
         */
        public boolean deleteRecordsFromILSourceMappingInfoSaved(Connection connection, String connectionId, String tableName, String querySchemaCond) {
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

        // Navin
        // To insert data into the table
        // Final step
        public int saveDataSourceMappingInfoIntoDB(Connection connection, String tableName, List<Map<String, Object>> data) throws SQLException {
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("Data list cannot be null or empty");
            }
            int rowsAffected;
            // Build SQL query dynamically
            String[] columns = data.get(0).keySet().toArray(new String[0]);
            // String placeholders = String.join(", ", new String[new String(columns.length).replace("\0", "?")]);
            // String placeholders = String.join(", ", new String[new String(columns.length).replace("\0", "?")]);
            // Create the placeholders for the SQL query
            StringJoiner placeholders = new StringJoiner(",", "", "");
            for (int i = 0; i < columns.length; i++) {
                placeholders.add("?");
        
            }

            String sql = "INSERT INTO " + tableName + " (" + String.join(", ", columns) + ") VALUES (" + placeholders + ")";
            boolean originalAutoCommit = connection.getAutoCommit();
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                connection.setAutoCommit(false); // Enable transaction for batch insert
    
                for (Map<String, Object> row : data) {
                    int i = 1;
                    for (String column : columns) {
                        preparedStatement.setObject(i++, row.get(column));
                    }
                    preparedStatement.addBatch();
                }

                int[] result = preparedStatement.executeBatch();
                rowsAffected = getNumberOfRowsUpdated(result);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                // Restore the original auto-commit setting
                connection.setAutoCommit(originalAutoCommit);
            }
            return rowsAffected;
        }
        /**
         * Calculates the total number of rows affected based on the batch execution result.
         *
         * @param result The array returned by executeBatch(), indicating rows affected.
         * @return Total number of rows affected.
         */
        private int getNumberOfRowsUpdated(int[] result) {
            int totalRowsAffected = 0;
            for (int count : result) {
                if (count != Statement.EXECUTE_FAILED) {
                    totalRowsAffected += (count == Statement.SUCCESS_NO_INFO ? 0 : count);
                }
            }
            System.out.println("Total Rows Affected: " + totalRowsAffected);
            return totalRowsAffected;
        }

        private void bulk(Connection connection, List<Map<String, Object>> row1) throws SQLException, IOException {
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

       
        
        private Map<String, Object> filterCommonKeys(Map<String, Object> row, Set<String> commonKeys) {

            if (row == null || row.isEmpty()) {
                return Collections.emptyMap();  // Return an empty map if the input is null or empty
            }

            Map<String, Object> result = new HashMap<>();
            int i = 0;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                //System.out.println("i: " + i++);
                if (commonKeys.contains(entry.getKey())) {
                   // System.out.println(entry.getKey());
//                    System.out.println(entry.getKey());
                    result.put(entry.getKey(), entry.getValue());
                }
            }

            return result;

//            return row.entrySet().stream()
//                .filter(entry -> commonKeys.contains(entry.getKey()))
//                .collect(Collectors.toMap(
//                    Map.Entry::getKey,
//                    Map.Entry::getValue,
//                    (v1, v2) -> v1,
//                    HashMap::new
//                ));
        }

        private Map<String, Object> calculateConstraints(Map<String, Object> row) {
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

        private List<Map<String, Object>> createFinalResult(List<Map<String, Object>> mergedData) {
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
        
        
        private int calculateVar1(Map<String, Object> row) {
            Integer characterMaxLength = (Integer) row.get("Character_Max_Length");
            return (characterMaxLength != null) ? 2 * characterMaxLength : 0;
        }

        private String calculateIlDataType(Map<String, Object> row) {
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

    static public class DBHelper {
        // Helper function to return a Connection object
        public static Connection getConnection(DataSourceType dataSourceType, String dbDetails) throws SQLException {
            Connection connection = null;

            JSONObject jsonDbDetails = new JSONObject(dbDetails);
            String serverIP = jsonDbDetails.getString("appdb_hostname");
            String serverPort = jsonDbDetails.getString("appdb_port");
            String serverIPAndPort = serverIP + ":" + serverPort;
            String schema = jsonDbDetails.getString("appdb_schema");
            String userName = jsonDbDetails.getString("appdb_username");
            String password = jsonDbDetails.getString("appdb_password");
            String vendorType = jsonDbDetails.getString("appdb_vendor_type");


            switch (dataSourceType) {
                case MYSQL:
                    // MySQL Connection
                    String mysqlUrl = "jdbc:mysql://" + serverIPAndPort + "/" + schema + "?noDatetimeStringSync=true";
                    // mysqlUrl = "jdbc:mysql://"+ serverIPAndPort +"/"+ schema +"?useUnicode=true&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true";
                    connection = DriverManager.getConnection(mysqlUrl, userName, password);

                    // Enable auto-commit
                    connection.setAutoCommit(true);
                    if (connection != null) {
                        System.out.println("DB Connection established");
                    }
                    break;
                case SNOWFLAKE:
                case SQLSERVER:
                    throw new SQLException(dataSourceType + " is not supported yet.");
                default:
                    throw new SQLException("Unsupported DataSourceType: " + dataSourceType);
            }

            return connection;
        }
    }
}

// TODo Might need it when added into

//// Enum to represent different data source types
//enum DataSourceType {
//    MYSQL,
//    SNOWFLAKE,
//    SQLSERVER
//}
// Enum to represent different return status
//enum Status {
//    SUCCESS,
//    FAILURE
//}
