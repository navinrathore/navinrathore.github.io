//package com.anvizent.elt.ui.jobopearations;
package com.anvizent.datamart;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.json.JSONObject;

public class DWScriptsGenerator {

    public static final String DATALOAD_PROPERTIES = "dataloadproperties";
    // Keys of "dataloadproperties"
    public static final String WRITE_MODE = "write_mode";
    public static final String CUSTOM_FILTER = "custom_filter";
    public static final String CONDITIONAL = "conditional";
    public static final String LIMIT = "limit";
    public static final String HISTORICAL = "historical";
    public static final String INCREMENTAL = "incremental";
    public static final String DELETE_FLAG = "delete_flag";
    // Sub-keys
    public static final String CONDITIONAL_DATE_PARAM = "conditional_date_param";
    public static final String CONDITIONAL_MONTH_PARAM = "trailing_months";
    public static final String HISTORICAL_DATE_PARAM = "historical_date_param";
    public static final String INCREMENTAL_REFRESH_COLUMN_TYPE = "incremental_refresh_column_type";
    public static final String INCREMENTAL_REFRESH_COLUMN_NAME = "incremental_refresh_column_name";

     
    private static final String DELETES_CONFIG_FILE_STRING = "_Deletes_Config_File_";
    private static final String STG_CONFIG_FILE_STRING = "_Stg_Config_File_";
    private static final String CONFIG_FILE_STRING = "_Config_File_";
    private static final String STG_KEYS_CONFIG_FILE_STRING = "_Stg_Keys_Config_File_";
    
    private long clientId;
    private String filePath;
    private String dbDetails;
    private LocalDateTime startTime;
    private String startTimeString;


    private String userName = "ETL Admin"; // default user
    private String schemaName;
    private String loadType;
    private String multiIlConfigFile;
    private String connectionId;
    private String dataSourceName;
    private String historicalDataFlag;
    private String selectTables;
    private String querySchemaCondition = ""; // based on TABLE_SCHEMA
    private String querySchemaCondition1 = ""; // based on Schema_Name
    Map<String, Map<String, String>> datatypeConversionsGlobal;

    SQLQueries sqlQueries;
    Connection conn;

    public DWScriptsGenerator(String clientId,
            String schemaName,
            String connectionId,
            String dataSourceName,
            String loadType,
            String multiILConfigFile,
            String historicalDataFlag,
            String selectTables,
            String dbDetails,
            String filePath) {
        this.dbDetails = dbDetails;
        this.clientId = Long.parseLong(clientId);
        this.schemaName = schemaName;
        this.connectionId = connectionId;
        this.dataSourceName = dataSourceName;
        this.loadType = loadType;
        this.multiIlConfigFile = multiILConfigFile;
        this.historicalDataFlag = historicalDataFlag;
        this.selectTables = selectTables;
        this.filePath = filePath;

        init();
    }

    private void init() {
        startTime = LocalDateTime.now();
        startTimeString = getCurrentDateFormatted(startTime);
        sqlQueries = new SQLQueries();
        try {
            // App DB connection
            conn = DBHelper.getConnection(DataSourceType.MYSQL, dbDetails);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        JSONObject jsonDbDetails = new JSONObject(dbDetails);
        userName = jsonDbDetails.getString("appdb_username");

        // The Below function shall create two Schema Conditions which are used
        // throughout the application
        Map<String, String> conditions = createQueryConditions(null, schemaName);
        this.querySchemaCondition = conditions.get("query_schema_cond");
        this.querySchemaCondition1 = conditions.get("query_schema_cond1");

        datatypeConversionsGlobal = getDatatypeConversions(conn);

    }

    public String getTimeStamp() {
        return startTimeString;
    }
    /**
     * Generates query conditions based on the provided schema name and optional
     * table alias.
     *
     * If a table alias is provided, it prefixes the schema references in the
     * generated conditions.
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
  
    private String getConfigFileName(String ilTableName, String configFileString) {
        String suffix = getTimeStamp();
        String configFileName = filePath + ilTableName + configFileString + clientId + suffix + ".config.properties"; // filePath is the directory name
        return configFileName;
    }

    // row7, datatype_conversion
    // Global moved from Value Script
    private Map<String, Map<String, String>> getDatatypeConversions(Connection connection) {
        String query = "SELECT " +
                        "  Id, " +
                        "  LOWER(Source_Data_Type) AS sourceDataType, " +
                        "  LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS ilDataType, " +
                        "  Java_Data_Type AS javaDataType, " +
                        "  Cleansing_Value AS cleansingValue, " +
                        "  PK_Cleansing_Value AS pkCleansingValue " +
                        "FROM ELT_Datatype_Conversions";
    
        Map<String, Map<String, String>> datatypeConversionsMap = new HashMap<>();
        // TODO make it global
        String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
        if (dateFormat == null) {
            dateFormat = "yyyy-MM-dd";
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String ilDataType = resultSet.getString("ilDataType");

                Map<String, String> details = new HashMap<>();
                details.put("id", String.valueOf(resultSet.getInt("Id")));
                details.put("sourceDataType", resultSet.getString("sourceDataType"));
                details.put("javaDataType", resultSet.getString("javaDataType"));
                details.put("cleansingValue", resultSet.getString("cleansingValue"));
                details.put("pkCleansingValue", resultSet.getString("pkCleansingValue"));

                datatypeConversionsMap.put(ilDataType, details);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return datatypeConversionsMap;
    }

    public void printDatatypeConversionsGlobal(Map<String, Map<String, String>> datatypeConversionsGlobal) {
        for (Map.Entry<String, Map<String, String>> entry : datatypeConversionsGlobal.entrySet()) {
            System.out.println("Key: " + entry.getKey());
            for (Map.Entry<String, String> innerEntry : entry.getValue().entrySet()) {
                System.out.println("  " + innerEntry.getKey() + ": " + innerEntry.getValue());
            }
        }
    }

    // ELT_Generate_parent
    public void generateScripts() {
        // The DB scripts generator
        if (new DWDBScriptsGenerator().generateDBScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The configs script generator
        if (new DWConfigScriptsGenerator().generateConfigScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The values script generator
        if (new DWValueScriptsGenerator().generateValueScript() != Status.SUCCESS) {
            System.out.println("Value script generation failed. Stopping process.");
            return;
        }

        // The Source Info script generator
        if (new DWSourceInfoScriptsGenerator().generateSourceInfoScript() != Status.SUCCESS) {
            System.out.println("Source Info script generation failed. Stopping process.");
            return;
        }

        // The Table Info script generator
        if (new DWTableInfoScriptsGenerator().generateTableInfoScript() != Status.SUCCESS) {
            System.out.println("Table Info script generation failed. Stopping process.");
            return;
        }
    }
    
    /**
     * get data load peroperties from the `Settings` field of the table
     * `ELT_IL_Load_Configs`, parses Settings value as JSON, and
     * returns a map of the "dataloadproperties" key.
     */
    public Map<String, Object> getLoadProperties(Connection connection, String connectionId, String ilTableName)
            throws SQLException {
        String query = "SELECT `Settings` " +
                "FROM `ELT_IL_Load_Configs` " +
                "WHERE `Connection_Id` = ? AND `IL_Table_Name` = ?";

        Map<String, Object> resultMap = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, connectionId);
            preparedStatement.setString(2, ilTableName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String settingsJson = resultSet.getString("Settings");
                    // TODO test purpose
                    settingsJson = "{\"dataloadproperties\":{"
                            + "\"write_mode\":\"upsert\","
                            + "\"custom_filter\":\"sample_filter\","
                            + "\"conditional\":{"
                            + "\"conditional_date_param\":\"1 July 2025\","
                            + "\"trailing_months\":\"July\""
                            + "},"
                            + "\"limit\":\"sample_limit\","
                            + "\"historical\":{"
                            + "\"historical_date_param\":\"1 dec 2024\""
                            + "},"
                            + "\"incremental\":{"
                            + "\"incremental_refresh_column_type\":\"date\","
                            + "\"incremental_refresh_column_name\":\"incre_name\""
                            + "},"
                            + "\"delete_flag\":false"
                            + "}}";
                    // System.out.println(settingsJson);
                    // id, date and other for type

                    JSONObject settings = new JSONObject(settingsJson);
                    if (settings.has("dataloadproperties")) {
                        JSONObject dataloadProperties = settings.getJSONObject("dataloadproperties");
                        resultMap = dataloadProperties.toMap();
                    }
                }
            }
        }
        return resultMap;
    }

    // Both delete values group initial query. differnce is Dimension_Transaction
    // Also, valid for two Config Groups
    private List<Map<String, String>> getILTableNamesWithDimentionTransactionFilter(
            Connection connection,
            String selectiveTables,
            String dimensionTransaction,
            String connectionId,
            String querySchemaCond, String limitFunct) throws SQLException {

        String query = "SELECT DISTINCT \n" +
                "    Connection_Id, \n" +
                "    Table_Schema, \n" +
                "    IL_Table_Name \n" +
                "FROM ELT_IL_Source_Mapping_Info_Saved \n" +
                "WHERE IL_Table_Name IN (" + selectiveTables + ") \n" +
                "  AND Dimension_Transaction = ? \n" +
                "  AND Connection_Id = ? " + querySchemaCond + " " + limitFunct;

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, connectionId);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<Map<String, String>> results = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, String> row = new HashMap<>();
                    row.put("Connection_Id", resultSet.getString("Connection_Id"));
                    row.put("Table_Schema", resultSet.getString("Table_Schema"));
                    row.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));
                    results.add(row);
                }
                return results;
            }
        }
    }

    // Timestamp in specific format
    public String getCurrentDateFormatted(LocalDateTime now) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        String formattedDateTime = now.format(formatter);
        formattedDateTime = formattedDateTime.replace(" ", "_");

        // Removing dashes, colons and period
        formattedDateTime = formattedDateTime.replace("-", "").replace(":", "");
        formattedDateTime = formattedDateTime.replace(".", "");
        System.out.println("Current Formatted Date and Time: " + formattedDateTime);
        return formattedDateTime;
    }

    public static void writeToFile(String data, String fileName) {
        try {
            File file = new File(fileName);
            if (fileName != null && !(fileName.isEmpty())) {
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileWriter writer = new FileWriter(fileName);
                writer.write(data);
                System.out.println("Data successfully written to " + fileName);
                writer.close();
            }
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }

    /**
     * Inserts multiple rows into the specified table using a dynamically constructed SQL INSERT statement.
     *
     * @param connection  the database connection
     * @param tableName   the name of the table where the rows will be inserted
     * @param data        a list of maps representing rows to insert, where keys are column names and values are the data
     * @return the total number of rows successfully inserted
     * @throws IllegalArgumentException if the data list is null or empty
     * @throws SQLException if a database access error occurs or the transaction fails
     */
        public int saveDataIntoDB(Connection connection, String tableName, List<Map<String, Object>> data) throws SQLException {
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("Data list cannot be null or empty");
            }
            int rowsAffected;
            // Building SQL query dynamically
            String[] columns = data.get(0).keySet().toArray(new String[0]);
            // Creating the placeholders for the SQL query
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
                System.out.println("Total Rows " + rowsAffected + " inserted into the table " + tableName);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
            return rowsAffected;
        }
        // Not in use
        public int updateDataInDB(Connection connection, String tableName, List<Map<String, Object>> data, String whereClause, Object[] whereValues) throws SQLException {
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("Data list cannot be null or empty");
            }
            int rowsAffected;
            
            // Building SQL query dynamically
            String[] columns = data.get(0).keySet().toArray(new String[0]);
            
            StringBuilder setClause = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                setClause.append(columns[i]).append(" = ?");
                if (i < columns.length - 1) {
                    setClause.append(", ");
                }
            }
        
            String sql = "UPDATE " + tableName + " SET " + setClause + " WHERE " + whereClause;
            
            boolean originalAutoCommit = connection.getAutoCommit();
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                connection.setAutoCommit(false); // Enable transaction for batch update
        
                for (Map<String, Object> row : data) {
                    int i = 1;
                    for (String column : columns) {
                        preparedStatement.setObject(i++, row.get(column));
                    }
                    
                    // Set WHERE clause values
                    for (Object value : whereValues) {
                        preparedStatement.setObject(i++, value);
                    }
        
                    preparedStatement.addBatch();
                }
        
                int[] result = preparedStatement.executeBatch();
                rowsAffected = getNumberOfRowsUpdated(result);
                System.out.println("Total Rows " + rowsAffected + " updated in the table " + tableName);
                
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
            return rowsAffected;
        }
        
    /**
     * Calculates the total number of rows affected based on the batch execution
     * result.
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
        return totalRowsAffected;
    }
    
    // TODO: loadProperties make a singleton
    private Boolean getDeleteFlag(String ilTableName) throws SQLException {
        Map<String, Object> result = getLoadProperties(conn, connectionId, ilTableName);
        Boolean deleteFlag = (Boolean) result.get("delete_flag");
        // deleteFlag = null;
        if (deleteFlag == null) {
            deleteFlag = false;
        }
        System.out.println("Delete Flag: " + deleteFlag);
        return deleteFlag;
    }

    private String getWriteMode(String ilTableName) throws SQLException {
        Map<String, Object> result = getLoadProperties(conn, connectionId, ilTableName);
        String writeMode = (String) result.get(WRITE_MODE);
        // writeMode = null;
        if (writeMode == null) {
            writeMode = "overwrite";
        }
        System.out.println("Write Mode: " + writeMode);
        return writeMode;
    }

    
        /**
         * Retrieves the Setting_Value for given Settings_Category
         *
         * @param Settings_Category  Settings_Category eg Dateformat
         * @param schemaName   The Schema_Name value.
         * @return The Setting_Value, or null if not found.
         */
        private  String getSettingValue(Connection connection, String connectionId, String schemaName, String settingsCategory) {
            String settingValue = null;
        
            String query = "SELECT `ELT_IL_Settings_Info`.`Setting_Value` " +
                           "FROM `ELT_IL_Settings_Info` " +
                           "WHERE Connection_Id = ? AND Schema_Name = ? " +
                           "AND Settings_Category = ? AND Active_Flag = 1";
        
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, connectionId);
                pstmt.setString(2, schemaName);
                pstmt.setString(3, settingsCategory);
        
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        settingValue = rs.getString("Setting_Value");
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        
            return settingValue;
        }
    public class DWDBScriptsGenerator {

        private static final String CONDITIONAL_DATE = "Conditional_Date";
        private static final String CONDITIONAL_LIMIT = "Conditional_Limit";
        private static final String TRAILING_MONTHS = "Trailing_Months";
        private static final String HISTORICAL2 = "historical";
        private static final String INCREMENTAL_DATE = "Incremental_Date";
        private static final String INCREMENTAL_ID = "Incremental_Id";
        private static final String CONDITIONAL_FILTER = "Conditional_Filter";

        String tgtDbName;
        // Constructor
        public DWDBScriptsGenerator() {
            init();
        }
    
        private void init() {
            JSONObject jsonDbDetails = new JSONObject(dbDetails);
            tgtDbName = jsonDbDetails.getString("datadb_schema");
        }
        // Method to generate the DB script
        public Status generateDBScript() {
            Status status = Status.FAILURE;
            try {
                // Select
                status = generateSelectQuery();

                // Create_Dim

                status = generateDBCreateScriptDimension();

                // Create_Trans

                status = generateDBCreateScriptTransaction();

                // stg_keys

                status = getStageKeysUpdate();

                // Alert_Script

                status = generateDBAlterScript();

                // Ends Here
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return status;
        }
            // DB Script - Select Query part
        public Status generateSelectQuery() {
            try {

                // Select code
                List<Map<String, Object>> tableSchemaList =  getTableSchemasFromMapInfo(conn, selectTables, connectionId, querySchemaCondition);
                System.out.println("Size of table schemas: " + tableSchemaList.size());

                // Step 2 - iteration (ELT_Select_Query)
                for (Map<String, Object> schema : tableSchemaList) {
                    String connectionIdResult = (String) schema.get("Connection_Id");
                    String tableSchema = (String) schema.get("Table_Schema"); //  This is used
                    String ilTableName = (String) schema.get("IL_Table_Name");
                    System.out.println("Connection_Id: " + connectionIdResult +
                            ", Table_Schema: " + tableSchema +
                            ", IL_Table_Name: " + ilTableName);

                    // TODO to test only
                    // ilTableName = "AbcAnalysis_Spark3";
                    // connectionId = "114";
                    // tableSchema = "dbo";
                    // schemaName = "dbo";
                    System.out.println("Connection_Id: " + connectionIdResult +
                            ", Table_Schema: " + tableSchema +
                            ", IL_Table_Name: " + ilTableName);
                     Boolean deleteFlag = getDeleteFlag(ilTableName);

                     Map<String, Map<String, Object>>  masterData = getMasterSourceMappingInfoData(conn, ilTableName, connectionId, querySchemaCondition);
                    //  System.out.println("Size of master data: " + masterData.size());
                     //System.out.println("master data: " + masterData);
                     // Called inside core transaction. TODO remvoe above

                     List<Map<String, Object>> finalResults = doCoreTransformations(conn, connectionId, tableSchema, ilTableName,
                             querySchemaCondition);

                     String tableName = "ELT_Select_Script";
                     if (finalResults != null && !finalResults.isEmpty()) {
                         // TODO delete query to be added
                         deleteSelectScripts(conn, ilTableName, connectionId, querySchemaCondition);
                         saveDataIntoDB(conn, tableName, finalResults);
                     }

                    // USed inside above funciton
                    //  Map<String, String> data = getSymbolValueforConnectionId(conn, connectionId);
                    //  System.out.println("Size of Symbol value data: " + data.size());
                    //  System.out.println("data: " + data);
                     
                     // TODO have single copy of the below
                     Map<String, Object> loadProperties = getLoadProperties(conn, connectionId,  ilTableName);
                    //  System.out.println("loadProperties: " + loadProperties);
                     Map<String, Object> dateParam = getConditionalDateParam(ilTableName, loadProperties);
                    //  System.out.println("conditional date: " + dateParam);
                     Map<String, Object> limitParam = getConditionalLimitParam(ilTableName, loadProperties);
                    //  System.out.println("conditional Limit: " + limitParam);
                     Map<String, Object> monthParam = getConditionalMonthParam(ilTableName, loadProperties);
                    //  System.out.println("conditional Month: " + monthParam);
                     Map<String, Object> filterParam = getConditionalFilterParam(ilTableName, loadProperties);
                    //  System.out.println("conditional Filter: " + filterParam);
                     Map<String, Object> historyDateParam = getHistoricalDateParam(ilTableName, loadProperties);
                    //  System.out.println("historical date: " + historyDateParam);
                     Map<String, Object> incrementalParam = getIncrementalParam(ilTableName, loadProperties);
                    //  System.out.println("incremental name/type: " + incrementalParam);

                     Map<Integer, Map<String, Object>> dwSettings = getDWSettings(conn, Integer.valueOf(connectionId));
                    //  System.out.println("DW settings: " + dwSettings);

                     // row6 (lookup)
                     Map<String, String> joinedData = aggregateColumnNames(conn, ilTableName, Integer.valueOf(connectionId), querySchemaCondition);
                    //  System.out.println("Aggregated data: " + joinedData);

                     // Deletes(lookup)
                     String query = getCoreSourceMappingInfoQuery(querySchemaCondition);
                     Map<String, Map<String, Object>> res = processSourceMapping(conn, ilTableName, connectionId, query);
                    //  System.out.println("Ptocess Source mapping: " + res);
                    // ELT_Selective_Source_Metadata`
                     Map<String, Map<String, Object>> selectivesourcemeta = getSelectiveSourceMetadata(conn, connectionId, querySchemaCondition1); // Note querySchemaCondition1
                    //  System.out.println("Selective Source metadata size: " + selectivesourcemeta.size());
                    //  System.out.println("Selective Source metadata: " + selectivesourcemeta);
                     // ELT_Custom_Source_Metadata_Info
                     Map<String, Map<String, Object>> sourcemeta = getCustomSourceMetadata(conn);
                    //  System.out.println("Custom Source metadata size : " + sourcemeta.size());
                    //  System.out.println("Custom Source metadata : " + sourcemeta);

                }


            } catch (SQLException e) {
                e.printStackTrace();
            }

            return Status.SUCCESS;
        }

        // DB Script - Create Script transaction group
        private Status generateDBCreateScriptDimension() {

            try {
                String dimensionTransaction = "D";

                List<Map<String, Object>> createScripDimList = GetDBCreateScriptDataList(conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition);
                System.out.println("Create Dim Script size: " + createScripDimList.size());
                

                for (Map<String, Object> map : createScripDimList) {

                    String sourceTableName = (String) map.get("Source_Table_Name");
                    String ilTableName = (String) map.get("IL_Table_Name");
                    Boolean deleteFlag = getDeleteFlag(ilTableName);
                    Map<String, Map<String, Object>> aggregatedData = fetchILSourceMappingInfoDim(conn, ilTableName, sourceTableName, connectionId, dimensionTransaction, querySchemaCondition);
                    
                    List<Map<String, Object>> finalResults = transformDataDim(aggregatedData, deleteFlag);

                    String tableName = "ELT_Create_Script";
                    if (finalResults != null && !finalResults.isEmpty()) {
                        deleteCreateScripts(conn, ilTableName, connectionId, querySchemaCondition);
                        saveDataIntoDB(conn, tableName, finalResults);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return Status.SUCCESS;
        }

                
        // DB Script - Create Script transaction group
        private Status generateDBCreateScriptTransaction() {

            try {
                String dimensionTransaction = "T";

                List<Map<String, Object>> createScripTransList = GetDBCreateScriptDataList(conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition);
                System.out.println("Create Script size: " + createScripTransList.size());
                

                for (Map<String, Object> map : createScripTransList) {

                    String sourceTableName = (String) map.get("Source_Table_Name");
                    String ilTableName = (String) map.get("IL_Table_Name");
                    Boolean deleteFlag = getDeleteFlag(ilTableName);
                    Map<String, Map<String, Object>> aggregatedData = fetchILSourceMappingInfo(conn, ilTableName, sourceTableName, connectionId, dimensionTransaction, querySchemaCondition);
                    
                    List<Map<String, Object>> finalResults = transformData(aggregatedData);

                    String tableName = "ELT_Create_Script";
                    if (finalResults != null && !finalResults.isEmpty()) {
                        deleteCreateScripts(conn, ilTableName, connectionId, querySchemaCondition);
                        saveDataIntoDB(conn, tableName, finalResults);
                    }
                    // final String fileName = getConfigFileName(ilTableName, STG_CONFIG_FILE_STRING);

                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return Status.SUCCESS;
        }

        private List<Map<String, Object>> transformDataDim(Map<String, Map<String, Object>> aggregatedData, Boolean deleteFlag) {

            List<Map<String, Object>> finalResults = new ArrayList<>(); // Return object 

            for (Map.Entry<String, Map<String, Object>> outerEntry : aggregatedData.entrySet()) {
                String key = outerEntry.getKey();
                Map<String, Object> data = outerEntry.getValue();

                String columnNamesValue = (String) data.get("Column_Names");
                String ILColumnName = (String) data.get("IL_Column_Name");
                String PKConstarints = (String) data.get("PK_Constarints");
                String columnNames1 = (String) data.get("Column_Names1");
                String connectionId = (String) data.get("Connection_Id");  
                String tableSchema = (String) data.get("Table_Schema");   
                String dimensionTransaction = (String) data.get("Dimension_Transaction");   
                String ilTableName = (String) data.get("IL_Table_Name");  
                String aIPKColumns = (String) data.get("AI_PK_Columns");
                String deleteAIPK = (String) data.get("Delete_AI_PK");
                String iLaIpKColumns = (String) data.get("IL_AI_PK_Columns");
                String PKColsInt = (String) data.get("PK_Cols_int");
                String skColumn = (String) data.get("SK_Column");
                String sourceTableName = (String) data.get("Source_Table_Name");

                String createTable = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "_Stg` (";

                String constantColumns = "\n" + (ILColumnName.toLowerCase().contains(",company,") || 
                        ILColumnName.toLowerCase().contains(",company_id,") || 
                        ILColumnName.toLowerCase().contains("company_id,") || 
                        ILColumnName.toLowerCase().contains(",company_id") ?
                        "`DataSource_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," :
                        "`Company_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," + "\n" +
                        "`DataSource_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',");

                String pkColsInt = PKColsInt;

                String pkCols = (PKConstarints.toLowerCase().contains(",`company_pk`,") || 
                        PKConstarints.toLowerCase().contains("company_id_pk")) ?
                        pkColsInt + "`DataSource_Id`" : pkColsInt + "`DataSource_Id`,`Company_Id`";

                String aiPkColumns = aIPKColumns;

                String columnNames = columnNamesValue + ",";

                String constantFields = "\n" + "`Source_Hash_Value` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL , " + "\n" +
                        "`Mismatch_Flag` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL , " + "\n" +
                        "`Added_Date` datetime DEFAULT NULL, " + "\n" +
                        "`Added_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                        "`Updated_Date` datetime DEFAULT NULL, " + "\n" +
                        "`Updated_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, ";

                String ending = "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;";

                String pkColumns = "\nPrimary Key (" + pkCols + ",`" +skColumn + "`), " + "\n" + 
                        "KEY `Key_" +skColumn + "`(`" +skColumn + "`)";

                String stgScript = createTable + " " + constantColumns + " " + aiPkColumns + " " + columnNames + " " + 
                        constantFields + " " + pkColumns + " " + ending;

                String ilCreate = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "` (";

                String deleteCreate = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "_Deletes` (";

                String ilConstantColumns = constantColumns;

                String ilAiPkColumns = iLaIpKColumns;

                String ilPkColumns = "\nPrimary Key (" + pkCols + ",`" +skColumn + "` )";

                String ilScript = ilCreate + " " + ilConstantColumns + " " + ilAiPkColumns + " " + columnNames + " " +
                        constantFields + " " + ilPkColumns + " " + ending;

                String deleteScript = deleteCreate + " " + ilConstantColumns + " " + ilAiPkColumns + " " + columnNames + " " +
                        constantFields + " " + ilPkColumns + " " + ending;

                String pkColsCleaned = pkCols.substring(0, pkCols.length() - 1); // Removing trailing comma

                // Output
                Map<String, Object> finalData = new HashMap<>();

                finalData.put("Connection_Id", connectionId) ;
                finalData.put("Table_Schema", tableSchema) ;
                finalData.put("IL_Table_Name", ilTableName) ;
                finalData.put("Dimension_Transaction", dimensionTransaction) ;
                finalData.put("Stg_Script", stgScript);
                finalData.put("IL_Script", ilScript);                 
                finalData.put("Delete_Script", deleteFlag.equals(false)? "" : deleteScript) ;
                                    
                finalResults.add(finalData);

            }
            System.out.println(finalResults.toString());
            return finalResults;

        }

        private List<Map<String, Object>> transformData(Map<String, Map<String, Object>> aggregatedData) {

            List<Map<String, Object>> finalResults = new ArrayList<>(); // Return object 

            for (Map.Entry<String, Map<String, Object>> outerEntry : aggregatedData.entrySet()) {
                String key = outerEntry.getKey();
                Map<String, Object> data = outerEntry.getValue();

                String columnNamesValue = (String) data.get("Column_Names");
                String ILColumnName = (String) data.get("IL_Column_Name");
                String PKConstarints = (String) data.get("PK_Constarints");
                String columnNames1 = (String) data.get("Column_Names1");
                String connectionId = (String) data.get("Connection_Id");  
                String tableSchema = (String) data.get("Table_Schema");   
                String dimensionTransaction = (String) data.get("Dimension_Transaction");   
                String ilTableName = (String) data.get("IL_Table_Name");  
                String aIPKColumns = (String) data.get("AI_PK_Columns");
                String deleteAIPK = (String) data.get("Delete_AI_PK");
                String iLaIpKColumns = (String) data.get("IL_AI_PK_Columns");
                String PKColsInt = (String) data.get("PK_Cols_int");
                String skColumn = (String) data.get("SK_Column");
                String sourceTableName = (String) data.get("Source_Table_Name");

                String createTableScript = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "` ( ";
                String deleteCreateTableScript = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "_Deletes` ( ";

                String constantColumns = "\n" + 
                    (ILColumnName.toLowerCase().contains(",company,") || 
                    ILColumnName.toLowerCase().contains(",company_id,") || 
                    ILColumnName.toLowerCase().contains("company_id,") || 
                    ILColumnName.toLowerCase().contains(",company_id") 
                    ? "`DataSource_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," 
                    : "`Company_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," + "\n" + 
                    "`DataSource_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',");

                String pkColumnsInt = PKColsInt;

                String pkColumns = (PKConstarints.toLowerCase().contains(",company_pk,") || 
                                    PKConstarints.toLowerCase().contains("company_id_pk")) 
                                    ? pkColumnsInt + "DataSource_Id" 
                                    : pkColumnsInt + "DataSource_Id,Company_Id";

                String aiPkColumns = aIPKColumns;
                String columnNames = columnNamesValue + ",";
                String constantFields = "\n" + "`Source_Hash_Value` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                                        "`Added_Date` datetime DEFAULT NULL, " + "\n" +
                                        "`Added_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                                        "`Updated_Date` datetime DEFAULT NULL, " + "\n" +
                                        "`Updated_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, ";
                String tableEnding = "\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;";

                String pkColumnsScript = "\n" + " Primary Key (" + pkColumns + ",`" + skColumn + "` ), " + "\n" +
                                         " KEY `KEY_" + skColumn + "`(`" + skColumn + "`)";

                String ilScript = createTableScript + " " + constantColumns + " " + aiPkColumns + " " + 
                                columnNames + " " + constantFields + " " + pkColumnsScript + " " + tableEnding;

                String createStageTableScript = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "_Stg` ( ";

                String constantColumnsStage = "\n" + 
                    (ILColumnName.toLowerCase().contains(",company,") || 
                    ILColumnName.toLowerCase().contains(",company_id,") || 
                    ILColumnName.toLowerCase().contains("company_id,") || 
                    ILColumnName.toLowerCase().contains(",company_id") 
                    ? "`DataSource_Id` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," 
                    : "`Company_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," + "\n" + 
                    "`DataSource_Id` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',");

                String columnNamesStage = columnNames1 + ",";
                String constantFieldsStage = "\n" + "`Source_Hash_Value` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                                            "`Added_Date` datetime DEFAULT NULL, " + "\n" +
                                            "`Added_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                                            "`Updated_Date` datetime DEFAULT NULL, " + "\n" +
                                            "`Updated_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL ";
                String tableEndingStage = "\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;";

                String stageScript = createStageTableScript + " " + constantColumnsStage + " " + columnNamesStage + " " + 
                                    constantFieldsStage + " " + tableEndingStage;

                String stageKeysScript = "CREATE TABLE IF NOT EXISTS `" + ilTableName + "_Stg_Keys` (" + "\n" + 
                                        "`PKValue` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," + "\n" +
                                        "`HashValue` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL," + "\n" +
                                        "`Added_Date` datetime DEFAULT NULL," + "\n" +
                                        "`Added_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL, " + "\n" +
                                        "`Updated_Date` datetime DEFAULT NULL, " + "\n" +
                                        "`Updated_User` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL " + "\n" +
                                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci";

                String deleteScript = deleteCreateTableScript + " " + constantColumns + " " + deleteAIPK + " " + 
                                    columnNames + " " + constantFields + " " + pkColumnsScript + " " + tableEnding;

                // Output
                Map<String, Object> finalData = new HashMap<>();

                finalData.put("Connection_Id", connectionId) ;
                finalData.put("Table_Schema", tableSchema) ;
                finalData.put("IL_Table_Name", ilTableName) ;
                finalData.put("Dimension_Transaction", dimensionTransaction) ;
                finalData.put("Stg_Script", stageScript);
                finalData.put("IL_Script", ilScript);
                finalData.put("Stg_Keys_Script", stageKeysScript) ;
                finalData.put("Delete_Script", deleteScript) ;
                                    
                finalResults.add(finalData); //

            }
            return finalResults;

        }

        // DB Create Script Dim main flow
        public Map<String, Map<String, Object>> fetchILSourceMappingInfoDim(
            Connection connection,
            String ilTableName,
            String sourceTableNameParent,
            String connectionId,
            String dimensionTransaction,
            String querySchemaCond) {

        String query = "SELECT " +
                "Connection_Id, " +
                "Table_Schema, " +
                "IL_Table_Name, " +
                "IL_Column_Name, " +
                "CASE WHEN IL_Data_Type = 'Bit(1)' THEN 'tinyint(4)' ELSE IL_Data_Type END AS IL_Data_Type, " +
                "Constraints, " +
                "Source_Table_Name, " +
                "PK_Constraint, " +
                "PK_Column_Name, " +
                "FK_Constraint, " +
                "FK_Column_Name, " +
                "Dimension_Transaction " +
                "FROM ELT_IL_Source_Mapping_Info_Saved " +
                "WHERE Dimension_Transaction = ? " +
                "AND IL_Column_Name != 'DataSource_Id' " +
                "AND Constraints != 'SK' " +
                "AND IL_Table_Name = ? " +
                "AND Connection_Id = ? " +
                querySchemaCond;

        Map<String, String> defaultValueMap = getdataTypeDefaultValueConversionMap(connection);
        // Output
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, ilTableName);
            preparedStatement.setString(3, connectionId);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String connectionIdValue = resultSet.getString("Connection_Id");
                    String tableSchema = resultSet.getString("Table_Schema");
                    String ilTableNameValue = resultSet.getString("IL_Table_Name"); // Key
                    String ilColumnName = resultSet.getString("IL_Column_Name");
                    String ilDataType = resultSet.getString("IL_Data_Type");
                    String constraints = resultSet.getString("Constraints");
                    String sourceTableName = resultSet.getString("Source_Table_Name");
                    String pkConstraint = resultSet.getString("PK_Constraint");
                    String pkColumnName = resultSet.getString("PK_Column_Name");
                    String fkConstraint = resultSet.getString("FK_Constraint");
                    String fkColumnName = resultSet.getString("FK_Column_Name");
                    String dimensionTransactionValue = resultSet.getString("Dimension_Transaction");

                    //System.out.println("Connection_Id: " + connectionIdValue + ", Table_Schema: " + tableSchema);
                    String defaultValue = defaultValueMap.getOrDefault(ilDataType, null); // Left Outer Join
                    //System.out.println("ilDataType: " + ilDataType + ", defaultValue: " + defaultValue);

                    // Map to handle IL_Data_Type and Constraints to its equivalent data type
                    // StringBuilder columnNames = new StringBuilder();
                    // StringBuilder columnNames1 = new StringBuilder();
                    // StringBuilder constantColumns = new StringBuilder();
                    // StringBuilder aiPkColumns = new StringBuilder();
                    // StringBuilder deleteAiPk = new StringBuilder();
                    // StringBuilder ilAiPkColumns = new StringBuilder();
                    // =================================================
                    // new code

                    // column names
                    String columnNames = "\n`" + ilColumnName + "` ";
                    String dataType = ilDataType;
                    
                    if (dataType.toLowerCase().startsWith("text") && constraints.toLowerCase().equals("pk")) {
                        columnNames += "varchar(150)";
                    } else {
                        columnNames += ilDataType;
                    }
                    
                    if (dataType.toLowerCase().startsWith("varchar")) {
                        columnNames += " COLLATE utf8_unicode_ci";
                    }

                    if (constraints.toLowerCase().equals("pk")) {
                        if (dataType.toLowerCase().contains("varchar")) {
                            columnNames += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("int")) {
                            columnNames += " NOT NULL DEFAULT '0'";
                        } else if (dataType.toLowerCase().contains("text")) {
                            columnNames += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("decimal")) {
                            columnNames += " NOT NULL DEFAULT '0.0'";
                        } else if (dataType.toLowerCase().contains("float")) {
                            columnNames += " NOT NULL DEFAULT '0.0'";
                        } else if (dataType.toLowerCase().contains("boolean")) {
                            columnNames += " NOT NULL DEFAULT '0'";
                        } else if (dataType.toLowerCase().contains("bit")) {
                            columnNames += " NOT NULL DEFAULT '0'";
                        } else if (dataType.toLowerCase().contains("char")) {
                            columnNames += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("date")) {
                            columnNames += " NOT NULL DEFAULT '" + defaultValue + "'";
                        } else {
                            columnNames += " DEFAULT NULL";
                        }
                    } else {
                        columnNames += " DEFAULT NULL";
                    }
                    // constant columns
                    String constantColumns = "\n`" + ilColumnName + "` ";
                    if (dataType.startsWith("text")) {
                        constantColumns += "varchar(150)";
                    } else {
                        constantColumns += dataType;
                    }

                    if (dataType.toLowerCase().startsWith("varchar")) {
                        constantColumns += " COLLATE utf8_unicode_ci";
                    }

                    if (constraints.toLowerCase().equals("pk")) {
                        if (dataType.toLowerCase().startsWith("varchar")) {
                            constantColumns += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("int")) {
                            constantColumns += " NOT NULL DEFAULT '0'";
                        } else if (dataType.toLowerCase().contains("text")) {
                            constantColumns += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("decimal")) {
                            constantColumns += " NOT NULL DEFAULT ''";
                        } else if (dataType.toLowerCase().contains("date")) {
                            constantColumns += " NOT NULL DEFAULT '1970-01-01'";
                        } else {
                            constantColumns += " DEFAULT NULL";
                        }
                    } else {
                        constantColumns += " DEFAULT NULL";
                    }
                    // AI PK columns
                    String aiPkColumns = "\n`" + sourceTableNameParent + "_Key` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'The Surrogate Key + The PK', ";
                    // IL AI PK columns
                    String ilAiPkColumns = "\n`" + sourceTableNameParent + "_Key` bigint(32) NOT NULL, ";
                    // PK columns
                    String pkColsInt1 = constraints.toLowerCase().equals("pk") ? "`" + ilColumnName + "`," : "";
                    String pkColsInt = pkColsInt1 == null ? "" : pkColsInt1;

                    String pkCols = (ilColumnName.toLowerCase().equals("company") || ilColumnName.toLowerCase().equals("company_id")) ?
                            ((constraints.toLowerCase().equals("pk")) ? pkColsInt + "DataSource_Id" : pkColsInt + "DataSource_Id,Company_Id") :
                            pkColsInt + "DataSource_Id,Company_Id";
                    // SK Column
                    String skColumn = sourceTableNameParent + "_Key";
                    //  PK constraint
                    String pkConstraints = ilColumnName + "_" + constraints;

                    String columnNamesOut = columnNames.toString();
                    // String columnNames1Out = columnNames1.toString();
                    // String constantColumnsOut = constantColumns.toString();
                    String aIPKColumnsOut = aiPkColumns.toString();
                    // String deleteAIPKOut = deleteAiPk.toString();
                    String iLaIpKColumnsOut = ilAiPkColumns.toString();
                    String PKColsIntOut = pkColsInt;
                    // String PKColsOut = pkCols;
                    String skColumnOut = skColumn;
                    String PKConstarintsOut = pkConstraints;


                    // Create or update the entry in the resultMap
                    resultMap.computeIfAbsent(ilTableNameValue, key -> new HashMap<>());
                    Map<String, Object> tableData = resultMap.get(ilTableNameValue);

                    // Aggregate Columns (concatenate) and Symbol (assign last value)
                    String existingColumnNames = (String) tableData.getOrDefault("Column_Names", "");
                    tableData.put("Column_Names", existingColumnNames.isEmpty() ? columnNamesOut : existingColumnNames + ", " + columnNamesOut);
                    String existingILColumnName = (String) tableData.getOrDefault("IL_Column_Name", "");
                    tableData.put("IL_Column_Name", existingILColumnName.isEmpty() ? ilColumnName : existingILColumnName + ", " + ilColumnName);
                    String existingPKConstarints = (String) tableData.getOrDefault("PK_Constarints", "");
                    tableData.put("PK_Constarints", existingPKConstarints.isEmpty() ? PKConstarintsOut : existingPKConstarints + ", " + PKConstarintsOut);
                    // String existingColumnNames1 = (String) tableData.getOrDefault("Column_Names1", "");
                    // tableData.put("Column_Names1", existingColumnNames1.isEmpty() ? columnNames1Out : existingColumnNames1 + ", " + columnNames1Out);

                    
                    // Last
                    tableData.put("Connection_Id", connectionIdValue);   
                    tableData.put("Table_Schema", tableSchema);   
                    tableData.put("Dimension_Transaction", dimensionTransactionValue);   

                    tableData.put("IL_Table_Name", ilTableNameValue);   
                    tableData.put("AI_PK_Columns", aIPKColumnsOut);
                    // tableData.put("Delete_AI_PK", deleteAIPKOut);
                    tableData.put("IL_AI_PK_Columns", iLaIpKColumnsOut);
                    tableData.put("PK_Cols_int", PKColsIntOut);
                    tableData.put("SK_Column", skColumnOut);
                    tableData.put("Source_Table_Name", sourceTableNameParent); // Note the input value

                    resultMap.put(ilTableNameValue, tableData);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

        // DB Create Script Trans main flow
        public Map<String, Map<String, Object>> fetchILSourceMappingInfo(
                Connection connection,
                String ilTableName,
                String sourceTableNameParent,
                String connectionId,
                String dimensionTransaction,
                String querySchemaCond) {

            String query = "SELECT " +
                    "Connection_Id, " +
                    "Table_Schema, " +
                    "IL_Table_Name, " +
                    "IL_Column_Name, " +
                    "CASE WHEN IL_Data_Type = 'Bit(1)' THEN 'tinyint(4)' ELSE IL_Data_Type END AS IL_Data_Type, " +
                    "Constraints, " +
                    "Source_Table_Name, " +
                    "PK_Constraint, " +
                    "PK_Column_Name, " +
                    "FK_Constraint, " +
                    "FK_Column_Name, " +
                    "Dimension_Transaction " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = ? " +
                    "AND IL_Column_Name != 'DataSource_Id' " +
                    "AND Constraints != 'SK' " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " +
                    querySchemaCond;

            Map<String, String> defaultValueMap = getdataTypeDefaultValueConversionMap(connection);
            // Output
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, dimensionTransaction);
                preparedStatement.setString(2, ilTableName);
                preparedStatement.setString(3, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String connectionIdValue = resultSet.getString("Connection_Id");
                        String tableSchema = resultSet.getString("Table_Schema");
                        String ilTableNameValue = resultSet.getString("IL_Table_Name"); // Key
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String ilDataType = resultSet.getString("IL_Data_Type");
                        String constraints = resultSet.getString("Constraints");
                        String sourceTableName = resultSet.getString("Source_Table_Name");
                        String pkConstraint = resultSet.getString("PK_Constraint");
                        String pkColumnName = resultSet.getString("PK_Column_Name");
                        String fkConstraint = resultSet.getString("FK_Constraint");
                        String fkColumnName = resultSet.getString("FK_Column_Name");
                        String dimensionTransactionValue = resultSet.getString("Dimension_Transaction");

                        System.out.println("Connection_Id: " + connectionIdValue + ", Table_Schema: " + tableSchema);
                        String defaultValue = defaultValueMap.getOrDefault(ilDataType, null); // Left Outer Join
                        System.out.println("ilDataType: " + ilDataType + ", defaultValue: " + defaultValue);

                        // Map to handle IL_Data_Type and Constraints to its equivalent data type
                        StringBuilder columnNames = new StringBuilder();
                        StringBuilder columnNames1 = new StringBuilder();
                        StringBuilder constantColumns = new StringBuilder();
                        StringBuilder aiPkColumns = new StringBuilder();
                        StringBuilder deleteAiPk = new StringBuilder();
                        StringBuilder ilAiPkColumns = new StringBuilder();
                        
                        // Build column names
                        columnNames.append("\n`").append(ilColumnName).append("` ");
                        if (ilDataType.toLowerCase().startsWith("text") && constraints.equalsIgnoreCase("pk")) {
                            columnNames.append("varchar(150) ");
                        } else {
                            columnNames.append(ilDataType).append(" ");
                        }
                        
                        if (ilDataType.toLowerCase().startsWith("varchar")) {
                            columnNames.append("COLLATE utf8_unicode_ci");
                        }
    
                        if (constraints.equalsIgnoreCase("pk")) {
                            columnNames.append(" NOT NULL DEFAULT ");
                            if (ilDataType.toLowerCase().contains("varchar")) {
                                columnNames.append("''");
                            } else if (ilDataType.toLowerCase().contains("int")) {
                                columnNames.append("'0'");
                            } else if (ilDataType.toLowerCase().contains("text")) {
                                columnNames.append("''");
                            } else if (ilDataType.toLowerCase().contains("decimal")) {
                                columnNames.append("'0.0'");
                            } else if (ilDataType.toLowerCase().contains("float")) {
                                columnNames.append("'0.0'");
                            } else if (ilDataType.toLowerCase().contains("boolean")) {
                                columnNames.append("0");
                            } else if (ilDataType.toLowerCase().contains("bit")) {
                                columnNames.append("0");
                            } else if (ilDataType.toLowerCase().startsWith("char")) {
                                columnNames.append("''");
                            } else if (ilDataType.toLowerCase().contains("date")) {
                                columnNames.append("'").append(defaultValue).append("'");
                            } else {
                                columnNames.append(" DEFAULT NULL");
                            }
                        } else {
                            columnNames.append(" DEFAULT NULL");
                        }

                        // Build column names1
                        columnNames1.append("\n`").append(ilColumnName).append("` ").append(ilDataType);
                        if (ilDataType.toLowerCase().startsWith("varchar")) {
                            columnNames1.append(" COLLATE utf8_unicode_ci DEFAULT NULL");
                        } else {
                            columnNames1.append(" DEFAULT NULL");
                        }

                        // Build constant columns
                        constantColumns.append(constraints.equalsIgnoreCase("company") || constraints.equalsIgnoreCase("company_id") ? 
                                "`DataSource_Id` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT ''," : 
                                "`Company_Id` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',\n`DataSource_Id` varchar(100) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',");

                        // Build AI PK columns
                        aiPkColumns.append("\n`").append(sourceTableNameParent).append("_Key` bigint(32) NOT NULL AUTO_INCREMENT COMMENT 'The Surrogate Key + The PK', ");

                        // Build delete AI PK
                        deleteAiPk.append("\n`").append(sourceTableNameParent).append("_Key` bigint(32) NOT NULL DEFAULT 0, ");

                        // Build IL AI PK columns
                        ilAiPkColumns.append("\n`").append(sourceTableNameParent).append("_Key` bigint(32) NOT NULL, ");

                        // Build PK columns for int1
                        // Seems It's a conditional aggreagation, Statement looks 
                        //String pkColsInt = constraints.equalsIgnoreCase("pk") ? (pkColsInt == null ? "" : pkColsInt) + "`" + ilColumnName + "`," : pkColsInt;
                        String pkColsInt = constraints.equalsIgnoreCase("pk") ? "`" + ilColumnName + "`," : "";

                        String pkCols = constraints.equalsIgnoreCase("company") || constraints.equalsIgnoreCase("company_id") ?
                                (constraints.equalsIgnoreCase("pk") ? pkColsInt + "DataSource_Id" : pkColsInt + "DataSource_Id,Company_Id") :
                                (pkColsInt + "DataSource_Id,Company_Id");

                        // Build SK Column
                        String skColumn = sourceTableNameParent + "_Key";
                        // Build PK constraint
                        String pkConstraints = ilColumnName + "_" + constraints;

                        // Setting the context variables
                        String columnNamesOut = columnNames.toString();
                        String columnNames1Out = columnNames1.toString();
                        String constantColumnsOut = constantColumns.toString();
                        String aIPKColumnsOut = aiPkColumns.toString();
                        String deleteAIPKOut = deleteAiPk.toString();
                        String iLaIpKColumnsOut = ilAiPkColumns.toString();
                        String PKColsIntOut = pkColsInt;
                        String PKColsOut = pkCols;
                        String skColumnOut = skColumn;
                        String PKConstarintsOut = pkConstraints;


                        // Create or update the entry in the resultMap
                        resultMap.computeIfAbsent(ilTableNameValue, key -> new HashMap<>());
                        Map<String, Object> tableData = resultMap.get(ilTableNameValue);

                        // Aggregate Columns (concatenate) and Symbol (assign last value)
                        String existingColumnNames = (String) tableData.getOrDefault("Column_Names", "");
                        tableData.put("Column_Names", existingColumnNames.isEmpty() ? columnNamesOut : existingColumnNames + ", " + columnNamesOut);
                        String existingILColumnName = (String) tableData.getOrDefault("IL_Column_Name", "");
                        tableData.put("IL_Column_Name", existingILColumnName.isEmpty() ? ilColumnName : existingILColumnName + ", " + ilColumnName);
                        String existingPKConstarints = (String) tableData.getOrDefault("PK_Constarints", "");
                        tableData.put("PK_Constarints", existingPKConstarints.isEmpty() ? PKConstarintsOut : existingPKConstarints + ", " + PKConstarintsOut);
                        String existingColumnNames1 = (String) tableData.getOrDefault("Column_Names1", "");
                        tableData.put("Column_Names1", existingColumnNames1.isEmpty() ? columnNames1Out : existingColumnNames1 + ", " + columnNames1Out);

                        
                        // Last
                        tableData.put("Connection_Id", connectionIdValue);   
                        tableData.put("Table_Schema", tableSchema);   
                        tableData.put("Dimension_Transaction", dimensionTransactionValue);   

                        tableData.put("IL_Table_Name", ilTableNameValue);   
                        tableData.put("AI_PK_Columns", aIPKColumnsOut);
                        tableData.put("Delete_AI_PK", deleteAIPKOut);
                        tableData.put("IL_AI_PK_Columns", iLaIpKColumnsOut); // Note the input value
                        tableData.put("PK_Cols_int", PKColsIntOut);
                        tableData.put("SK_Column", skColumnOut);
                        tableData.put("Source_Table_Name", sourceTableNameParent); // Note the input value

                        resultMap.put(ilTableNameValue, tableData);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        // DB craete script row3, 
        private Map<String, String> getdataTypeDefaultValueConversionMap(Connection connection) {
            String query = "SELECT DISTINCT " +
                           "IL_Data_Type, " +
                           "Deafult_Value " +
                           "FROM ELT_Datatype_Conversions";
        
            Map<String, String> datatypeConversionsMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
        
                while (resultSet.next()) {
                    String ilDataType = resultSet.getString("IL_Data_Type");
                    String defaultValue = resultSet.getString("Deafult_Value");
                    datatypeConversionsMap.put(ilDataType, defaultValue);
                }
        
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return datatypeConversionsMap;
        }

        private List<Map<String, Object>> doCoreTransformations(Connection connection, String connectionId, String tableSchema, String ilTableName, String querySchemaCond) throws SQLException {

            System.out.println("");
            System.out.println("");
            List<Map<String, Object>> finalResults = new ArrayList<>(); // Return object 

            Map<String, Map<String, Object>>  masterData = getMasterSourceMappingInfoData(conn, ilTableName, connectionId, querySchemaCondition);
            System.out.println("Size of master data: " + masterData.size());
            System.out.println("Size of master data is going to be 1 or less only as it is aggregated over ilTableName itself." );
            if (masterData.size() == 0) {
                return finalResults;
            }

            Map<String, Object> masterDataForConId = masterData.get(ilTableName);
            String row1Symbol = (String) masterDataForConId.get("Symbol");
            String row1Columns = (String) masterDataForConId.get("Columns");
            String row1SourceTableName = (String) masterDataForConId.get("Source_Table_Name");
            // String row1SourceTableName = (String) masterDataForConId.get("Source_Table_Name");

            // System.out.println("symbol: " + row1Symbol);
            // System.out.println("Columns: " + row1Columns);
            // System.out.println("Columns: " + row1Columns);


            // Initialize variables with null checks and ternary operators where applicable.
            //delete_flag
            Boolean deleteFlag = getDeleteFlag(ilTableName);
            // String deleteFlag = (String) globalMap.get("delete_flag");

            Map<String, Object> loadProperties = getLoadProperties(conn, connectionId,  ilTableName);
            // System.out.println("loadProperties: " + loadProperties);
            // row2
            // DW Settings - server name
            Map<Integer, Map<String, Object>> row6DWSettings = getDWSettings(conn, Integer.valueOf(connectionId));
            // System.out.println("row6DWSettings value: " + row6DWSettings);
            String row2Name = (String) row6DWSettings.get(Integer.parseInt(connectionId)).get("name");
            // System.out.println("name: " + row2Name);

            // row6 (lookup)
            Map<String, String> joinedData = aggregateColumnNames(conn, ilTableName, Integer.valueOf(connectionId), querySchemaCond);
            String row6LookupKey = connectionId + "-" + tableSchema + "-" + ilTableName;
            String row6ILColumnName = joinedData.get(row6LookupKey);
            // System.out.println("Aggregated data: " + joinedData);
            // System.out.println("row6LookupKey: " + row6LookupKey);
            // System.out.println("row6ILColumnName: " + row6ILColumnName);

            // server
            String server = row2Name == null ? ""
                    : row2Name.equals("SQL Server") ? " Order by  " + (row6ILColumnName == null ? " Company_Id " : row6ILColumnName) : "";
            // System.out.println("server: " + server);

            // conditional_limit
            Map<String, Object> limitParam = getConditionalLimitParam(ilTableName, loadProperties);
            // System.out.println("conditional Limit: " + limitParam);
            String limitParamSettingsCategory = (String) limitParam.get("Settings_Category");
            String limitParamSettingsValue = (String) limitParam.get("Setting_Value");
            // Var_conditionallimit
            String conditionalLimitSettingCategory = limitParamSettingsCategory == null ? "" : limitParamSettingsCategory;
            // Var_limit
            String limitValue = limitParamSettingsCategory == null ? ""
                    : limitParamSettingsCategory.equals("Conditional_Limit") ? limitParamSettingsValue : "";
            // System.out.println("conditional Limit: " + conditionalLimitSettingCategory + ", " + limitValue);

            // dates (lookup)
            Map<String, Object> datesDWSettings = row6DWSettings.get(Integer.parseInt(connectionId));
            // System.out.println("datesDWSettings: " + datesDWSettings);
            String datesIncrementalDate = (String) datesDWSettings.get("Incremental_Date");
            String datesConditionalDate = (String) datesDWSettings.get("Conditional_Date");

            // conditional (lookup)
            Map<String, Object> conditional = getConditionalDateParam(ilTableName, loadProperties);
            // System.out.println("conditional date: " + conditional);
            // String dateParamConditionalLimit = (String) conditional.get("Settings_Category");
            // Var_ConditionalLimitsett
            String conditionalLimitSetting = (String) datesDWSettings.get("Conditional_Limit") == null ? "limit" : (String) datesDWSettings.get("Conditional_Limit");
            // System.out.println("conditionalLimitSetting: " + conditionalLimitSetting);
            // Var_conditional
            String conditionalSetting = conditional.get("Settings_Category") == null ? "" : (String) conditional.get("Settings_Category");
            // System.out.println("conditionalSetting: " + conditionalSetting);

            // incremental1
            Map<String, Object> incremental1 = getIncrementalParam(ilTableName, loadProperties);
            // System.out.println("incremental name/type: " + incremental1);
            String incremental1SettingCategory = (String) incremental1.get("Settings_Category");
            String incremental1SettingValue = (String) incremental1.get("Setting_Value");
            // Var_Incremental
            String incrementalSetting = incremental1SettingCategory == null ? "" : incremental1SettingCategory;

            // Conditional_filter1 (Lookup), Conditional_filter
            Map<String, Object> conditionalFilter1 = getConditionalFilterParam(ilTableName, loadProperties);
            // System.out.println("conditional Filter: " + conditionalFilter1);
            // Conditionalfilter
            String conditionalFilter = conditionalFilter1.get("Settings_Category") == null ? "" : (String) conditionalFilter1.get("Settings_Category");
            // System.out.println("conditionalFilter: " + conditionalFilter);
            //IL_Column_Name
            String ilColumnName = row1Symbol == null ? " {Schema_Company} as Company_Id ," :
                row1Symbol.equals("\"") ? " {Schema_Company} as \"Company_Id\" ," : " {Schema_Company} as Company_Id ,";
            // System.out.println("ilColumnName: " + ilColumnName);

            // Conditional_limit, Limit
            Map<String, Object> conditionalLimit = getConditionalLimitParam(ilTableName, loadProperties);
            // System.out.println("conditional Limit: " + limitParam);

            String statement = conditionalLimitSetting.toLowerCase().contains("limit") ? " Select "
                    : conditionalLimitSetting.toLowerCase().contains("top")
                            && conditionalLimit.get("Settings_Category") == null ? " Select  "
                                    : conditionalLimitSetting.toLowerCase().contains(
                                            "top") && conditionalLimit.get("Settings_Category").equals("Conditional_Limit") ? " Select TOP " + limitValue : " Select ";
            // System.out.println("statement: " + statement);

            // Deletes(lookup)
            String query = getCoreSourceMappingInfoQuery(querySchemaCondition);
            Map<String, Map<String, Object>> deletesAggregateData = processSourceMapping(conn, ilTableName, connectionId, query);
            // System.out.println("Ptocess Source mapping: " + deletesAggregateData);
            Map<String, Object> deletes = deletesAggregateData.get(ilTableName);

            String columns = row1Columns;
            String deleteColumns = (String) deletes.get("Columns");

            String ending = " from " + (row1Symbol == null ? ("[" + row1SourceTableName + "]") :
                row1Symbol.equals("[") ? ("[" + schemaName + "].[" + row1SourceTableName + "]") :
                row1Symbol.equals("`") ? ("`" + schemaName + "`.`" + row1SourceTableName + "`") :
                row1Symbol.equals("\"") ? ("\"" + schemaName + "\".\"" + row1SourceTableName + "\"") :
                ("[" + schemaName + "].[" + row1SourceTableName + "]"));
            // System.out.println("deleteColumns: " + deleteColumns);
            // System.out.println("ending: " + ending);

            String script = statement + ilColumnName + columns + ending;
            String deleteScript = statement + ilColumnName + deleteColumns + ending + " " + server;

            String incrementalDate = incremental1SettingCategory == null && datesIncrementalDate == null ? "" :
                incremental1SettingCategory == null ? "" :
                incremental1SettingCategory.equals("Incremental_Date") && datesIncrementalDate == null ? "{DateColumn}>={date}" :
                incremental1SettingCategory.equals("Incremental_Date") ? datesIncrementalDate : "";

            String finalIncrementalDate = incrementalDate == null ? "" : incrementalDate.replace("DateColumnName", incremental1SettingValue);

            String incrementalId = incremental1SettingCategory == null ? "" :
                incremental1SettingCategory.equals("Incremental_Id") ? incremental1SettingValue + ">={ColumnId}" : "";

            String incrementalFinal = incremental1SettingCategory == null ? "" :
                incremental1SettingCategory.equals("Incremental_Date") ? finalIncrementalDate :
                incremental1SettingCategory.equals("Incremental_Id") ? incrementalId : "";

            String conditionalDate = conditionalSetting == null && datesConditionalDate == null ? "" :
                conditionalSetting.equals("Conditional_Date") && datesConditionalDate == null ? "{DateColumn}>=DATEADD(m,-{Months1}, GetDate())" :
                conditionalSetting.equals("Conditional_Date") ? datesConditionalDate : "";

            String finalConditionalDate = conditionalDate == null ? "" : conditionalDate.replace("DateColumnName", (String) conditional.get("Setting_Value"));

            // Months1, Months
            Map<String, Object> months1 = getConditionalMonthParam(ilTableName, loadProperties);
            // System.out.println("months1: " + months1);


            String finalConditionalMonth = finalConditionalDate == null ? "" : finalConditionalDate.replace("Months", (String) months1.get("Setting_Value"));
            // System.out.println("finalConditionalMonth: " + finalConditionalMonth);

            String conditionalFilterValue = conditionalFilter == null ? ""
                    : conditionalFilter.equals("Conditional_Filter") ? conditionalFilter : "";

            // Round 2
            // final_conditions_limit
            String finalConditionsLimit = conditionalSetting == null && conditionalLimitSettingCategory == null && conditionalFilter == null ? "" :
                conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.equals("Conditional_Filter") ?
                    " Where " + finalConditionalMonth + " and " + conditionalFilterValue + " limit " + limitValue :
                conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.isEmpty() ?
                    " Where " + finalConditionalMonth + " limit " + limitValue :
                conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.equals("Conditional_Filter") ?
                    " Where " + finalConditionalMonth + " and " + conditionalFilterValue :
                conditionalSetting.isEmpty() && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.equals("Conditional_Filter") ?
                    " Where " + conditionalFilterValue + " limit " + limitValue :
                conditionalSetting.isEmpty() && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.equals("Conditional_Filter") ?
                    " Where " + conditionalFilterValue :
                conditionalSetting.isEmpty() && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.isEmpty() ?
                    " limit " + limitValue :
                conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.isEmpty() ?
                    " Where " + finalConditionalMonth : " ";
                
                // System.out.println("finalConditionsLimit: " + finalConditionsLimit);

                // final_conditions_top
                // Initialize the final_conditions_top variable with appropriate conditions.
                String finalConditionsTop = (conditionalSetting == null && conditionalLimitSettingCategory == null && conditionalFilter == null) ? "" :
                (conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + finalConditionalMonth + " and " + conditionalFilterValue :
                (conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.isEmpty()) ?
                    " Where " + finalConditionalMonth :
                (conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + finalConditionalMonth + " and " + conditionalFilterValue :
                (conditionalSetting.isEmpty() && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue :
                (conditionalSetting.isEmpty() && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue :
                (conditionalSetting.isEmpty() && conditionalLimitSettingCategory.equals("Conditional_Limit") && conditionalFilter.isEmpty()) ?
                    " " :
                (conditionalSetting.equals("Conditional_Date") && conditionalLimitSettingCategory.isEmpty() && conditionalFilter.isEmpty()) ?
                    " Where " + finalConditionalMonth : " ";
                // System.out.println("finalConditionsTop: " + finalConditionsTop);

                // final_Incremental_limit
                // Initialize the finalIncrementalLimit variable with appropriate conditions.
                String finalIncrementalLimit = (incrementalSetting == null && conditionalLimitSettingCategory == null && conditionalFilter == null) ? "" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) && 
                conditionalLimitSettingCategory.equals("Conditional_Limit") && 
                conditionalFilter.equals("Conditional_Filter")) ?
                    "/* Where " + incrementalFinal + " and " + conditionalFilterValue + " limit " + limitValue + " */" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) && 
                conditionalLimitSettingCategory.equals("Conditional_Limit") && 
                conditionalFilter.isEmpty()) ?
                    "/* Where " + incrementalFinal + " limit " + limitValue + " */" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) && 
                conditionalLimitSettingCategory.isEmpty() && 
                conditionalFilter.equals("Conditional_Filter")) ?
                    "/* Where " + incrementalFinal + " and " + conditionalFilterValue + " */" :
                (incrementalSetting.isEmpty() && 
                conditionalLimitSettingCategory.equals("Conditional_Limit") && 
                conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue + " limit " + limitValue :
                (incrementalSetting.isEmpty() && 
                conditionalLimitSettingCategory.isEmpty() && 
                conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue :
                (incrementalSetting.isEmpty() && 
                conditionalLimitSettingCategory.equals("Conditional_Limit") && 
                conditionalFilter.isEmpty()) ?
                    " limit " + limitValue :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) && 
                conditionalLimitSettingCategory.isEmpty() && 
                conditionalFilter.isEmpty()) ?
                    "/* Where " + incrementalFinal + " */" : "";
                // System.out.println("finalIncrementalLimit: " + finalIncrementalLimit);

                // final_Incremental_top
                String finalIncrementalTop = (incrementalSetting == null && conditionalLimitSettingCategory == null && conditionalFilter == null) ? "" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) &&
                conditionalLimitSettingCategory.equals("Conditional_Limit") &&
                conditionalFilter.equals("Conditional_Filter")) ?
                    "/* Where " + incrementalFinal + " and " + conditionalFilterValue + " */" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) &&
                conditionalLimitSettingCategory.equals("Conditional_Limit") &&
                conditionalFilter.isEmpty()) ?
                    "/* Where " + incrementalFinal + " */" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) &&
                conditionalLimitSettingCategory.isEmpty() &&
                conditionalFilter.equals("Conditional_Filter")) ?
                    "/* Where " + incrementalFinal + " and " + conditionalFilterValue + " */" :
                (incrementalSetting.isEmpty() &&
                conditionalLimitSettingCategory.equals("Conditional_Limit") &&
                conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue :
                (incrementalSetting.isEmpty() &&
                conditionalLimitSettingCategory.isEmpty() &&
                conditionalFilter.equals("Conditional_Filter")) ?
                    " Where " + conditionalFilterValue :
                (incrementalSetting.isEmpty() &&
                conditionalLimitSettingCategory.equals("Conditional_Limit") &&
                conditionalFilter.isEmpty()) ?
                    " l" :
                ((incrementalSetting.equals("Incremental_Id") || incrementalSetting.equals("Incremental_Date")) &&
                conditionalLimitSettingCategory.isEmpty() &&
                conditionalFilter.isEmpty()) ?
                    "/* Where " + incrementalFinal + " */" : "";

                // System.out.println("finalIncrementalTop: " + finalIncrementalTop);

                // History_Date (lookup)
                Map<String, Object> historyDate = getHistoricalDateParam(ilTableName, loadProperties);
                // System.out.println("historical date: " + historyDate);
                String historyDateSettingsCategory = (String) historyDate.get("Settings_Category");
                String historyDateSettingValue = (String) historyDate.get("Setting_Value");

                String varHist = historyDateSettingsCategory == null || historyDateSettingsCategory.isEmpty() ? "" : historyDateSettingsCategory;

                String historicalCond = varHist.isEmpty() && conditionalFilterValue.isEmpty() ? "" :
                    conditionalFilterValue.isEmpty() && varHist.equals("historical") ?
                        "where " + historyDateSettingValue + ">={fromdate} and " + historyDateSettingValue + "<={todate} " :
                    conditionalFilterValue.equals("Conditional_Filter") && varHist.equals("historical") ?
                        "where " + historyDateSettingValue + ">={fromdate} and " + historyDateSettingValue + "<={todate} and " + conditionalFilterValue : "";
                
                String condition = (String) conditional.get("Setting_Value") == null || (String) months1.get("Setting_Value") == null ? "" :
                    " Where " + (String) conditional.get("Setting_Value") + " >=DATEADD(m,-" + (String) months1.get("Setting_Value") + ", GetDate())";

                    // System.out.println("historicalCond: " + historicalCond);
                    // System.out.println("condition: " + condition);


                    // ELT_Selective_Source_Metadata
                    Map<String, Map<String, Object>> selectiveSourceMetadata = getSelectiveSourceMetadata(conn, connectionId, querySchemaCondition1); // Note Query Condiiton1
                    System.out.println("Selective Source metadata size: " + selectiveSourceMetadata.size());
                    String selectiveLookupKey = connectionId + "-" + tableSchema + "-" + row1SourceTableName;

                    Map<String, Object> selectiveData = selectiveSourceMetadata.get(selectiveLookupKey);
                    Boolean IsWebService = (Boolean) selectiveData.get("IsWebService"); // TODO Boolean
                    String selectiveCustomType = (String) selectiveData.get("Custom_Type");
                    Integer fileId = (Integer) selectiveData.get("File_Id"); // TODO int

                     // ELT_Custom_Source_Metadata_Info
                     Map<String, Map<String, Object>> sourcemeta = getCustomSourceMetadata(conn);
                     System.out.println("Custom Source metadata size : " + sourcemeta.size());
                     String customLookupKey = connectionId + "-" + tableSchema + "-" + row1SourceTableName;
                     Map<String, Object> customData = selectiveSourceMetadata.get(customLookupKey);
                     String customCustomType = (String) customData.get("Custom_Type");
                     String customConnectionType = (String) customData.get("connection_type");

                    //  System.out.println("Custom Source metadata : " + sourcemeta);

                    String finalCondition;

                    if (incrementalSetting == null && conditionalSetting == null) {
                        finalCondition = "";
                    } else if ((incrementalSetting.equals("Incremental_Date") || incrementalSetting.equals("Incremental_Id")) &&
                               conditionalLimitSetting.toLowerCase().contains("limit")) {
                        finalCondition = finalIncrementalLimit;
                    } else if ((incrementalSetting.equals("Incremental_Date") || incrementalSetting.equals("Incremental_Id")) &&
                               conditionalLimitSetting.toLowerCase().contains("top")) {
                        finalCondition = finalIncrementalTop;
                    } else if (conditionalSetting.equals("Conditional_Date") &&
                               conditionalLimitSetting.toLowerCase().contains("limit")) {
                        finalCondition = finalConditionsLimit;
                    } else if (conditionalSetting.equals("Conditional_Date") &&
                               conditionalLimitSetting.toLowerCase().contains("top")) {
                        finalCondition = finalConditionsTop;
                    } else if (conditionalLimitSetting.toLowerCase().contains("top")) {
                        finalCondition = finalConditionsTop;
                    } else if (conditionalLimitSetting.toLowerCase().contains("limit")) {
                        finalCondition = finalConditionsLimit;
                    } else {
                        finalCondition = "";
                    }
                    
                    String finalScript = script + "\n" + finalCondition + " " + server;

                    String finalizedScript = IsWebService ? " "
                            : selectiveCustomType.equals("FileasSource") ? ""
                                    : selectiveCustomType.equals("OneDrive") ? "" : finalScript;
                    
                    String customType = selectiveCustomType.equals("Common") && IsWebService ? "API"
                            : selectiveCustomType.equals("Common") && !IsWebService ? "DB" : selectiveCustomType;


                    String maxSelectScript = incremental1SettingCategory == null ? "" :
                        incremental1SettingCategory.equals("Incremental_Date") ?
                            "Select max(" + incremental1SettingValue + ") as Incremental_Date from " +
                            (row1Symbol == null ? "[" + row1SourceTableName + "]" :
                            row1Symbol.equals("[") ? schemaName + ".[" + row1SourceTableName + "]" :
                            row1Symbol.equals("`") ? schemaName + ".`" + row1SourceTableName + "`" :
                            schemaName + ".[" + row1SourceTableName + "]") + "\n" + finalCondition :
                        incremental1SettingCategory.equals("Incremental_Id") ?
                            "Select max(" + incremental1SettingValue + ") as Incremental_Id from " +
                            (row1Symbol == null ? "[" + row1SourceTableName + "]" :
                            row1Symbol.equals("[") ? schemaName + ".[" + row1SourceTableName + "]" :
                            row1Symbol.equals("`") ? schemaName + ".`" + row1SourceTableName + "`" :
                            schemaName + ".[" + row1SourceTableName + "]") + finalCondition : "";
                    
                        // System.out.println("finalScript: " + finalScript);
                        // System.out.println("finalizedScript: " + finalizedScript);
                        // System.out.println("customType: " + customType);
                        // System.out.println("maxSelectScript: " + maxSelectScript);

                    String historicalScript = historyDateSettingsCategory == null ? "" :
                        script + "\n" + historicalCond + " " + server;
                    
                    String finalHistoricalQuery = IsWebService ? " " :
                    selectiveCustomType.equals("FileasSource") ? "" : historicalScript;
                    
                    String customTypeInfo = customCustomType == null ?
                        (selectiveCustomType == null ? "" :
                        (selectiveCustomType.toLowerCase().equals("common") &&
                        fileId != 0 ? "metadata_file" :
                        selectiveCustomType.toLowerCase().equals("common") &&
                        fileId == 0 ? "dbSource" :
                        selectiveCustomType)) : customCustomType;
                    
                    String sourceType = customCustomType == null ?
                        (selectiveCustomType == null ? "" :
                        (selectiveCustomType.toLowerCase().equals("common") &&
                        fileId != 0 && IsWebService ? "web_service" :
                        selectiveCustomType.toLowerCase().equals("common") &&
                        fileId == 0 && !IsWebService ? "dbSource" :
                        selectiveCustomType.toLowerCase().equals("metadata_file") &&
                        fileId != 0 && IsWebService ? "web_service" :
                        selectiveCustomType.toLowerCase().equals("metadata_file") &&
                        fileId == 0 && !IsWebService ? "dbSource" :
                        selectiveCustomType.toLowerCase().equals("metadata_file") ? "shared_folder" :
                        selectiveCustomType.toLowerCase().equals("dbSource") ? "dbSource" :
                        selectiveCustomType.toLowerCase().equals("web_service") ? "web_service" :
                        selectiveCustomType)) : customConnectionType;

                        // System.out.println("historicalScript: " + historicalScript);
                        // System.out.println("finalHistoricalQuery: " + finalHistoricalQuery);

                        // System.out.println("customTypeInfo: " + customTypeInfo);

                        // System.out.println("sourceType: " + sourceType);

                        // Data to be saved
                        Map<String, Object> finalData = new HashMap<>();
                        finalData.put("Connection_Id", connectionId);
                        finalData.put("Table_Schema", tableSchema);
                        finalData.put("IL_Table_Name", ilTableName);
                        finalData.put("Source_Table_Name", row1SourceTableName);
                        finalData.put("Select_Script", finalizedScript);
                        finalData.put("IsWebService", IsWebService);
                        finalData.put("Source_Type", sourceType);
                        finalData.put("Custom_Type", customType);
                        finalData.put("Max_Query", maxSelectScript);
                        finalData.put("Historical_Query", finalHistoricalQuery);
                        finalData.put("Delete_Script", deleteFlag ? deleteScript : "" );

                        System.out.println("######################################################");
                        System.out.println(finalData);
                        System.out.println("######################################################");
            System.out.println("");
            System.out.println("");
            finalResults.add(finalData); //

            return finalResults;

        }
         /**
         * Executes a SQL query to fetch master data from `ELT_IL_Source_Mapping_Info_Saved`and returns the result as a list of maps.
         */
        // TODO main flow master data
        public Map<String, Map<String, Object>> getMasterSourceMappingInfoData(Connection connection, String ilTableName,
                String connectionId, String querySchemaCond) throws SQLException {
            
            // TODO It should be single value at max
            Map<String, String> symbolValues = getSymbolValueforConnectionId(conn, connectionId);
            // System.out.println("Size of Symbol value data:  " + symbolValues.size());
            String query = "SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, IL_Data_Type, " +
                    "Constraints, Source_Table_Name, Source_Column_Name, PK_Constraint, PK_Column_Name, " +
                    "FK_Constraint, FK_Column_Name " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                    "WHERE (Constant_Insert_Column IS NULL OR Constant_Insert_Column != 'Y') " +
                    "AND Constraints NOT IN ('FK', 'SK') " +
                    "AND IL_Column_Name != 'Company_Id' " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;

            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String ilTableNameRes = resultSet.getString("IL_Table_Name");
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String sourceColumnName = resultSet.getString("Source_Column_Name");

                        // Transformation
                        //Map<String, Object> settingsMap = new HashMap<>(dwSettings.getOrDefault(connectionId, new HashMap<>()));
                        String symbol = (String) symbolValues.get("symbol");
                        symbol = (symbol == null) ? "[" : symbol;
                        
                        String column = "";
                        if (sourceColumnName == null || sourceColumnName.isEmpty() ||
                                sourceColumnName.equals("NULL")) {
                            column = " '' as " + ilColumnName;
                        } else if (symbol.equals("[")) {
                            column = "[" + sourceColumnName + "] as [" + ilColumnName + "]";
                        } else if (symbol.equals("`")) {
                            column = "`" + sourceColumnName + "` as `" + ilColumnName + "`";
                        } else if (symbol.equals("\"")) {
                            column = "\"" + sourceColumnName + "\" as \"" + ilColumnName + "\"";
                        } else {
                            column = "[" + sourceColumnName + "] as [" + ilColumnName + "]";
                        }

                        // Create or update the entry in the resultMap
                        resultMap.computeIfAbsent(ilTableNameRes, key -> new HashMap<>());
                        Map<String, Object> tableData = resultMap.get(ilTableNameRes);
    
                        // Aggregate Columns (concatenate) and Symbol (assign last value)
                        String existingColumns = (String) tableData.getOrDefault("Columns", "");
                        tableData.put("Columns", existingColumns.isEmpty() ? column : existingColumns + ", " + column);
                        String existingILColumnName = (String) tableData.getOrDefault("IL_Column_Name", "");
                        tableData.put("IL_Column_Name", existingILColumnName.isEmpty() ? ilColumnName : existingILColumnName + ", " + ilColumnName);
                        // Last
                        tableData.put("Connection_Id", resultSet.getString("Connection_Id"));
                        tableData.put("Table_Schema", resultSet.getString("Table_Schema"));
                        tableData.put("Source_Table_Name", resultSet.getString("Source_Table_Name"));
                        tableData.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));                      
                        tableData.put("Symbol", symbol);
    
                        resultMap.put(ilTableNameRes, tableData);
                    }
                }
            }

            return resultMap;
        }

        public Map<String, String> getSymbolValueforConnectionId(Connection connection, String connectionId)
                throws SQLException {
            String query = "SELECT a.connection_id, symbol " +
                    "FROM minidwcs_database_connections a " +
                    "INNER JOIN minidwcm_database_connectors b ON a.DB_type_id = b.id " +
                    "INNER JOIN minidwcm_database_types c ON c.id = b.connector_id " +
                    "INNER JOIN ELT_Connectors_Settings_Info d ON d.Connectors_Id = c.id " +
                    "WHERE a.connection_id = ?";

            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String connectionIdKey = resultSet.getString("connection_id");
                        String symbolValue = resultSet.getString("symbol");
                        resultMap.put(connectionIdKey, symbolValue);
                    }
                }
            }
            return resultMap;
        }
        // ELT_Selective_Source_Metadata`
        public Map<String, Map<String, Object>> getSelectiveSourceMetadata(Connection connection, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT DISTINCT " +
                    "Connection_Id, Schema_Name, Table_Name, IsWebService, Custom_Type, File_Id " +
                    "FROM ELT_Selective_Source_Metadata " +
                    "WHERE Connection_Id = ? " + querySchemaCond;
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
    
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String connectionIdRes = resultSet.getString("Connection_Id");
                        String schemaName = resultSet.getString("Schema_Name");
                        String tableName = resultSet.getString("Table_Name");
    
                        String key = connectionIdRes + "-" + schemaName + "-" + tableName;
    
                        resultMap.computeIfAbsent(key, k -> new HashMap<>());
                        Map<String, Object> tableData = resultMap.get(key);
    
                        tableData.put("Connection_Id", connectionIdRes);
                        tableData.put("Schema_Name", schemaName);
                        tableData.put("Table_Name", tableName);
                        tableData.put("IsWebService", resultSet.getBoolean("IsWebService"));
                        tableData.put("Custom_Type", resultSet.getString("Custom_Type"));
                        tableData.put("File_Id", resultSet.getInt("File_Id"));
    
                        resultMap.put(key, tableData);
                    }
                }
            }
            return resultMap;
        }
        // ELT_Custom_Source_Metadata_Info
        public Map<String, Map<String, Object>> getCustomSourceMetadata(Connection connection) throws SQLException {
            String query = "SELECT " +
                    "Connection_Id, Schema_Name, Table_Name, Source_Table_Name, Custom_Type, connection_type " +
                    "FROM ELT_Custom_Source_Metadata_Info";
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
    
                while (resultSet.next()) {
                    String connectionId = resultSet.getString("Connection_Id");
                    String schemaName = resultSet.getString("Schema_Name");
                    String tableName = resultSet.getString("Table_Name");
                    String sourceTableName = resultSet.getString("Source_Table_Name");
    
                    String key = connectionId + "-" + schemaName + "-" + tableName;
    
                    resultMap.computeIfAbsent(key, k -> new HashMap<>());
                    Map<String, Object> tableData = resultMap.get(key);
    
                    tableData.put("Connection_Id", connectionId);
                    tableData.put("Schema_Name", schemaName);
                    tableData.put("Table_Name", tableName);
                    tableData.put("Source_Table_Name", sourceTableName);
                    tableData.put("Custom_Type", resultSet.getString("Custom_Type"));
                    tableData.put("connection_type", resultSet.getString("connection_type"));
    
                    resultMap.put(key, tableData);
                }
            }
    
            return resultMap;
        }
        /**
         * Aggregate IL_COLUMN_NAME, Columns for given il_table_name
         */

        // TOOD -  // Deletes(lookup) not used in first/main - ELT_IL_Source_Mapping_Info_Saved
        public Map<String, Map<String, Object>> processSourceMapping(Connection connection, String ilTableName, String connectionId, String query) throws SQLException {
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            Map<Integer, Map<String, Object>> dwSettings = getDWSettings(connection, Integer.parseInt(connectionId));

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
    
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String ilTableNameRes = resultSet.getString("IL_Table_Name");
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String sourceColumnName = resultSet.getString("Source_Column_Name");

                        // Transformation
                        Map<String, Object> settingsMap = new HashMap<>(dwSettings.getOrDefault(connectionId, new HashMap<>()));
                        String symbol = (String) settingsMap.get("symbol");
                        symbol = (symbol == null) ? "[" : symbol;
                        
                        String column = "";
                        if (sourceColumnName == null || sourceColumnName.isEmpty() ||
                                sourceColumnName.equals("NULL")) {
                            column = " '' as " + ilColumnName;
                        } else if (symbol.equals("[")) {
                            column = "[" + sourceColumnName + "] as [" + ilColumnName + "]";
                        } else if (symbol.equals("`")) {
                            column = "`" + sourceColumnName + "` as `" + ilColumnName + "`";
                        } else if (symbol.equals("\"")) {
                            column = "\"" + sourceColumnName + "\" as \"" + ilColumnName + "\"";
                        } else {
                            column = "[" + sourceColumnName + "] as [" + ilColumnName + "]";
                        }

                        // Create or update the entry in the resultMap
                        resultMap.computeIfAbsent(ilTableNameRes, key -> new HashMap<>());
                        Map<String, Object> tableData = resultMap.get(ilTableNameRes);
    
                        // Aggregate Columns (concatenate) and Symbol (assign last value)
                        String existingColumns = (String) tableData.getOrDefault("Columns", "");
                        tableData.put("Columns", existingColumns.isEmpty() ? column : existingColumns + ", " + column);
                        String existingILColumnName = (String) tableData.getOrDefault("IL_Column_Name", "");
                        tableData.put("IL_Column_Name", existingILColumnName.isEmpty() ? ilColumnName : existingILColumnName + ", " + ilColumnName);
                        // Last
                        tableData.put("Connection_Id", resultSet.getString("Connection_Id"));
                        tableData.put("Table_Schema", resultSet.getString("Table_Schema"));
                        tableData.put("Source_Table_Name", resultSet.getString("Source_Table_Name"));
                        tableData.put("Symbol", symbol);
    
                        resultMap.put(ilTableNameRes, tableData);
                    }
                }
            }
    
            return resultMap;
        }

        private String getCoreSourceMappingInfoQuery(String querySchemaCond) {
            String query = "SELECT " +
                    "Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, " +
                    "Source_Table_Name, Source_Column_Name, PK_Constraint, PK_Column_Name, FK_Constraint, FK_Column_Name " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE (Constant_Insert_Column IS NULL OR Constant_Insert_Column != 'Y') " +
                    "AND Constraints NOT IN ('FK', 'SK') " +
                    "AND Constraints = 'PK' " +
                    "AND IL_Column_Name != 'Company_Id' " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;
            return query;
        }

        /**
         * Aggregate IL_COLUMN_NAME for given coonetion id, table_schema, il_table_name
         */
        private Map<String, String> aggregateColumnNames(
                Connection connection,
                String ilTableName,
                int connectionId,
                String querySchemaCond) throws SQLException {
            String query = "SELECT " +
                    "`Connection_Id`, " +
                    "`Table_Schema`, " +
                    "`IL_Table_Name`, " +
                    "`IL_Column_Name` " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                    "WHERE Column_Type='Source' AND Constraints='PK' " +
                    "AND IL_Table_Name = ? AND Connection_Id = ? " + querySchemaCond;

            Map<String, String> joinedData = new HashMap<>();
            Map<Integer, Map<String, Object>> dwSettings = getDWSettings(connection, connectionId);

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setInt(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        int connId = resultSet.getInt("Connection_Id");

                        String tableSchema = resultSet.getString("Table_Schema");
                        String ilTableNameRes = resultSet.getString("IL_Table_Name");
                        Map<String, Object> rowMap = new HashMap<>();
                        rowMap.put("Table_Schema", tableSchema);
                        rowMap.put("IL_Table_Name", ilTableNameRes);

                        // if (dwSettings.containsKey(connId)) {
                        //     rowMap.putAll(dwSettings.get(connId));
                        // }
                        Map<String, Object> settingsMap = new HashMap<>(dwSettings.getOrDefault(connId, new HashMap<>()));
                        String symbol = (String) settingsMap.get("symbol");
                        symbol = (symbol == null) ? "[" : symbol;
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        if (symbol.equals("[")) {
                            ilColumnName = "[" + ilColumnName + "]";
                        } else if (symbol.equals("\"")) {
                            ilColumnName = "\"" + ilColumnName + "\"";
                        } else if (symbol.equals("`")) {
                            ilColumnName = "`" + ilColumnName + "`";
                        } else {
                            ilColumnName = "[" + ilColumnName + "]";
                        }

                        rowMap.put("IL_Column_Name", ilColumnName);
                        String keyLookup = connId + "-" + tableSchema + "-" + ilTableNameRes;
                        String existingColumnName = joinedData.getOrDefault(keyLookup, ""); // Default value

                        String aggregatedColumnName = existingColumnName.isEmpty()
                            ? ilColumnName : existingColumnName + ", " + ilColumnName;

                        joinedData.put(keyLookup, aggregatedColumnName);
                    }
                }
            }

            return joinedData;
        }

        /**
         * Retrieves conditional Date param from load properties.
         */
        private Map<String, Object> getConditionalDateParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(CONDITIONAL)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> conditionalMap = (Map<String, Object>) loadProperties.get(CONDITIONAL);
                
                // JSONObject conditional = new JSONObject(conditionalString);
                // Map<String, Object> conditionalMap = conditional.toMap();
                String conditionalDateParam = (String) conditionalMap.get(CONDITIONAL_DATE_PARAM);
                if (conditionalDateParam == null || conditionalDateParam.isEmpty()) {
                    settings.put("Setting_Value", null);
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                } else {
                    settings.put("Setting_Value", conditionalDateParam);
                    settings.put("Settings_Category", CONDITIONAL_DATE);
                    settings.put("Main_Value", ilTableName);
                }
            }
            return settings;
        }

        /**
         * Retrieves conditional Limit param from load properties.
         */
        private Map<String, Object> getConditionalLimitParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(LIMIT)) {
                String limit = (String) loadProperties.get(LIMIT);
                
                if (limit == null || limit.isEmpty()) {
                    settings.put("Setting_Value", null);
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                } else {
                    settings.put("Setting_Value", limit);
                    settings.put("Settings_Category", CONDITIONAL_LIMIT);
                    settings.put("Main_Value", ilTableName);
                }
            }
            return settings;
        }

        /**
         * Retrieves conditional Trailing Month param from load properties.
         */
        private Map<String, Object> getConditionalMonthParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(CONDITIONAL)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> conditionalMap = (Map<String, Object>) loadProperties.get(CONDITIONAL);
                
                // JSONObject conditional = new JSONObject(conditionalString);
                // Map<String, Object> conditionalMap = conditional.toMap();
                String conditionalMonthParam = (String) conditionalMap.get(CONDITIONAL_MONTH_PARAM);
                if (conditionalMonthParam == null || conditionalMonthParam.isEmpty()) {
                    settings.put("Setting_Value", null);
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                } else {
                    settings.put("Setting_Value", conditionalMonthParam);
                    settings.put("Settings_Category", TRAILING_MONTHS);
                    settings.put("Main_Value", ilTableName);
                }
            }
            return settings;
        }

        /**
         * Retrieves Historical Date param from load properties.
         */
        private Map<String, Object> getHistoricalDateParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(HISTORICAL)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> historicalMap = (Map<String, Object>) loadProperties.get(HISTORICAL);
                
                // JSONObject historical = new JSONObject(historicalString);
                // Map<String, Object> historicalMap = historical.toMap();
                String historyDateParam = (String) historicalMap.get(HISTORICAL_DATE_PARAM);
                if (historyDateParam == null || historyDateParam.isEmpty()) {
                    settings.put("Setting_Value", null);
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                } else {
                    settings.put("Setting_Value", historyDateParam);
                    settings.put("Settings_Category", HISTORICAL2);
                    settings.put("Main_Value", ilTableName);
                }
            }
            return settings;
        }
        /**
         * Retrieves incremental column, type param from load properties.
         */
        // TODO not in use
        private Map<String, Object> getIncrementalParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(INCREMENTAL)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> incrementalMap = (Map<String, Object>) loadProperties.get(INCREMENTAL);
                
                // JSONObject incremental = new JSONObject(incrementalString);
                // Map<String, Object> incrementalMap = incremental.toMap();
                String incrementalTypeParam = (String) incrementalMap.get(INCREMENTAL_REFRESH_COLUMN_TYPE);
                String incrementalNameParam = (String) incrementalMap.get(INCREMENTAL_REFRESH_COLUMN_NAME);

                if (incrementalNameParam == null || incrementalNameParam.isEmpty()) {
                    settings.put("Setting_Value", null);
                } else {
                    settings.put("Setting_Value", incrementalNameParam);
                }

                if (incrementalTypeParam == null || incrementalTypeParam.isEmpty()) {
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                 } else {
                    if (incrementalTypeParam.equalsIgnoreCase("date")) {
                        settings.put("Settings_Category", INCREMENTAL_DATE);
                    } else if (incrementalTypeParam.equalsIgnoreCase("id")) {
                        settings.put("Settings_Category", INCREMENTAL_ID);
                    } else {
                        settings.put("Settings_Category", null);
                    }
                     settings.put("Main_Value", ilTableName);
                 }
            }
            return settings;
        }
                /**
         * Retrieves conditional Limit param from load properties.
         */
        private Map<String, Object> getConditionalFilterParam(String ilTableName, Map<String, Object> loadProperties) {
            Map<String, Object> settings = new HashMap<>();
            if (loadProperties.containsKey(LIMIT)) {
                String customFilter = (String) loadProperties.get(CUSTOM_FILTER);
                
                if (customFilter == null || customFilter.isEmpty()) {
                    settings.put("Setting_Value", null);
                    settings.put("Settings_Category", null);
                    settings.put("Main_Value", null);
                } else {
                    settings.put("Setting_Value", customFilter);
                    settings.put("Settings_Category", CONDITIONAL_FILTER);
                    settings.put("Main_Value", ilTableName);
                }
            }
            return settings;
        }

        /**
         * Retrieves distinct table schemas from the mapping info table.
         *
         * @param selectiveTables a comma-separated list of table names to filter, formatted for the SQL IN clause
         * @param querySchemaCond additional schema condition to append to the query
         * @return a list of maps, where each map has column name-value pairs
         * @throws SQLException 
        */
        public List<Map<String, Object>> getTableSchemasFromMapInfo(Connection connection,
                String selectiveTables, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT DISTINCT " +
                    "`Connection_Id`, " +
                    "`Table_Schema`, " +
                    "`IL_Table_Name` " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved`" +
                    "WHERE `IL_Table_Name` IN (" + selectiveTables + ") " +
                    "AND `Connection_Id` = ? " + querySchemaCond;

            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", resultSet.getString("Connection_Id"));
                        row.put("Table_Schema", resultSet.getString("Table_Schema"));
                        row.put("IL_Table_Name", resultSet.getString("Table_Schema"));
                        results.add(row);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw e;
            }
            return results;
        }

        /**
         * get data load peroperties from the `Settings` field of the table
         * `ELT_IL_Load_Configs`, parses Settings value as JSON, and
         * returns a map of the "dataloadproperties" key.
         */
        public Map<String, Object> getLoadProperties(Connection connection, String connectionId, String ilTableName) throws SQLException {
            String query = "SELECT `Settings` " +
                           "FROM `ELT_IL_Load_Configs` " +
                           "WHERE `Connection_Id` = ? AND `IL_Table_Name` = ?";
    
            Map<String, Object> resultMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                preparedStatement.setString(2, ilTableName);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        String settingsJson = resultSet.getString("Settings");
                        // TODO test purpose
                        settingsJson = "{\"dataloadproperties\":{"
                                + "\"write_mode\":\"upsert\","
                                + "\"custom_filter\":\"sample_filter\","
                                + "\"conditional\":{"
                                + "\"conditional_date_param\":\"1 July 2025\","
                                + "\"trailing_months\":\"July\""
                                + "},"
                                + "\"limit\":\"sample_limit\","
                                + "\"historical\":{"
                                + "\"historical_date_param\":\"1 dec 2024\""
                                + "},"
                                + "\"incremental\":{"
                                + "\"incremental_refresh_column_type\":\"date\","
                                + "\"incremental_refresh_column_name\":\"incre_name\""
                                + "},"
                                + "\"delete_flag\":false"
                                + "}}";
                        // System.out.println(settingsJson);
                        // id, date and other for type

                        JSONObject settings = new JSONObject(settingsJson);
                        if (settings.has("dataloadproperties")) {
                            JSONObject dataloadProperties = settings.getJSONObject("dataloadproperties");
                            resultMap = dataloadProperties.toMap();
                        }
                    }
                }
            }
            return resultMap;
        }

        /**
         * Returns the DW settings for given connection_id
         */
        private Map<Integer, Map<String, Object>> getDWSettings(
                Connection connection, int connectionId) throws SQLException {

        String query = "SELECT a.connection_id, " +
                       "Conditional_Date, Incremental_Date, Conditional_Limit, symbol, c.name " +
                       "FROM minidwcs_database_connections a " +
                       "INNER JOIN minidwcm_database_connectors b ON a.DB_type_id = b.id " +
                       "INNER JOIN minidwcm_database_types c ON c.id = b.connector_id " +
                       "INNER JOIN ELT_Connectors_Settings_Info d ON d.Connectors_Id = c.id " +
                       "WHERE a.connection_id = ?";

        Map<Integer, Map<String, Object>> resultMap = new HashMap<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, connectionId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    int connectionIdKey = resultSet.getInt("connection_id");

                    Map<String, Object> rowMap = new HashMap<>();
                    rowMap.put("Conditional_Date", resultSet.getObject("Conditional_Date"));
                    rowMap.put("Incremental_Date", resultSet.getObject("Incremental_Date"));
                    rowMap.put("Conditional_Limit", resultSet.getObject("Conditional_Limit"));
                    rowMap.put("symbol", resultSet.getObject("symbol"));
                    rowMap.put("name", resultSet.getObject("name"));

                    resultMap.put(connectionIdKey, rowMap);
                }
            }
        }
        return resultMap;
    }

    // Select script delete 
    public void deleteSelectScripts(Connection connection, String ilTableName, String connectionId, String querySchemaCond) {
        String query = "DELETE FROM ELT_Select_Script WHERE IL_Table_Name = '" + ilTableName + "' "
                     + "AND Connection_Id = '" + connectionId + "' "
                     + querySchemaCond;
    
        try (Statement statement = connection.createStatement()) {
            int rowsDeleted = statement.executeUpdate(query);
            System.out.println(
                    rowsDeleted + " rows deleted from ELT_Select_Script where IL_Table_Name is " + ilTableName);
        } catch (SQLException e) {
            System.err.println("Error while deleting from ELT_Select_Script: " + e.getMessage());
        }
    }
    
    // Create script delete 
    public void deleteCreateScripts(Connection connection, String ilTableName, String connectionId, String querySchemaCond) {
        String query = "DELETE FROM ELT_Create_Script WHERE IL_Table_Name = '" + ilTableName + "' "
                        + "AND Connection_Id = '" + connectionId + "' "
                        + querySchemaCond;
    
        try (Statement statement = connection.createStatement()) {
            int rowsDeleted = statement.executeUpdate(query);
            System.out.println(
                    rowsDeleted + " rows deleted from ELT_Create_Script where IL_Table_Name is " + ilTableName);
        } catch (SQLException e) {
            System.err.println("Error while deleting from ELT_Create_Script: " + e.getMessage());
        }
    }

    /*
     * Create_Script_Transactioon Group - main left outer Join
     */
    public List<Map<String, Object>> GetDBCreateScriptDataList(Connection dbConnection, String selectiveTables, String dimensionTransaction,
            String connectionId, String querySchemaCond) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        // Main Query
        String mainQuery = "SELECT DISTINCT " +
                "  `Connection_Id`, " +
                "  `Table_Schema`, " +
                "  `IL_Table_Name`, " +
                "  `Dimension_Transaction` " +
                "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                "WHERE `IL_Table_Name` IN (" + selectiveTables + ") " +
                "  AND `Dimension_Transaction` = ? " +
                "  AND `Connection_Id` = ? " +
                querySchemaCond;

        // Lookup Query
        String lookupQuery = "SELECT DISTINCT " +
                "  `Connection_Id`, " +
                "  `Table_Schema`, " +
                "  `IL_Table_Name`, " +
                "  `Source_Table_Name`, " +
                "  `Dimension_Transaction` " +
                "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                "WHERE `IL_Table_Name` IN (" + selectiveTables + ") " +
                "  AND `Source_Table_Name` IS NOT NULL " +
                "  AND `Source_Table_Name` != '' " +
                "  AND `Dimension_Transaction` = ? " +
                "  AND `Connection_Id` = ? " +
                querySchemaCond;

        // SQL for LEFT OUTER JOIN comprising above two queries
        String joinQuery = "SELECT " +
                "  main.`Connection_Id`, " +
                "  main.`Table_Schema`, " +
                "  main.`IL_Table_Name`, " +
                "  main.`Dimension_Transaction`, " +
                "  lookup.`Source_Table_Name` " +
                "FROM (" + mainQuery + ") AS main " +
                "LEFT OUTER JOIN (" + lookupQuery + ") AS lookup " +
                "ON main.`Connection_Id` = lookup.`Connection_Id` " +
                "  AND main.`Table_Schema` = lookup.`Table_Schema` " +
                "  AND main.`IL_Table_Name` = lookup.`IL_Table_Name`";

        try (PreparedStatement ps = dbConnection.prepareStatement(joinQuery)) {
            ps.setString(1, dimensionTransaction);
            ps.setString(2, connectionId);
            ps.setString(3, dimensionTransaction);
            ps.setString(4, connectionId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Connection_Id", rs.getString("Connection_Id"));
                    row.put("Table_Schema", rs.getString("Table_Schema"));
                    row.put("IL_Table_Name", rs.getString("IL_Table_Name"));
                    row.put("Dimension_Transaction", rs.getString("Dimension_Transaction"));
                    row.put("Source_Table_Name", rs.getString("Source_Table_Name"));
                    resultList.add(row);
                }
            }
        }

        return resultList;
    }

    /* Stage Keys Update */
    public Status getStageKeysUpdate() {
        try {
            String dimensionTransaction = "T";
            Map<String, Map<String, String>> mainData = getStageKeysMainData(conn, connectionId, querySchemaCondition, dimensionTransaction);
            Map<String, Map<String, String>> lookupData = getStageKeysLookupData(conn, connectionId, querySchemaCondition, dimensionTransaction);
            Map<String, Map<String, String>> updatedLookupData = updateKeys(lookupData);

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);
            
            List<Map<String, Object>> finalResults = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> entry : mainData.entrySet()) {
                String outerKey = entry.getKey();        // Get the key from the outer map
                Map<String, String> innerMap = entry.getValue();   // Get the inner map

                String connectionIdValue = innerMap.get("Connection_Id");
                String tableSchemaValue = innerMap.get("Table_Schema");
                String ilTableNameValue = innerMap.get("IL_Table_Name");

                String ilColumnName = innerMap.get("IL_Column_Name");
                String ilDataType = innerMap.get("IL_Data_Type");
                String constraints = innerMap.get("Constraints");
                String tablename = innerMap.get("IL_Table_Name") + "_Stg";

                String key = connectionIdValue + "-" + tableSchemaValue + "-" + ilTableNameValue;
                Map<String, String> data = updatedLookupData.getOrDefault(key, new HashMap<>());

                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionIdValue); // Global
                resultRow.put("TABLE_SCHEMA", tableSchemaValue); // Global
                resultRow.put("Table_Name", tablename);
                
                resultRow.put("PK_Columns", innerMap.get("IL_Column_Name"));
                resultRow.put("Hash_Columns", data.get("ilColumnName"));

                resultRow.put("Tilt_PK_Columns", innerMap.get("Tilt_IL_Column_Name"));
                resultRow.put("Tilt_Hash_Columns", data.get("Tilt_IL_Column_Name"));

                resultRow.put("Date_Formats", innerMap.get("DateFormats"));
                resultRow.put("precisions", innerMap.get("precisions"));            
                resultRow.put("scales", innerMap.get("scales"));
                resultRow.put("Datatypes", innerMap.get("Java_Data_Type"));

                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);
                System.out.println("connectionIdValue:" + connectionIdValue);
            }

            String tableName = "stg_hashcolumn_pick";
            if (finalResults != null && !finalResults.isEmpty()) {
                deleteEntryFromStageHashColumnPick(conn, connectionId, querySchemaCondition);
                saveDataIntoDB(conn, tableName, finalResults);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Status.FAILURE;
        }

        return Status.SUCCESS;
    }
            /* Stage Keys Update */
    public Map<String, Map<String, String>> getStageKeysMainData(Connection conn, String connectionId, String querySchemaCondition, String dimensionTransaction) {
        String query = "SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, " +
                       "Constraints, Incremental_Column, IL_Data_Type, Source_Table_Name, " +
                       "Source_Data_Type, LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Data_Type " +
                       "FROM ELT_IL_Source_Mapping_Info_Saved " +
                       "WHERE Constraints = 'PK' AND Dimension_Transaction = ? " +
                       "AND Connection_Id = ? " + querySchemaCondition +
                       " ORDER BY IL_Table_Name, Constraints DESC";
        
        //String dimensionTransaction = "T";

        Map<String, Map<String, String>> lookupMapData = fetchMappingInfo(conn, dimensionTransaction, connectionId, querySchemaCondition);
        // Map<String, Map<String, String>> dataTypeConversions = getDatatypeConversions(connection);
        // printDatatypeConversionsGlobal(datatypeConversionsGlobal);

        String dateFormat = getSettingValue(conn, connectionId, schemaName, "Dateformat");
        if (dateFormat == null) {
            dateFormat = "yyyy-MM-dd";
        }
        Map<String, Map<String, String>> aggregateData = new HashMap<>();
        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, connectionId);
            
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String dataType = resultSet.getString("Data_Type");
                    String connectionIdValue = resultSet.getString("Connection_Id");
                    String tableSchema = resultSet.getString("Table_Schema");
                    String ilTableNameValue = resultSet.getString("IL_Table_Name"); // Key
                    String ilColumnName = resultSet.getString("IL_Column_Name");
                    String ilDataType = resultSet.getString("IL_Data_Type");
                    String constraints = resultSet.getString("Constraints");

                    String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue + "-" + ilColumnName;

                    // Left Outer join
                    Map<String, String> ValueMap = lookupMapData.getOrDefault(key, new HashMap<>());
                    String numericPrecisionVal = ValueMap.get("Precision_Val");
                    String numericScaleVal = ValueMap.get("Scale_Val");

                    // Left Outer join
                    Map<String, String> conversionDetails = datatypeConversionsGlobal.getOrDefault(dataType, new HashMap<>());
                    String javaDataType = conversionDetails.get("javaDataType");

                    String dateformat = dateFormat;

                    String ilDataTypeLower = ilDataType.toLowerCase();
                    boolean isDecimalOrFloatOrDouble = ilDataTypeLower.contains("decimal")
                            || ilDataTypeLower.contains("float") || ilDataTypeLower.contains("double");

                    String precisions = constraints.equals("PK")
                            ? (isDecimalOrFloatOrDouble ? numericPrecisionVal : "")
                            : null;

                    String scales = constraints.equals("PK") ? (isDecimalOrFloatOrDouble ? numericScaleVal : "")
                            : null;

                    String incrementalColumn = resultSet.getString("Incremental_Column");
                    String tiltILColumnName = "`" + ilColumnName + "`" ;
                    String dateformats = ilDataType.equals("datetime") ? dateformat : "";

                    Map<String, String> data = aggregateData.getOrDefault(ilTableNameValue, new HashMap<>());
                    // Last 
                    data.put("Connection_Id", connectionId);
                    data.put("Table_Schema", tableSchema);
                    data.put("IL_Table_Name", ilTableNameValue);
                    data.put("Incremental_Column", incrementalColumn);
                    // List
                    
                    if (data.get("IL_Column_Name") != null && !data.get("IL_Column_Name").isEmpty()) {
                        data.put("IL_Column_Name", data.get("IL_Column_Name") + "," + ilColumnName);
                    } else {
                        data.put("IL_Column_Name", ilColumnName);
                    }
                    
                    if (data.get("Tilt_IL_Column_Name") != null && !data.get("Tilt_IL_Column_Name").isEmpty()) {
                        data.put("Tilt_IL_Column_Name", data.get("Tilt_IL_Column_Name") + "," + tiltILColumnName);
                    } else {
                        data.put("Tilt_IL_Column_Name", tiltILColumnName);
                    }
                    
                    if (data.get("Constraints")  != null && !data.get("Constraints").isEmpty()) {
                        data.put("Constraints", data.get("Constraints") + "," + constraints);
                    } else {
                        data.put("Constraints", constraints);
                    }
                    
                    if (data.get("DateFormats")  != null && !data.get("DateFormats").isEmpty()) {
                        data.put("DateFormats", data.get("DateFormats") + "," + dateformats);
                    } else {
                        data.put("DateFormats", dateformats);
                    }
                    
                    if (data.get("precisions")  != null && !data.get("precisions").isEmpty()) {
                        data.put("precisions", data.get("precisions") + "," + precisions);
                    } else {
                        data.put("precisions", precisions);
                    }
                    
                    if (data.get("scales")  != null && !data.get("scales").isEmpty()) {
                        data.put("scales", data.get("scales") + "," + scales);
                    } else {
                        data.put("scales", scales);
                    }
                    
                    if (data.get("Java_Data_Type") != null && !data.get("Java_Data_Type").isEmpty()) {
                        data.put("Java_Data_Type", data.get("Java_Data_Type") + "," + javaDataType);
                    } else {
                        data.put("Java_Data_Type", javaDataType);
                    }
                    // Update the map
                    aggregateData.put(ilTableNameValue, data);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return aggregateData;
    }
    
    public Map<String, Map<String, String>> fetchMappingInfo(Connection connection, String dimensionTransaction, String connectionId, String querySchemaCond) {
        String query = "SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, " +
                       "IL_Data_Type, SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(IL_Data_Type,'(',-1),')',1),',',1) AS Precision_Val, " +
                       "SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(IL_Data_Type,'(',-1),')',1),',',-1) AS Scale_Val " +
                       "FROM ELT_IL_Source_Mapping_Info_Saved " +
                       "WHERE Constraints = 'PK' AND Dimension_Transaction = ? " +
                       "AND Connection_Id = ? " + querySchemaCond +
                       " ORDER BY IL_Table_Name, Constraints DESC";
        
        Map<String, Map<String, String>> resultData = new HashMap<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, connectionId);
            
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String key = resultSet.getString("Connection_Id") + "-" + resultSet.getString("Table_Schema") + "-" +
                                 resultSet.getString("IL_Table_Name") + "-" + resultSet.getString("IL_Column_Name");
                    
                    Map<String, String> valueMap = new HashMap<>();
                    valueMap.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                    valueMap.put("Precision_Val", resultSet.getString("Precision_Val"));
                    valueMap.put("Scale_Val", resultSet.getString("Scale_Val"));
                    
                    resultData.put(key, valueMap);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return resultData;
    }

    public Map<String, Map<String, String>> getStageKeysLookupData(Connection dbConnection, 
                                            String connectionId, 
                                            String querySchemaCondition, 
                                            String dimensionTransaction) throws SQLException {
        String sql = "SELECT `Connection_Id`, `Table_Schema`, `IL_Table_Name`, " +
                     "`IL_Column_Name`, `Constraints`, `Incremental_Column` " +
                     "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                     "WHERE `Incremental_Column` = 'Y' AND `Constraints` NOT IN ('PK', 'SK') " +
                     "AND `Dimension_Transaction` = ? AND `Connection_Id` = ? " + querySchemaCondition;

        Map<String, Map<String, String>> aggregateData = new HashMap<>();
        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, dimensionTransaction);
            stmt.setString(2, connectionId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String connectionIdValue = resultSet.getString("Connection_Id");
                    String tableSchema = resultSet.getString("Table_Schema");
                    String ilTableNameValue = resultSet.getString("IL_Table_Name"); // Key
                    String ilColumnName = resultSet.getString("IL_Column_Name");
                    String constraints = resultSet.getString("Constraints");
                    String tiltILColumnName = "`" + ilColumnName + "`";
                    String incrementalColumn = resultSet.getString("Incremental_Column");

                    Map<String, String> data = aggregateData.getOrDefault(ilTableNameValue, new HashMap<>());
                    // last values
                    data.put("Connection_Id", connectionIdValue);
                    data.put("Table_Schema", tableSchema);
                    data.put("Incremental_Column", incrementalColumn);
                    // List
                    if (data.containsKey("ilColumnName") && !data.get("ilColumnName").isEmpty()) {
                        data.put("ilColumnName", data.get("ilColumnName") + "," + ilColumnName);
                    } else {
                        data.put("ilColumnName", ilColumnName);
                    }
                    if (data.containsKey("Constraints") && !data.get("Constraints").isEmpty()) {
                        data.put("Constraints", data.get("Constraints") + "," + constraints);
                    } else {
                        data.put("Constraints", constraints);
                    }
                    if (data.containsKey("Tilt_IL_Column_Name") && !data.get("Tilt_IL_Column_Name").isEmpty()) {
                        data.put("Tilt_IL_Column_Name", data.get("Tilt_IL_Column_Name") + "," + tiltILColumnName);
                    } else {
                        data.put("Tilt_IL_Column_Name", tiltILColumnName);
                    }
                    // Update the map
                    aggregateData.put(ilTableNameValue, data);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return aggregateData;
    }

    public Map<String, Map<String, String>> updateKeys(Map<String, Map<String, String>> lookupData) {
        Map<String, Map<String, String>> updatedData = new HashMap<>();

        for (Map.Entry<String, Map<String, String>> entry : lookupData.entrySet()) {
            String oldKey = entry.getKey();
            Map<String, String> innerMap = entry.getValue();

            String connectionIdValue = innerMap.get("Connection_Id");
            String tableSchema = innerMap.get("Table_Schema");
            String ilTableNameValue = innerMap.get("IL_Table_Name");
            // old key is made of only IL_Table_Name
            String newKey = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
            updatedData.put(newKey, innerMap);
        }
        return updatedData;
    }

    public void deleteEntryFromStageHashColumnPick(Connection connection, String connectionId, String querySchemaCond) throws SQLException {
        String sql = "DELETE FROM stg_hashcolumn_pick WHERE Connection_Id = ? " + querySchemaCond;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, connectionId);

            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println(rowsAffected + " rows deleted from 'stg_hashcolumn_pick'.");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException("Error executing delete operation", e);
        }
    }

    //#####################################################

    private Status generateDBAlterScript() throws SQLException  {

        updateActiveFlagInAlterScriptInfo(conn, selectTables, connectionId, querySchemaCondition);
        Timestamp maxUpdatedDate =  getMaxUpdatedDate(conn, selectTables, connectionId, querySchemaCondition) ;
        System.out.println("maxUpdatedDate: " + maxUpdatedDate);

        // Flow 1
        List<Map<String, String>> mapInfo = processAlterDelete(conn, selectTables, connectionId, querySchemaCondition);

        // Flow 2
        processAlterTable(conn, selectTables, connectionId, querySchemaCondition, maxUpdatedDate.toString());


        return Status.SUCCESS;
    }
        // Alter JOb

        /**
         * Updates the Active_flag to 0 for specific IL_Table_Names and Connection_Id.
         *
         * @param connection     The active database connection.
         * @param selectiveTables Comma-separated IL_Table_Names for the WHERE clause.
         * @param connectionId   The connection ID to filter the records.
         * @param querySchemaCond Additional schema condition (e.g., AND conditions).
         * @throws SQLException If an SQL error occurs during execution.
         */
        private void updateActiveFlagInAlterScriptInfo(Connection connection, String selectiveTables, String connectionId, String querySchemaCond) throws SQLException {
            String sql = "UPDATE ELT_Alter_Script_Info SET Active_flag = 0 " +
                        "WHERE IL_Table_Name IN (" + selectiveTables + ") " +
                        "AND Connection_Id = ? " + querySchemaCond;

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, connectionId);
                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println(rowsAffected + " rows updated.");
            } catch (SQLException e) {
                System.err.println("Error executing UPDATE query: " + e.getMessage());
                throw e;
            }
        }
        

        private void deactivateAlterScriptInfo(Connection conn, String tableName) throws SQLException {
            String query = "UPDATE ELT_Alter_Script_Info SET Active_flag = 0 WHERE IL_Table_Name = ?";
    
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, tableName);
                int rowsAffected = stmt.executeUpdate();
                System.out.println(rowsAffected + " rows updated for Active_flag = 0.");
            }
        }

        // Note String return type
        // Simple timestamp may also work
        // global variable set as "max_updated_date"
        private Timestamp getMaxUpdatedDate(Connection connection, String selectiveTables, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT MAX(Updated_Date) FROM ELT_IL_Source_Mapping_Info WHERE IL_Table_Name IN (" + selectiveTables + ") AND Connection_Id = ? " + querySchemaCond;
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        Timestamp maxUpdatedDate = resultSet.getTimestamp(1);
                        if (maxUpdatedDate == null) {
                            return null;
                        } else {
                            // return "'" + maxUpdatedDate + "'"; // Transformation: Format as a string with single quotes
                            return maxUpdatedDate; // Transformation: Format as a string with single quotes
                        }
                    }
                }
            } catch (SQLException e) {
                throw new SQLException("Error while executing getMaxUpdatedDate query.", e);
            }
            // Returning null if no row is found
            return null;
        }
        /* row1 - to call alter_delete iterative job*/
        private List<Map<String, String>> processAlterDelete(Connection connection, String selectiveTables, String connectionId, String querySchemaCond) throws SQLException {
            String createTable = "NO"; // default Value
            // Connection for this function only
            Connection targetDBConnection = DBHelper.getTargetDBConnection(DataSourceType.MYSQL, dbDetails);
            String databaseName = tgtDbName;
            String query = "SELECT DISTINCT `ELT_IL_Source_Mapping_Info_Saved`.`Connection_Id`, `ELT_IL_Source_Mapping_Info_Saved`.`Table_Schema`, `ELT_IL_Source_Mapping_Info_Saved`.`IL_Table_Name` " +
                           "FROM `ELT_IL_Source_Mapping_Info_Saved` WHERE IL_Table_Name IN (" + selectiveTables + ") AND Connection_Id = ? " + querySchemaCond;
    
            List<Map<String, String>> result = new ArrayList<>();
            List<Map<String, Object>> finalResults = new ArrayList<>(); // Return object 
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        // Map<String, String> row = new HashMap<>();
                        // row.put("Connection_Id", resultSet.getString("Connection_Id"));
                        // row.put("Table_Schema", resultSet.getString("Table_Schema"));
                        // row.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));
                        String connectionIdvalue = resultSet.getString("Connection_Id");
                        String tableSchema = resultSet.getString("Table_Schema");
                        String ilTableName = resultSet.getString("IL_Table_Name");

                        boolean exists = doesTableExistInTargetDB(targetDBConnection, ilTableName, databaseName);
                        System.out.println("Does table exist: " + ilTableName + " - " + exists);
                        if (exists) {
                            createTable = "N";
                        }
                        if (createTable.equals("N")) {
                            System.out.println("Table Exists. Executing Alter Statement");
                            Map<String, Map<String, Object>> data = alterDeleteScriptComplete(ilTableName);
                            prepareAlterDeleteScriptData(data, tableSchema, ilTableName, connectionIdvalue);
                        } else {
                            System.out.println("Table Doesn't Exists. Proceeding with Create Statement");
                            continue;
                        }
                    }
                }
            } catch (SQLException e) {
                throw new SQLException("Error while executing getDistinctMappingInfo query.", e);
            }
            // Close the connection
            DBHelper.closeDBConnection(targetDBConnection);
            return result;
        }

        private void processAlterTable(Connection connection, String selectiveTables, String connectionId, String querySchemaCond, String maxUpdatedDate) throws SQLException {
            String sql = "SELECT DISTINCT Connection_Id, Table_Schema, IL_Table_Name " +
                         "FROM ELT_IL_Source_Mapping_Info_Saved " +
                         "WHERE IL_Table_Name IN (" + selectiveTables + ") AND Connection_Id = ? " +
                         querySchemaCond + " AND Updated_Date > ?";
            
            String createTable = "NO"; // default Value
            Connection targetDBConnection = DBHelper.getTargetDBConnection(DataSourceType.MYSQL, dbDetails);
            String databaseName = tgtDbName;
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, connectionId);
                stmt.setString(2, maxUpdatedDate);
        
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String connectionIdValue = rs.getString("Connection_Id");
                        String tableSchema = rs.getString("Table_Schema");
                        String ilTableName = rs.getString("IL_Table_Name");

                        Map<String, String> out = getChangeFlagAndPKColumns(conn, ilTableName);
                        String pkColumns = out.get("PKColumns");
                        String changeFlag = out.get("Change_Flag");
                        String finalStatement = getFinalStatement(conn, ilTableName);
                        String nonNullFinalStatement = getNonNullFinalStatement(conn, ilTableName);

                        boolean exists = doesTableExistInTargetDB(targetDBConnection, ilTableName, databaseName);
                        System.out.println("Does table exist: " + ilTableName + " - " + exists);
                        if (exists) {
                            createTable = "N";
                            deactivateAlterScriptInfo(conn, ilTableName);
                        }
                        if (createTable.equals("N")) {
                            System.out.println("Table Exists. Executing Alter Statement");
                            String deleteAlterScript = getFinalDropColumn(conn, ilTableName,connectionId, querySchemaCondition);
                            // maxUpdatedDate globally
                            Map<String, Map<String, Object>> data = alterScriptComplete(conn, ilTableName, maxUpdatedDate, nonNullFinalStatement, finalStatement, deleteAlterScript, changeFlag, pkColumns);
                            prepareAlterScriptData(data, tableSchema, ilTableName, connectionIdValue);
                        } else {
                            System.out.println("Table Doesn't Exists. Proceeding with Create Statement");
                            continue;
                        }

                        System.out.println("pkColumns: " + pkColumns);
                        System.out.println("changeFlag: " + changeFlag);
                        System.out.println("finalStatement: " + finalStatement);
                        System.out.println("nonNullFinalStatement: " + nonNullFinalStatement);
                        System.out.println(rs.getString("Connection_Id") + ", " +
                                           rs.getString("Table_Schema") + ", " +
                                           rs.getString("IL_Table_Name"));
                    }
                }
            }
        }

        private String getFinalDropColumn(Connection conn, String ilTableName, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, IL_Data_Type, " +
                           "Source_Table_Name, Source_Column_Name, Source_Data_Type " +
                           "FROM ELT_IL_Source_Mapping_Info " +
                           "WHERE IL_Table_Name = ? AND Column_Type <> 'Anvizent' AND Connection_Id = ? " + querySchemaCond;
            Map<String, Map<String, Object>> lookupData = getSourceMappingInfo(ilTableName, connectionId, querySchemaCond);
            Map<String, Map<String, String>> aggregateData = new HashMap<>();
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, ilTableName);
                stmt.setString(2, connectionId);
    
                ResultSet rs = stmt.executeQuery();
                String finalDropColumn = null;

                while (rs.next()) {
                    String key = rs.getString("Connection_Id") + "-" + rs.getString("Table_Schema") + "-" +
                                 rs.getString("Source_Table_Name") + "-" + rs.getString("Source_Column_Name");
                    String connectionIdValue = rs.getString("Connection_Id");
                    String tableSchemaValue = rs.getString("Table_Schema");
                    String ilColumnNameValue = rs.getString("Source_Table_Name");
                    String ilTableNameValue = rs.getString("IL_Table_Name");

                    // Anti Join
                    String ilColumnName = "";
                    if (!lookupData.containsKey(key)) {
                        ilColumnName = "Drop Column `" + ilColumnNameValue + "`";
                    }
                    System.out.println("Processing key: " + key);

                    String dropColumn = ilColumnName;
                    finalDropColumn = (finalDropColumn == null) ? dropColumn : finalDropColumn + "," + dropColumn;
                    String script = "ALTER TABLE `" + ilTableNameValue + "` DROP COLUMN " + finalDropColumn;
                    String ilAlterScript = script.substring(0, script.length() - 1) + ";"; // out
                    String stgScript = "ALTER TABLE `" + ilTableNameValue + "_Stg` DROP COLUMN " + finalDropColumn;
                    String stgAlterScript = stgScript.substring(0, stgScript.length() - 1) + ";"; // out

                    String aggregationKey = connectionIdValue + tableSchemaValue + ilTableNameValue;
                    Map<String, String> data = aggregateData.getOrDefault(aggregationKey, new HashMap<>());
                    data.put("IL_Alter_Script", ilAlterScript);
                    data.put("Stg_Alter_Script", stgAlterScript);
                    data.put("Final_Drop_Column", finalDropColumn);
                    
                    aggregateData.put(aggregationKey, data);
                }
            }
            // only one key value pair likely
            System.out.println("aggregateData size:  " + aggregateData.size());
            String finalDropColumnOut = "";
            for (Map<String, String> innerMap : aggregateData.values()) {
                if (innerMap.containsKey("Final_Drop_Column")) {
                    finalDropColumnOut = innerMap.get("Final_Drop_Column");
                    break; // Exit the loop once the value is found
                }
            }
            return finalDropColumnOut;
        }

        public Map<String, Map<String, Object>> getSourceMappingInfo(String ilTableName, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT `Connection_Id`, `Table_Schema`, `IL_Table_Name`, `IL_Column_Name`, `IL_Data_Type`, "
                         + "`Source_Table_Name`, `Source_Column_Name`, `Source_Data_Type` "
                         + "FROM `ELT_IL_Source_Mapping_Info_Saved` WHERE IL_Table_Name = ? AND Connection_Id = ? "
                         + querySchemaCond;
        
            Map<String, Map<String, Object>> result = new HashMap<>();
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, ilTableName);
                stmt.setString(2, connectionId);
    
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    String key = rs.getString("Connection_Id") + "-" + rs.getString("Table_Schema") + "-"
                            + rs.getString("Source_Table_Name") + "-" + rs.getString("Source_Column_Name");

                    Map<String, Object> value = new HashMap<>();
                    value.put("IL_Table_Name", rs.getString("IL_Table_Name"));
                    value.put("IL_Column_Name", rs.getString("IL_Column_Name"));
                    value.put("IL_Data_Type", rs.getString("IL_Data_Type"));
                    value.put("Source_Data_Type", rs.getString("Source_Data_Type"));
                    result.put(key, value);
                }
            }
            return result;
        }
        // Alter script main component
        public Map<String, Map<String, Object>> alterScriptComplete(Connection conn, String ilTableName, String maxUpdatedDate,
                                String notNullFinalStatement, String finalStatement, String deleteAlterScriptIn, String changeFlag, String pkColumns ) {
            String query = "SELECT "
                         + "esms.`IL_Table_Name`, "
                         + "esms.`IL_Column_Name`, "
                         + "esms.`Constraints`, "
                         + "esms.`IL_Data_Type`, "
                         + "esms.`Dimension_Transaction`, "
                         + "esmi.IL_Column_Name AS IL_Column_Name_Lookup, "
                         + "esmi.IL_Data_Type AS IL_Data_Type_Lookup "
                         + "FROM `ELT_IL_Source_Mapping_Info_Saved` AS esms "
                         + "LEFT OUTER JOIN `ELT_IL_Source_Mapping_Info` AS esmi "
                         + "ON esms.IL_Table_Name = esmi.IL_Table_Name "
                         + "AND esms.IL_Column_Name = esmi.IL_Column_Name "
                         + "WHERE esms.IL_Table_Name = ? "
                         + "AND esms.Updated_Date > ?";
        
            Map<String, Map<String, Object>> result = new HashMap<>();
            
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, ilTableName);
                ps.setString(2, maxUpdatedDate);
                ResultSet rs = ps.executeQuery();
        
                String finalAddingColumn = null;
                while (rs.next()) {
                    // String key = rs.getString("IL_Table_Name") + "-" + rs.getString("IL_Column_Name");
                    Map<String, Object> value = new HashMap<>();
                    String ilTableNameValue = rs.getString("IL_Table_Name");
                    String ilColumnNameValue = rs.getString("IL_Column_Name");
                    String ConstraintsValue = rs.getString("Constraints");
                    String ilDataTypeValue = rs.getString("IL_Data_Type");
                    String dimensionTransactionValue = rs.getString("Dimension_Transaction");

                    String ilColumnNameLookup = rs.getString("IL_Column_Name_Lookup");
                    String ilDataTypeLookup = rs.getString("IL_Data_Type_Lookup");;

                    String notnullQuery = notNullFinalStatement;
                    String notnullCheck = (notnullQuery == null || notnullQuery.isEmpty() || "NULL".equals(notnullQuery)) ? "" : notnullQuery + ",";
                    String nullQuery = finalStatement;
                    String notnullFlag = (nullQuery == null || nullQuery.isEmpty() || "NULL".equals(nullQuery)) ? notnullCheck : notnullCheck + nullQuery + ",";

                    String DLDataTypes = ilDataTypeValue.toLowerCase().contains("bit") ? "tinyint(1)" : ilDataTypeValue;

                    String addingColumn = (ilColumnNameLookup == null) ?
                            "ADD COLUMN `" + ilColumnNameValue + "` " + DLDataTypes :
                            (ilColumnNameLookup == null ? "" :
                                    ((!ilColumnNameValue.equals(ilColumnNameLookup) || !ilDataTypeValue.equals(ilDataTypeLookup)) && "PK".equals(ConstraintsValue)) ?
                                            "CHANGE `" + ilColumnNameLookup + "` `" + ilColumnNameValue + "` " + DLDataTypes + " NOT NULL " :
                                            (!ilColumnNameValue.equals(ilColumnNameLookup) || !ilDataTypeValue.equals(ilDataTypeLookup)) ?
                                                    "CHANGE `" + ilColumnNameLookup + "` `" + ilColumnNameValue + "` " + DLDataTypes : ""
                            );

                    finalAddingColumn = (addingColumn == null || addingColumn.isEmpty()) ? finalAddingColumn :
                            (finalAddingColumn == null ? addingColumn : finalAddingColumn + "," + addingColumn);

                    String dropColumns = (deleteAlterScriptIn == null || deleteAlterScriptIn.isEmpty()) ? "" : deleteAlterScriptIn;

                    String dropFlag = (dropColumns == null || dropColumns.isEmpty()) ? "N" : "Y";

                    String scripts = ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("N")) ? "" :
                            ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("Y")) ?
                                    "ALTER TABLE `" + ilTableNameValue + "` " + dropColumns :
                                    (dropFlag.equals("N")) ?
                                            "ALTER TABLE `" + ilTableNameValue + "` " + "\n " + notnullFlag + " " + finalAddingColumn :
                                            "ALTER TABLE `" + ilTableNameValue + "` " + "\n " + notnullFlag + " " + dropColumns + ", " + finalAddingColumn;

                    String ilScript = (scripts == null || scripts.isEmpty()) && "N".equals(changeFlag) ? "" :
                            (scripts == null || scripts.isEmpty()) && "Y".equals(changeFlag) ?
                                    "Alter table `" + ilTableNameValue + "`\n " + notnullFlag + "\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " + pkColumns + ");" :
                                    (scripts != null && !scripts.isEmpty() && "Y".equals(changeFlag)) ?
                                            scripts.substring(0, scripts.length()) + ",\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " +
                                                    pkColumns + ");" :
                                            scripts.substring(0, scripts.length()) + ";";

                    String dScripts = ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("N")) ? "" :
                            ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("Y")) ?
                                    "ALTER TABLE `" + ilTableNameValue + "_Stg` " + dropColumns :
                                    (dropFlag.equals("N")) ?
                                            "ALTER TABLE `" + ilTableNameValue + "_Stg` " + "\n " + notnullFlag + " " + finalAddingColumn :
                                            "ALTER TABLE `" + ilTableNameValue + "_Stg` " + "\n " + notnullFlag + " " + dropColumns + ", " + finalAddingColumn;

                    String dimStgScript = (dScripts == null || dScripts.isEmpty()) && "N".equals(changeFlag) ? "" :
                            (dScripts == null || dScripts.isEmpty()) && "Y".equals(changeFlag) ?
                                    "Alter table `" + ilTableNameValue + "_Stg`\n " + notnullFlag + "\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " + pkColumns + ");" :
                                    (dScripts != null && !dScripts.isEmpty() && "Y".equals(changeFlag)) ?
                                            dScripts.substring(0, dScripts.length()) + ",\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " +
                                                    pkColumns + ");" :
                                            dScripts.substring(0, dScripts.length()) + ";";

                    String tranStgScript = ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("N")) ? "" :
                            ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("Y")) ?
                                    "ALTER TABLE `" + ilTableNameValue + "_Stg` " + dropColumns :
                                    (dropFlag.equals("N")) ?
                                            "ALTER TABLE `" + ilTableNameValue + "_Stg` " + finalAddingColumn :
                                            "ALTER TABLE `" + ilTableNameValue + "_Stg` " + dropColumns + ", " + finalAddingColumn;

                    String tranStgAlterScript = (tranStgScript == null || tranStgScript.isEmpty()) ? "" : tranStgScript.substring(0, tranStgScript.length()) + ";";

                    String deleteScript = ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("N")) ? "" :
                            ((finalAddingColumn == null || finalAddingColumn.isEmpty()) && dropFlag.equals("Y")) ?
                                    "ALTER TABLE `" + ilTableNameValue + "_Deletes` " + dropColumns :
                                    (dropFlag.equals("N")) ?
                                            "ALTER TABLE `" + ilTableNameValue + "_Deletes` " + "\n " + notnullFlag + " " + finalAddingColumn :
                                            "ALTER TABLE `" + ilTableNameValue + "_Deletes` " + "\n " + notnullFlag + " " + dropColumns + ", " + finalAddingColumn;

                    String deleteAlterScript = (deleteScript == null || deleteScript.isEmpty()) && "N".equals(changeFlag) ? "" :
                            (deleteScript == null || deleteScript.isEmpty()) && "Y".equals(changeFlag) ?
                                    "Alter table `" + ilTableNameValue + "_Deletes`\n " + notnullFlag + "\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " + pkColumns + ");" :
                                    (deleteScript != null && !deleteScript.isEmpty() && "Y".equals(changeFlag)) ?
                                            deleteScript.substring(0, deleteScript.length()) + ",\n DROP PRIMARY KEY, \n ADD PRIMARY KEY ( " +
                                                    pkColumns + ");" :
                                            deleteScript.substring(0, deleteScript.length()) + ";";

                    String ilChecking = (ilScript == null || "NULL".equals(ilScript) || ilScript.isEmpty()) ? "N" : ilScript;
                    String deletesChecking = (deleteAlterScript == null || "NULL".equals(deleteAlterScript) || deleteAlterScript.isEmpty()) ? "N" : deleteAlterScript;
                    String tranChecking = (tranStgAlterScript == null || "NULL".equals(tranStgAlterScript) || tranStgAlterScript.isEmpty()) ? "" : tranStgAlterScript;
                    String dimChecking = (dimStgScript == null || "NULL".equals(dimStgScript) || dimStgScript.isEmpty()) ? "N" : dimStgScript;

                    // conditional
                    String key =  connectionId + "-" + schemaName + "-" + ilTableNameValue;
                    if (!ilChecking.equals("N")) {
                        Map<String, Object> data = result.getOrDefault(key, new HashMap<>());
                        data.put("Connection_Id", connectionId);
                        data.put("Table_Schema", schemaName);
                        data.put("IL_Table_Name", ilTableNameValue);
                        data.put("Dimension_Transaction", dimensionTransactionValue);
                        data.put("IL_Alter_Script", ilChecking);
                        data.put("Dim_Stg_Script", dimChecking);
                        data.put("Tran_Stg_Alter_Script", tranChecking);
                        data.put("Delete_Alter_Script", deletesChecking);

                        result.put(key, data);
                    }

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        
            return result;
        }
        
        /* to check if table exists but check it in target DB */
        // Do we need dtabase name here 
        // Function to check if a table exists in the target database, excluding views
        public boolean doesTableExistInTargetDB(Connection targetConnection, String tableName, String databaseName) throws SQLException  {
            String checkQuery = "SELECT COUNT(*) FROM information_schema.tables " +
                                "WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'";
            try (PreparedStatement stmt = targetConnection.prepareStatement(checkQuery)) {
                stmt.setString(1, databaseName);
                stmt.setString(2, tableName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // Returning true if count is greater than 0, indicating the table exists
                        return rs.getInt(1) > 0;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;
        }
        /* Alter delete job main 2 queries */
        // Check if throw exception can be changed to SQL Exception
        private List<Map<String, String>> executeAntiJoinQuery(Connection connection, String ilTableName, String connectionId, String querySchemaCond) throws SQLException {
            // anti join
            String query = "SELECT A.Connection_Id, A.Table_Schema, A.IL_Table_Name, A.IL_Column_Name " +
                           "FROM (" +
                           "    SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, Source_Table_Name, Source_Column_Name " +
                           "    FROM ELT_IL_Source_Mapping_Info " +
                           "    WHERE IL_Table_Name = ? AND Column_Type <> 'Anvizent' AND Connection_Id = ? " + querySchemaCond +
                           ") AS A " +
                           "WHERE NOT EXISTS (" +
                           "    SELECT 1 " +
                           "    FROM (" +
                           "        SELECT Connection_Id, Table_Schema, Source_Table_Name, Source_Column_Name " +
                           "        FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "        WHERE IL_Table_Name = ? AND Connection_Id = ? " + querySchemaCond +
                           "    ) AS B " +
                           "    WHERE A.Connection_Id = B.Connection_Id AND A.Table_Schema = B.Table_Schema " +
                           "      AND A.Source_Table_Name = B.Source_Table_Name AND A.Source_Column_Name = B.Source_Column_Name" +
                           ")";
        
            List<Map<String, String>> result = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
                preparedStatement.setString(3, ilTableName);
                preparedStatement.setString(4, connectionId);
        
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, String> row = new HashMap<>();
                        row.put("Connection_Id", resultSet.getString("Connection_Id"));
                        row.put("Table_Schema", resultSet.getString("Table_Schema"));
                        row.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));
                        row.put("IL_Column_Name", " Drop Column `" + resultSet.getString("IL_Column_Name") + "`"); // Transformation
                        result.add(row);
                    }
                }
            } catch (SQLException e) {
                throw new SQLException("Error while executing anti-join query.", e);
            }
        
            return result;
        }
// main flow of alter delete
        void alterDeleteScript (String ilTableName) {

            try {
                List<Map<String, String>> data = executeAntiJoinQuery(conn, ilTableName, connectionId, querySchemaCondition);

                String finalDropColumn = new String(); 
                for (Map<String, String> map : data) {
                    String ilColumnName = map.get("IL_Column_Name");
                    
                    System.out.println("IL_Column_Name: " + ilColumnName);

                    String dropColumn = ilColumnName;
                    // TODO check below original conversion is valid? As we are doing aggregation later. It is same effect.
                    // String finalDropColumn = (finalDropColumn == null) ? dropColumn + "," : finalDropColumn + dropColumn + ",";
                    finalDropColumn = (finalDropColumn == null) ? dropColumn + "," : finalDropColumn + dropColumn + ",";

                    // the main Alter table script
                    String script = "ALTER TABLE `" + ilTableName + "` " + finalDropColumn;
                    String ilAlterScript = script.substring(0, script.length() - 1) + ";";

                    // the staging script
                    String stgScript = "ALTER TABLE `" + ilTableName + "_Stg` " + finalDropColumn;
                    String stgAlterScript = stgScript.substring(0, stgScript.length() - 1) + ";"; // Remove the last comma and append a semicolon

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }
        // With all the aggregation
        Map<String, Map<String, Object>> alterDeleteScriptComplete(String ilTableName) throws SQLException {

            List<Map<String, String>> data = executeAntiJoinQuery(conn, ilTableName, connectionId, querySchemaCondition);
            Map<String, Map<String, Object>> groupedScripts = new HashMap<>();
        
            for (Map<String, String> map : data) {
                String connectionId = map.get("Connection_Id");
                String tableSchema = map.get("Table_Schema");
                String tableName = map.get("IL_Table_Name");
                String ilColumnName = map.get("IL_Column_Name");
        
                System.out.println("IL_Column_Name: " + ilColumnName);
        
                // group key
                String groupKey = connectionId + "-" + tableSchema + "-" + tableName;
        
                // Retrieve or initialize the group entry
                Map<String, Object> group = groupedScripts.getOrDefault(groupKey, new HashMap<>());
        
                // Initialize or append drop columns
                String finalDropColumn = group.getOrDefault("Final_Drop_Column", "") + ilColumnName + ",";
        
                // the main and staging scripts
                String ilAlterScript = "ALTER TABLE `" + tableName + "`" + finalDropColumn;
                ilAlterScript = ilAlterScript.substring(0, ilAlterScript.length() - 1) + ";"; // Remove the last comma and append a semicolon
        
                String stgAlterScript = "ALTER TABLE `" + tableName + "_Stg`" + finalDropColumn;
                stgAlterScript = stgAlterScript.substring(0, stgAlterScript.length() - 1) + ";"; // Remove the last comma and append a semicolon
        
                // Update the group entry
                group.put("Final_Drop_Column", finalDropColumn);
                // group.put("IL_Alter_Script", group.getOrDefault("IL_Alter_Script", "") + ilAlterScript + "\n");  // TODO just assignmentis sufficient
                // group.put("Stg_Alter_Script", group.getOrDefault("Stg_Alter_Script", "") + stgAlterScript + "\n"); // TODO
                group.put("IL_Alter_Script", ilAlterScript );  // TODO just assignmentis sufficient
                group.put("Stg_Alter_Script", stgAlterScript); // TODO

                // Store back the group entry
                groupedScripts.put(groupKey, group);
            }

            // Print aggregated results
            for (Map.Entry<String, Map<String, Object>> entry : groupedScripts.entrySet()) {
                String groupKey = entry.getKey();
                Map<String, Object> group = entry.getValue();
        
                System.out.println("Group: " + groupKey);
                System.out.println("IL_Alter_Script:\n" + group.get("IL_Alter_Script"));
                System.out.println("Stg_Alter_Script:\n" + group.get("Stg_Alter_Script"));
            }
            return groupedScripts;
        }

        void prepareAlterDeleteScriptData(Map<String, Map<String, Object>> data, String tableSchema, String ilTableName, String connectionIdvalue) throws SQLException {

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            List<Map<String, Object>> finalResults = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> value = entry.getValue();

                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionIdvalue);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("IL_Alter_Script", value.get("IL_Alter_Script"));
                resultRow.put("Stg_Alter_Script", value.get("Stg_Alter_Script"));
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);
            }

            String tableName = "ELT_Alter_Script_Info";
            if (finalResults != null && !finalResults.isEmpty()) {
                // TODO delete query to be added
                // deleteSelectScripts(conn, ilTableName, connectionId, querySchemaCondition);
                saveDataIntoDB(conn, tableName, finalResults);
            }
        }
        /* Alter Script main*/
        void prepareAlterScriptData(Map<String, Map<String, Object>> data, String tableSchema, String ilTableName, String connectionIdvalue) throws SQLException {

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            List<Map<String, Object>> finalResults = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : data.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> value = entry.getValue();

                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionIdvalue);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("IL_Alter_Script", value.get("IL_Alter_Script"));

                String stageAlterScript = "";
                if (value.get("Dimension_Transaction").equals("D")) {
                    stageAlterScript = (String)  value.get("Dim_Stg_Script"); // Dim_Stg_Script
                } else {
                    stageAlterScript = (String) value.get("Tran_Stg_Alter_Script"); // Tran_Stg_Alter_Script
                }
                resultRow.put("Stg_Alter_Script", stageAlterScript);

                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);
            }

            String tableName = "ELT_Alter_Script_Info";
            if (finalResults != null && !finalResults.isEmpty()) {
                // TODO delete query to be added
                // deleteSelectScripts(conn, ilTableName, connectionId, querySchemaCondition);
                saveDataIntoDB(conn, tableName, finalResults);
            }
        }

        private String getNonNullFinalStatement(Connection conn, String tableName)  throws SQLException {
                String query = "SELECT main.IL_Table_Name, main.IL_Column_Name, " +
                               "main.tilt_columns, main.IL_Data_Type, " +
                               "COALESCE(main.Constraints, '') AS Constraints_main, " +
                               "COALESCE(lookup.Constraints, '') AS Constraints_lookup " +
                               "FROM (SELECT IL_Table_Name, IL_Column_Name, " +
                               "CONCAT('`', IL_Column_Name, '`') AS tilt_columns, " +
                               "IL_Data_Type, Constraints " +
                               "FROM ELT_IL_Source_Mapping_Info_Saved " +
                               "WHERE IL_Table_Name = ? AND Constraints = 'PK') AS main " +
                               "LEFT JOIN (SELECT IL_Table_Name, IL_Column_Name, " +
                               "CONCAT('`', IL_Column_Name, '`') AS tilt_columns, " +
                               "IL_Data_Type, Constraints " +
                               "FROM ELT_IL_Source_Mapping_Info " +
                               "WHERE IL_Table_Name = ?) AS lookup " +
                               "ON main.IL_Column_Name = lookup.IL_Column_Name " +
                               "ORDER BY main.IL_Column_Name";
            
            String finalStatement = new String();

            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, tableName);  // For both main and lookup queries
                stmt.setString(2, tableName);  // For both main and lookup queries

                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    String tiltColumns = rs.getString("tilt_columns");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String mainConstraints = rs.getString("Constraints_main");

                    // Lookup values
                    String lookupConstraints = rs.getString("Constraints_lookup");


                    String condition = (ilDataType == null) ? null
                            : (mainConstraints != null && mainConstraints.toLowerCase().equalsIgnoreCase("pk") && lookupConstraints.isEmpty()
                                    ? "Change column " + tiltColumns + " " + tiltColumns + " " + ilDataType
                                            + " not NULL"
                                    : null);

                    // String finalStatement = null;
                    finalStatement = (finalStatement == null) ? condition
                            : (condition == null ? finalStatement : finalStatement + "," + condition);


                    // String condition = (lookupIlDataType == null) ? null :
                    //     (constraints.toLowerCase().contains("pk") && lookupConstraints.equals("")) ?
                    //     "Change column " + tiltColumns + " " + tiltColumns + " " + ilDataType + " not NULL " : null;

                    // finalStatement = (finalStatement == null) ? condition :
                    //     (condition == null ? finalStatement : finalStatement + "," + condition);
                    // String ilTableName = tableName;

                    // Process the data as needed
                    // System.out.println("IL Column Name: " + ilColumnName);
                    // System.out.println("Tilt Columns: " + tiltColumns);
                    // System.out.println("IL Data Type: " + ilDataType);
                    // System.out.println("Constraints: " + constraints);
                    // System.out.println("Lookup IL Column Name: " + lookupIlColumnName);
                    // System.out.println("Lookup Tilt Columns: " + lookupTiltColumns);
                    // System.out.println("Lookup IL Data Type: " + lookupIlDataType);
                    // System.out.println("Lookup Constraints: " + lookupConstraints);

                    System.out.println("condition: " + condition);
                    System.out.println("final_statements: " + finalStatement);

                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return finalStatement;
        }

        private String getFinalStatement(Connection conn, String tableName) {
            // Main query
            String mainQuery = "SELECT " + "`IL_Column_Name`, " +
                    "CONCAT('`', `IL_Column_Name`, '`') AS tilt_columns, " +
                    "IL_Data_Type, " + "IFNULL(Constraints, '') AS Constraints " +
                    "FROM `ELT_IL_Source_Mapping_Info` " +
                    "WHERE IL_Table_Name = ? AND Constraints = 'PK' " +
                    "ORDER BY IL_Column_Name";

            // Lookup query
            String lookupQuery = "SELECT " + "`IL_Column_Name`, " +
                                "CONCAT('`', `IL_Column_Name`, '`') AS tilt_columns, " +
                                "IL_Data_Type, " + "IFNULL(Constraints, '') AS Constraints " +
                                "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                                "WHERE IL_Table_Name = ? " + "ORDER BY IL_Column_Name";

            // Left Outer Join query
            String joinQuery = "SELECT " + "a.`IL_Column_Name`, " + "a.tilt_columns, " +
                            "a.IL_Data_Type, " + "a.Constraints, " +
                            "b.`IL_Column_Name` AS lookup_IL_Column_Name, " +
                            "b.tilt_columns AS lookup_tilt_columns, " +
                            "b.IL_Data_Type AS lookup_IL_Data_Type, " +
                            "b.Constraints AS lookup_Constraints " +
                            "FROM (" + mainQuery + ") a " +
                            "LEFT JOIN (" + lookupQuery + ") b " +
                            "ON a.`IL_Column_Name` = b.`IL_Column_Name` " +
                            "AND a.IL_Data_Type = b.IL_Data_Type";
            
            String finalStatement = new String();

            try (PreparedStatement stmt = conn.prepareStatement(joinQuery)) {
                stmt.setString(1, tableName);  // For both main and lookup queries
                stmt.setString(2, tableName);  // For both main and lookup queries

                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    String ilColumnName = rs.getString("IL_Column_Name");
                    String tiltColumns = rs.getString("tilt_columns");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String constraints = rs.getString("Constraints");

                    // Lookup values
                    String lookupIlColumnName = rs.getString("lookup_IL_Column_Name");
                    String lookupTiltColumns = rs.getString("lookup_tilt_columns");
                    String lookupIlDataType = rs.getString("lookup_IL_Data_Type");
                    String lookupConstraints = rs.getString("lookup_Constraints");


                    String condition = (lookupIlDataType == null) ? null :
                        (constraints.toLowerCase().contains("pk") && lookupConstraints.equals("")) ?
                        "Change column " + tiltColumns + " " + tiltColumns + " " + ilDataType + "  NULL " : null;

                    finalStatement = (finalStatement == null) ? condition :
                        (condition == null ? finalStatement : finalStatement + "," + condition);
                    String ilTableName = tableName;


                    // Process the data as needed
                    // System.out.println("IL Column Name: " + ilColumnName);
                    // System.out.println("Tilt Columns: " + tiltColumns);
                    // System.out.println("IL Data Type: " + ilDataType);
                    // System.out.println("Constraints: " + constraints);
                    // System.out.println("Lookup IL Column Name: " + lookupIlColumnName);
                    // System.out.println("Lookup Tilt Columns: " + lookupTiltColumns);
                    // System.out.println("Lookup IL Data Type: " + lookupIlDataType);
                    // System.out.println("Lookup Constraints: " + lookupConstraints);

                    System.out.println("condition: " + condition);
                    System.out.println("final_statements: " + finalStatement);

                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return finalStatement;
        }

        private Map<String, Map<String, String>> getAggregatedColumns(Connection conn, String tableName) throws SQLException {
            String query = "SELECT IL_Table_Name, IL_Column_Name, " +
                           "CONCAT('`', IL_Column_Name, '`') AS tilt_column_names " +
                           "FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "WHERE IL_Table_Name = ? AND Constraints = 'PK' " +
                           "ORDER BY IL_Column_Name";
    
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, tableName);
    
            ResultSet rs = stmt.executeQuery();
            StringBuilder ilColumnNames = new StringBuilder();
            StringBuilder tiltColumnNames = new StringBuilder();
    
            while (rs.next()) {
                if (ilColumnNames.length() > 0) {
                    ilColumnNames.append(",");
                    tiltColumnNames.append(",");
                }
                ilColumnNames.append(rs.getString("IL_Column_Name"));
                tiltColumnNames.append(rs.getString("tilt_column_names"));
            }

            Map<String, String> data = new HashMap<>();
            data.put("IL_Column_Names", ilColumnNames.toString());
            data.put("Tilt_Column_Names", tiltColumnNames.toString());
            // Return as key, value. There is only one entry
            Map<String, Map<String, String>> dataMap = new HashMap<>();

            String key = tableName + "-" + ilColumnNames.toString();

            dataMap.put(key, data);
            System.out.println("IL_Column_Names: " + ilColumnNames.toString() + ", Tilt_Column_Names: " + tiltColumnNames.toString());
            return dataMap;
        }
        
        private Map<String, String> getChangeFlagAndPKColumns(Connection conn, String tableName) throws SQLException {

            Map<String, String> returnData = new HashMap<>();
            Map<String, String> lookupData = getAggregatedILColumnNames(conn, tableName);
            Map<String, Map<String, String>> mainData = getAggregatedColumns(conn, tableName);
            
            for (Map.Entry<String, Map<String, String>> outerEntry : mainData.entrySet()) {
                String key = outerEntry.getKey();
                Map<String, String> values = outerEntry.getValue();

                String tiltColumnNames = values.get("Tilt_Column_Names");
                String ilColumnName = lookupData.getOrDefault(key, new String());

                String pkColumns = tiltColumnNames;
                String changeFlag = (ilColumnName == null) ? "Y" : "N"; 
                returnData.put("PKColumns", pkColumns);
                returnData.put("Change_Flag", changeFlag);
            }
            return returnData;
        }
        private Map<String, String> getAggregatedILColumnNames(Connection conn, String tableName) throws SQLException {
            String query = "SELECT IL_Table_Name, IL_Column_Name " +
                           "FROM ELT_IL_Source_Mapping_Info " +
                           "WHERE IL_Table_Name = ? AND Constraints = 'PK' " +
                           "ORDER BY IL_Column_Name";
    
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, tableName);
    
            ResultSet rs = stmt.executeQuery();
            StringBuilder ilColumnNames = new StringBuilder();
    
            while (rs.next()) {
                if (ilColumnNames.length() > 0) {
                    ilColumnNames.append(",");
                }
                ilColumnNames.append(rs.getString("IL_Column_Name"));
            }
    
            // Return as key, value. There is only one entry
            Map<String, String> updatedData = new HashMap<>();

            String key = tableName + "-" + ilColumnNames.toString();
            updatedData.put(key, ilColumnNames.toString());
            System.out.println("IL_Column_Names: " + ilColumnNames.toString());

            return updatedData;
        }
 //   }
    
/*
 * ######################################################################################################
 * ELT Delete Jobs - start
 */
        
        
 /*
 * ELT Delete Jobs - End
 * ######################################################################################################
 */

}
    public class DWConfigScriptsGenerator {



        String limitFunction;

        // Constructor
        public DWConfigScriptsGenerator() {
            // Initialize limitFunc from input parameters. It's applicable only for Config scripts
            initConfigScript();
        }

        private void initConfigScript() {
            if (multiIlConfigFile.equals("Y")) {
                limitFunction = "";
            } else {
                limitFunction = " limit 1";
            }
        }
    
        // Method to generate the configuration script
        public Status generateConfigScript() {
            System.out.println("\n### Generating Config Scripts ...");
            //String dimensionTransaction = "D";
            try {

            // Dim_SRC_STG
            dimSrcToStgConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);
            
            // Dim_STG_IL
            dimStgToIlConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            // Trans_SRC_STG
            transSrcToStgConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            // Trans_STG_Keys
            transStgKeysConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            // Trans_STG_IL
            transStgToIlConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            // Deletes_Dim
            dimDeleteConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            // Deletes_Trans 
            transDeleteConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            } catch (SQLException e) {
                e.printStackTrace();
                return Status.FAILURE;
            }
            return Status.SUCCESS;
        }

        /* Dimension Source to Staging Config */
        void dimSrcToStgConfigScript(Connection connection, String selectiveTables, 
        String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Dimension Source to Staging Config..");
            String dimensionTransaction = "D";
            String jobType = "Dimension_src_stg";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            // System.out.println("\n    list of data (dimSrcToStgConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) { // member variable
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }
                // Delete ilTable + " _Stg_Keys" 
                deleteConfigProperties(connection, ilTable + "_Stg_Keys", connectionId);

                String writeMode = getWriteMode(ilTableName);

                Map<String, AggregatedData> mainDataMap = processSrcStgConfigJobProperties(connection, connectionId, tableSchema, jobType, writeMode);
                final String fileName = getConfigFileName(ilTableName, STG_CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    final String script = value.getScript();

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", tableSchema);
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    // resultRow.put("Script", script);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }
                //System.out.println("    SRC to STG script fileName: " + fileName);

                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable, connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }
            }
        }
        /* Dimension Stage to DW Config */
        void dimStgToIlConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Dimension Stage to DW Config..");
            String dimensionTransaction = "D";
            String jobType = "Dimension_stg_il";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            // System.out.println("\n    list of data (dimStgToIlConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName;
                } else {
                    ilTable = "STG_IL_ALL";
                }

                String writeMode = getWriteMode(ilTableName);

                // TODO name change of the function
                Map<String, AggregatedData> mainDataMap = processSrcStgConfigJobProperties(connection, connectionId, tableSchema, jobType, writeMode);
                final String fileName = getConfigFileName(ilTableName, CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    final String script = value.getScript();

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", schemaName); // Note schemaName
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }

                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable, connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }
            }
        }
        /* Transaction Source to Staging Config */
        void transSrcToStgConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Transaction Source to Staging Config..");
            String dimensionTransaction = "T";
            String jobType = "Transaction_src_stg";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            // System.out.println("\n    list of data (dimSrcToStgConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "TRANS_SRC_STG_All";
                }

                String writeMode = getWriteMode(ilTableName);

                Map<String, AggregatedData> mainDataMap = processSrcStgConfigJobProperties(connection, connectionId, tableSchema, jobType, writeMode);
                final String fileName = getConfigFileName(ilTableName, STG_CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    final String script = value.getScript();

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", schemaName); // Note Schema Name
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }
                // System.out.println("  Trans SRC to STG script fileName: " + fileName);

                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable, connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }
            }
        }
        /*  Transaction Stage Keys Config */
        void transStgKeysConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Transaction Stage Keys Config..");
            String dimensionTransaction = "T";
            String jobType = "Transaction_stg_keys";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            // System.out.println("\n    list of data (dimSrcToStgConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg_Keys";
                } else {
                    ilTable = "TRANS_SRC_STG_Keys_All";
                }

                String writeMode = getWriteMode(ilTableName);

                Map<String, AggregatedData> mainDataMap = processSrcStgConfigJobProperties(connection, connectionId, tableSchema, jobType, writeMode);
                final String fileName = getConfigFileName(ilTableName, STG_KEYS_CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    String script = value.getScript();
                    script = script.replace(",", "\n"); // TODO Note in many of them it has to go

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", schemaName); // Note Schema Name
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }

                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable, connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }
            }
        }
        /*  Transaction Stage to DW Config */
        void transStgToIlConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Transaction Stage to DW Config..");
            String dimensionTransaction = "T";
            String jobType = "Transaction_stg_il";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            // System.out.println("\n    list of data (dimSrcToStgConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName;
                } else {
                    ilTable = "TRANS_SRC_STG_All";
                }

                String writeMode = getWriteMode(ilTableName);

                Map<String, AggregatedData> mainDataMap = processSrcToIlConfigJobProperties(connection, connectionId, tableSchema, ilTableName, jobType, writeMode);
                final String fileName = getConfigFileName(ilTableName, CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    String script = value.getScript();
                    // script = script.replace(",", "\n"); // TODO Note in many of them it has to go Here it is commented

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", schemaName); // Note Schema Name
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    finalResults.add(resultRow);
                    writeToFile(script, fileName);

                }

                // System.out.println("  Trans SRC to IL script fileName: " + fileName);

                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable, connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }
            }
        }
        /* Dimension Deletes Config */
        void dimDeleteConfigScript(Connection connection, String selectiveTables, 
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            
            System.out.println("..Dimension Deletes Config..");
            String dimensionTransaction = "D";
            String jobType = "Deletes_Dim";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);
            System.out.println("            limitFunct: " + limitFunct);

            // TODO: it is different in two cases based on dimensionTransaction
            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            Map<String, Map<String, String>> groupedScripts = new HashMap<>();
            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }

                // Delete ilTable + " _Stg_Keys" 
                deleteConfigProperties(connection, ilTable + " _Stg_Keys", connectionId);
                Boolean deleteFlag = getDeleteFlag(ilTableName);
                if (deleteFlag != true) {
                    System.out.println("    Delete Flag is false for ilTable: " + ilTable + ". Hence, skipping.");
                    continue;
                }

                Map<String, AggregatedData> mainDataMap = processConfigJobProperties(connection, connectionId, tableSchema, jobType);

                final String fileName = getConfigFileName(ilTableName, DELETES_CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("Key: " + key);
                    // the only aggregated field
                    final String script = value.getScript();

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", tableSchema);
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    // resultRow.put("Script", script);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }
                System.out.println("    deletes script fileName: " + fileName);

                // insertIntoEltValuesProperties(conn, finalResults);
                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable + "_Deletes", connectionId);
                    saveDataIntoDB(connection, tableName, finalResults);
                }

            }
        }
        /* Transaction Deletes Config */
        void transDeleteConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("..Transaction Deletes Config..");
            String dimensionTransaction = "T";
            String jobType = "Deletes_Trans";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);
            System.out.println("            limitFunct: " + limitFunct);

            // TODO: it is different in two cases based on dimensionTransaction
            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            Map<String, Map<String, String>> groupedScripts = new HashMap<>();
            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }
                
                // Delete ilTable + " _Stg_Keys" 
                deleteConfigProperties(connection, ilTable + " _Stg_Keys", connectionId); // Note the space in the effective ilTableName
                Boolean deleteFlag = getDeleteFlag(ilTableName);
                if (deleteFlag != true) {
                    System.out.println("    Delete Flag is false for ilTable: " + ilTable + ". Hence, skipping.");
                    continue;
                }

                Map<String, AggregatedData> mainDataMap = processConfigJobProperties(connection, connectionId,
                        tableSchema, jobType);

                final String fileName = getConfigFileName(ilTableName, DELETES_CONFIG_FILE_STRING);
                String addedUser = userName;
                Timestamp addedDate = Timestamp.valueOf(startTime);
                String updatedUser = userName;
                Timestamp updatedDate = Timestamp.valueOf(startTime);

                List<Map<String, Object>> finalResults = new ArrayList<>();
                for (Map.Entry<String, AggregatedData> entry : mainDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedData value = entry.getValue();
                    System.out.println("    Key: " + key);
                    // the only aggregated field
                    final String script = value.getScript();

                    Map<String, Object> resultRow = new HashMap<>();
                    resultRow.put("Connection_Id", connectionId);
                    resultRow.put("TABLE_SCHEMA", tableSchema);
                    resultRow.put("IL_Table_Name", ilTable);
                    resultRow.put("config_file_name", fileName);
                    resultRow.put("Active_Flag", true);
                    resultRow.put("Added_Date", addedDate);
                    resultRow.put("Added_User", addedUser);
                    resultRow.put("Updated_Date", updatedDate);
                    resultRow.put("Updated_User", updatedUser);
                    //resultRow.put("Script", script);
                    finalResults.add(resultRow);

                    writeToFile(script, fileName);
                }
                System.out.println("    deletes script fileName: " + fileName);

                // insertIntoEltValuesProperties(conn, finalResults);
                String tableName = "ELT_CONFIG_PROPERTIES";
                if (finalResults != null && !finalResults.isEmpty()) {
                    deleteConfigProperties(connection, ilTable + "_Deletes", connectionId); // TODO check it
                    saveDataIntoDB(connection, tableName, finalResults);
                }

            }
        }

        // With Write_Mode_Type
        private Map<String, AggregatedData> processSrcStgConfigJobProperties(Connection connection, String connectionId, String tableSchema, String jobType, String writeMode) {
            String query = "SELECT Id, Job_Type, Component, Key_Name, Value_Name, Active_Flag, Dynamic_Flag "
                    + "FROM ELT_Job_Properties_Info "
                    + "WHERE Job_Type = '" + jobType + "' AND Active_Flag = 1"
                    + " and Write_Mode_Type = '" + writeMode + "'";

            Map<String, AggregatedData> aggregatedDataMap = new HashMap<>();
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(query)) {

                while (resultSet.next()) {
                    String keyName = resultSet.getString("Key_Name");
                    String valueName = resultSet.getString("Value_Name");

                    // Transformation
                    String condition = (valueName != null && valueName.length() > 0) ? "=" : "";
                    String script = keyName + condition + valueName;

                    // IL_Table_Name transformation is done but not used in output
                    // config_file_name transformation is done but not used in output
                    // Some fields including above ones are common. Hence, updated once later.

                    // Aggregation
                    String key = connectionId + "-" + tableSchema + "-" + jobType;
                    AggregatedData aggregatedData = aggregatedDataMap.getOrDefault(key, new AggregatedData());
                    aggregatedData.addScript(script);
                    aggregatedDataMap.put(key, aggregatedData);
                }
            } catch (SQLException e) {
                System.err.println("Error processing config job properties: " + e.getMessage());
            }

            return aggregatedDataMap;
        }


        // With Write_Mode_Type +  Component not in ('sqlsink')
        private Map<String, AggregatedData> processSrcToIlConfigJobProperties(Connection connection, String connectionId, String tableSchema, String ilTableName, String jobType, String writeMode) {
            String query = "SELECT Id, Job_Type, Component, Key_Name, Value_Name, Active_Flag, Dynamic_Flag "
                    + "FROM ELT_Job_Properties_Info "
                    + "WHERE Job_Type = '" + jobType + "' AND Active_Flag = 1"
                    + " and Component not in ('sqlsink')"
                    + " and Write_Mode_Type = '" + writeMode + "'";

            String key = connectionId + "-" + tableSchema + "-" + ilTableName;
            Map<String, AggregatedData> aggregatedDataMap = new HashMap<>();
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(query)) {

                while (resultSet.next()) {
                    String keyName = resultSet.getString("Key_Name");
                    String valueName = resultSet.getString("Value_Name");

                    // Transformation
                    String condition = (valueName != null && valueName.length() > 0) ? "=" : "";
                    String script = keyName + condition + valueName;

                    // IL_Table_Name transformation is done but not used in output
                    // config_file_name transformation is done but not used in output
                    // Some fields including above ones are common. Hence, updated once later.

                    // Aggregation
                    AggregatedData aggregatedData = aggregatedDataMap.getOrDefault(key, new AggregatedData());
                    aggregatedData.addScript(script);
                    aggregatedDataMap.put(key, aggregatedData);
                }
            } catch (SQLException e) {
                System.err.println("Error processing config job properties: " + e.getMessage());
            }
            
            AggregatedData mainData = aggregatedDataMap.get(key);
            String finalScript = mainData.getScript();

            // subjob STG_To_IL_Lkp config
            String prevComponent = "Filter_By_Expression";
            Map<String, String> stgToILLookupConfigMap = getLookupConfig(connection, connectionId, ilTableName, querySchemaCondition, prevComponent);
            String theScriptLookup = stgToILLookupConfigMap.get("theScript");
            prevComponent = stgToILLookupConfigMap.get("prevComponent");

            // Add script to previous one
            if (theScriptLookup != null && !theScriptLookup.isEmpty()) {
                finalScript = finalScript + "\n" + theScriptLookup;
            }

            // subjob STG_To_IL_Sink config
            // key remains same 
            Map<String, AggregatedData> sinkConfigmap = getSinkConfig(connection, connectionId, tableSchema, ilTableName, jobType, writeMode, prevComponent);
            AggregatedData sinkData = sinkConfigmap.get(key);
            String theScriptSink = sinkData.getScript();
            
            // Add script to previous one
            if (theScriptSink != null && !theScriptSink.isEmpty()) {
                finalScript = finalScript + "\n" + theScriptSink;
            }
            AggregatedData finalAggregatedData = aggregatedDataMap.getOrDefault(key, new AggregatedData());
            finalAggregatedData.addScript(finalScript);
            aggregatedDataMap.put(key, finalAggregatedData);

            return aggregatedDataMap;
        }

        private Map<String, String> getLookupConfig(Connection connection, String connectionId, String ilTableName, String querySchemaCondition, String prevComponent) {
            String query = "SELECT " +
                           "IL_Table_Name, " +
                           "IL_Column_Name, " +
                           "Dimension_Key, " +
                           "Dimension_Name, " +
                           "Dimension_Join_Condition " +
                           "FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "WHERE Connection_Id = ? " +
                           "AND IL_Table_Name = ? " +
                           querySchemaCondition + " " +
                           "AND UPPER(Constraints) = 'FK'";
        
            Map<String, String> paramMap = new HashMap<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        
                preparedStatement.setString(1, connectionId);
                preparedStatement.setString(2, ilTableName);
                
                StringBuilder thelookupConfigBuilder = new StringBuilder();
                String sqllookupName = null;
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String dimensionName = resultSet.getString("Dimension_Name");
        
                        String sqllookupSource = (sqllookupName == null) 
                            ? prevComponent 
                            : sqllookupName;

                        // in previous statement, where is the 'sqllookupName' coming from?
                        sqllookupName = dimensionName.replace(" ", "_") + "_Lkp";
                        String sqllookupTable = "${" + sqllookupName + ".lookup.table}";
                        String sqllookupWhereFields = "${" + sqllookupName + ".where.fields}";
                        String sqllookupWhereColumns = "${" + sqllookupName + ".where.columns}";
                        String sqllookupSelectColumns = "${" + sqllookupName + ".select.columns}";

                        String thelookupConfig = "sqllookup\n" +
                            "sqllookup.name=" + sqllookupName + "\n" +
                            "sqllookup.source=" + sqllookupSource + "\n" +
                            "sqllookup.jdbc.url=${src.jdbc.url}\n" +
                            "sqllookup.jdbc.driver=${src.jdbc.driver}\n" +
                            "sqllookup.user.name=${src.db.user}\n" +
                            "sqllookup.password=${src.db.password}\n" +
                            "sqllookup.table=" + sqllookupTable + "\n" +
                            "sqllookup.select.columns=" + sqllookupSelectColumns + "\n" +
                            "sqllookup.where.fields=" + sqllookupWhereFields + "\n" +
                            "sqllookup.where.columns=" + sqllookupWhereColumns + "\n" +
                            "sqllookup.on.zero.fetch=IGNORE\n" +
                            "sqllookup.cache.type=${cache.type}\n" +
                            "sqllookup.cache.mode=${cache.mode}\n" +
                            "sqllookup.cache.max.elements.in.memory=${max.elements.in.memory}\n" +
                            "sqllookup.cache.time.to.idle.seconds=${time.to.idle.seconds}\n" +
                            "sqllookup.eoc";
                            // Aggregate 'thelookupConfig' param
                            if (thelookupConfigBuilder.length() > 0) {
                                thelookupConfigBuilder.append("\n");
                            }
                            thelookupConfigBuilder.append(thelookupConfig);
                    }
                    // Output
                    if (thelookupConfigBuilder.toString().isEmpty()) {
                        paramMap.put("prevComponent", "LKP_Stg_Keys");
                        paramMap.put("theScript", "");
                    } else {
                        paramMap.put("prevComponent", thelookupConfigBuilder.toString());
                        paramMap.put("theScript", sqllookupName);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("Error while fetching IL source mapping info", e);
            }
            return paramMap;
        }
             
        private Map<String, AggregatedData> getSinkConfig(Connection connection, String connectionId, String tableSchema, String ilTableName, String jobType, String writeMode, String prevComponent) {
            String query = "SELECT Id, Job_Type, Component, Key_Name, Value_Name, Active_Flag, Dynamic_Flag "
                    + "FROM ELT_Job_Properties_Info "
                    + "WHERE Job_Type = '" + jobType + "' AND Active_Flag = 1"
                    + " and Component in ('sqlsink')"
                    + " and Write_Mode_Type = '" + writeMode + "'";

            Map<String, AggregatedData> aggregatedDataMap = new HashMap<>();
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(query)) {

                while (resultSet.next()) {
                    String keyName = resultSet.getString("Key_Name");
                    String valueName = resultSet.getString("Value_Name");

                    // Transformation
                    String condition = (valueName != null && valueName.length() > 0) ? "=" : "";
                    String script;
                    if (keyName.equals("sqlsink.source")) {
                        script = keyName + condition + prevComponent;
                    } else {
                        script = keyName + condition + valueName;
                    }
                    // IL_Table_Name transformation is done but not used in output
                    // config_file_name transformation is done but not used in output
                    // above fields are common. Hence, updated once later.

                    // Aggregation
                    String key = connectionId + "-" + tableSchema + "-" + ilTableName;
                    AggregatedData aggregatedData = aggregatedDataMap.getOrDefault(key, new AggregatedData());
                    script = script.replace(",", "\n"); // TODO isn't duplicate
                    aggregatedData.addScript(script);
                    aggregatedDataMap.put(key, aggregatedData);
                }
            } catch (SQLException e) {
                System.err.println("Error processing config job properties: " + e.getMessage());
            }

            return aggregatedDataMap;
        }
        private Map<String, AggregatedData> processConfigJobProperties(Connection connection, String connectionId, String tableSchema, String jobType) {
            String query = "SELECT Id, Job_Type, Component, Key_Name, Value_Name, Active_Flag, Dynamic_Flag "
                    + "FROM ELT_Job_Properties_Info "
                    + "WHERE Job_Type = '" + jobType + "' AND Active_Flag = 1";

            Map<String, AggregatedData> aggregatedDataMap = new HashMap<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(query)) {

                while (resultSet.next()) {
                    String keyName = resultSet.getString("Key_Name");
                    String valueName = resultSet.getString("Value_Name");

                    // Transformation
                    String condition = (valueName != null && valueName.length() > 0) ? "=" : "";
                    String script = keyName + condition + valueName;

                    // IL_Table_Name transformation is done but not used in output
                    // config_file_name transformation is done but not used in output
                    // Some fields including above ones are common. Hence, updated once later.

                    // Aggregation
                    String key = connectionId + "-" + tableSchema + "-" + jobType;
                    AggregatedData aggregatedData = aggregatedDataMap.getOrDefault(key, new AggregatedData());
                    aggregatedData.addScript(script);
                    aggregatedDataMap.put(key, aggregatedData);
                }
            } catch (SQLException e) {
                System.err.println("Error processing config job properties: " + e.getMessage());
            }

            return aggregatedDataMap;
        }

        // AggregatedData class for handling aggregation
        class AggregatedData {
            private StringBuilder script = new StringBuilder();

            public void addScript(String newScript) {
                if (script.length() > 0) {
                    script.append("\n");
                }
                script.append(newScript);
            }

            public String getScript() {
                return script.toString();
            }
        }

        private void deleteConfigProperties(Connection connection, String ilTable, String connectionId) {
            String query = "DELETE FROM ELT_CONFIG_PROPERTIES WHERE IL_Table_Name = '" 
                           + ilTable + "' AND Connection_Id = '" + connectionId + "'";
            try (Statement statement = connection.createStatement()) {
                int rowsDeleted = statement.executeUpdate(query);
                System.out.println(rowsDeleted + " rows deleted from ELT_CONFIG_PROPERTIES where IL_Table_Name is " + ilTable);
            } catch (SQLException e) {
                System.err.println("Error while deleting from ELT_CONFIG_PROPERTIES: " + e.getMessage());
            }
        }

    }
    
    public class DWValueScriptsGenerator {
        private static final String DELETES_VALUE_FILE_STRING = "_Deletes_Value_File_";
        private static final String STG_VALUE_FILE_STRING = "_Stg_Value_File_";
        private static final String VALUE_FILE_STRING = "_Value_File_";
        private static final String STG_KEYS_VALUE_FILE_STRING = "_Stg_Keys_Value_File_";

        String srcHost;
        String srcPort;
        String srcUn;
        String srcPwd;
        String srcDbName;
        String tgtHost;
        String tgtPort;
        String tgtUn;
        String tgtPwd;
        String tgtDbName;
        String limitFunction;
        Map<String, Map<String, String>> datatypeConversionsGlobal;
        // Constructor
        public DWValueScriptsGenerator() {
            initValueScript();
        }
    
        private void initValueScript() {
            JSONObject jsonDbDetails = new JSONObject(dbDetails);
            System.out.println(jsonDbDetails);
            srcHost = jsonDbDetails.getString("stagingdb_hostname");
            srcPort = jsonDbDetails.getString("stagingdb_port");
            srcUn = jsonDbDetails.getString("stagingdb_username");
            srcPwd = jsonDbDetails.getString("stagingdb_password");
            srcPwd = srcPwd.replaceAll("\\$", "\\\\\\$");
            srcDbName = jsonDbDetails.getString("stagingdb_schema");

            tgtHost = jsonDbDetails.getString("datadb_hostname");
            tgtPort = jsonDbDetails.getString("datadb_port");
            tgtUn = jsonDbDetails.getString("datadb_username");
            tgtPwd = jsonDbDetails.getString("datadb_password");
            tgtPwd = tgtPwd.replaceAll("\\$", "\\\\\\$");
            tgtDbName = jsonDbDetails.getString("datadb_schema");

            // limitFunction is not applicable for Value scripts
            limitFunction = "";
            // To make single call of the same
            datatypeConversionsGlobal = getDatatypeConversions(conn);
        }
        // Method to generate the value script
        public Status generateValueScript() {
            System.out.println("### Generating Value Scripts ...");
            try {
                // datatypeConversionsGlobal is used at many places
                // Map<String, Map<String, String>> datatypeConversionsGlobal = getDatatypeConversions(conn);

                // Total 7 sub parts
                // Dim_SRC_STG
                dimSourceToStagingValuesScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

                // Dim_STG_IL
                dimStageToDWValuesScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

                // Trans_SRC_STG
                transSourceToStagingValuesScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);


                // Trans_STG_Keys
                transStagingToStagingKeysValuesScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

                // Trans_STG_IL

                transStageToDWValuesScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

                // Deletes_Delete
                dimDeleteScriptComplete(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

                // Trans_deletes
                transDeleteScriptComplete(conn, selectTables, connectionId, querySchemaCondition, limitFunction);

            } catch (SQLException e) {
                e.printStackTrace();
                return Status.FAILURE;
            }

                

            return Status.SUCCESS;
        }

        /* Dimension Source to Staging Values script */
        void dimSourceToStagingValuesScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Dimension Source To Staging Values..");

            String dimensionTransaction = "D";
            String jobType = "Dimension_src_stg";

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                System.out.println("    iLTableName: " + ilTableName);

                String fileName = getValueFileName(ilTableName, STG_VALUE_FILE_STRING);
                // Delete ilTableName + "_Stg_Keys" Value Properties
                deleteValuesProperties(connection, ilTableName + "_Stg_Keys", connectionId);
                String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
                if (dateFormat == null) {
                    dateFormat = "yyyy-MM-dd";
                }
                long connectionType =  getIsWebService(connection, ilTableName, connectionId, schemaName);
                String writeMode = getWriteMode(ilTableName);
                String lhs = getAggregatedValueNames(connection, jobType, writeMode);
                System.out.println("    lhs: " + lhs);

                Map<String, DimSourceValues> mainData = getDimensionSourceMappingInfo(connection, dimensionTransaction, querySchemaCond, connectionId, ilTableName);
                System.out.println("mainData: " + mainData.size());

                Map<String, Map<String, String>> constantDataMap = getConstantInsertDataMap(
                    connection, dimensionTransaction, ilTableName, connectionId, querySchemaCond); 

                List<Map<String, Object>> finalData = dimSourceToStagingValuesProcess(mainData, constantDataMap, lhs);
                // Update DB
                if (finalData != null && !finalData.isEmpty()) {
                    deleteValuesProperties(connection, ilTableName + "_Stg", connectionId);
                    insertIntoEltValuesProperties(conn, finalData);
                }
            }

        }

        private List<Map<String, Object>> dimSourceToStagingValuesProcess(Map<String, DimSourceValues> mainData, Map<String, Map<String, String>> constantDataMap, String lhs) {
            // String lhs = (String) globalMap.get("lhs");
            // String srcPwd = (String) globalMap.get("src_pwd");
            // String srcHost = context.SRC_HOST;
            // String srcPort = context.SRC_PORT;
            // String srcDbName = context.SRC_DBNAME;
            // String srcUn = context.SRC_UN;
            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            // Iterate over the map
            for (Map.Entry<String, DimSourceValues> entry : mainData.entrySet()) {
                String key = entry.getKey();
                DimSourceValues dimSourceValues = entry.getValue();
                final String tableSchema = dimSourceValues.getTableSchema();
                String ilTableName = dimSourceValues.getIlTableName();

                // Left Outer Join
                Map<String, String> constantInsertData = constantDataMap.getOrDefault(ilTableName, new HashMap<>()); // key is ilTableName
                String constantFields = constantInsertData.get("ConstantFields");
                String constantValues = constantInsertData.get("ConstantValues");
                String constantTypes = constantInsertData.get("ConstantTypes");

                // Transformations
                String var1 = lhs.replaceAll("\\$\\{sources3csv.aws.access.key.id}", "sources3csv.aws.access.key.id=");
                String var2 = var1.replaceAll("\\$\\{sources3csv.aws.secret.access.key}", "sources3csv.aws.secret.access.key=");
                String var3 = var2.replaceAll("\\$\\{sources3csv.bucket}", "sources3csv.bucket=");
                String var4 = var3.replaceAll("/\\$\\{stg.source.path}", "stg.source.path=");
                String var5 = var4.replaceAll("\\$\\{mapping.constants.fields}", "mapping.constants.fields=" + constantFields);
                String var6 = var5.replaceAll("\\$\\{mapping.constants.fields.types}", "mapping.constants.fields.types=" + constantTypes);
                String var7 = var6.replaceAll("\\$\\{mapping.constants.fields.values}", "mapping.constants.fields.values=" + constantValues);
                String varPosition = var7.replaceAll("\\$\\{mapping.constants.fields.positions}", "mapping.constants.fields.positions=''");
                String var8 = varPosition.replaceAll("\\$\\{class.names}", "class.names=com.anvizent.elt.anvizent.util.ChecksumGenerator");
                String var9 = var8.replaceAll("\\$\\{method.names}", "method.names=generate");
                String var10 = var9.replaceAll("\\$\\{method.argument.fields}", "method.argument.fields=\"" + dimSourceValues.getAllCols().replaceAll("\\$", "\\\\\\$") + "\"");
                String var11 = var10.replaceAll("\\$\\{return.fields}", "return.fields=Source_Hash_Value");
                String var12 = var11.replaceAll("\\$\\{source.coerce.fields}", "source.coerce.fields=" + dimSourceValues.getAllCols().replaceAll("\\$", "\\\\\\$"));
                String var13 = var12.replaceAll("\\$\\{source.coerce.to}", "source.coerce.to=" + dimSourceValues.getAllJavaDataTypes());
                String var14 = var13.replaceAll("\\$\\{source.coerce.format}", "source.coerce.format=" + dimSourceValues.getAllDateformats());
                String var15 = var14.replaceAll("\\$\\{src.jdbc.url}", "src.jdbc.url=jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDbName);
                String var16 = var15.replaceAll("\\$\\{src.jdbc.driver}", "src.jdbc.driver=com.mysql.jdbc.Driver");
                String var17 = var16.replaceAll("\\$\\{src.db.user}", "src.db.user=" + srcUn);
                String srcPwdEscaped = srcPwd.replaceAll("\\$", "\\$");
                String var18 = var17.replaceAll("\\$\\{src.db.password}", "src.db.password=" + srcPwdEscaped);
                String var19 = var18.replaceAll("\\$\\{target.table}", "target.table=" + dimSourceValues.getStgTableName().replaceAll("\\$", "\\\\\\$"));
                String var20 = var19.replaceAll("\\$\\{key.fields}", "key.fields=" + dimSourceValues.getAllPk());
                String var21 = var20.replaceAll("\\$\\{key.columns}", "key.columns=" + (dimSourceValues.getAllPk() == null ? dimSourceValues.getAllPk() : dimSourceValues.getAllPk().replaceAll("\\$", "\\\\\\$")));
                //String var21 = var20.replaceAll("\\$\\{key.columns}", "key.columns=" + dimSourceValues.getAllPk().replaceAll("\\$", "\\\\\\$"));
                String var22 = var21.replaceAll("\\$\\{key.fields.case.sensitive}", "key.fields.case.sensitive=TRUE");
                String var23 = var22.replaceAll("\\$\\{insert.constant.columns}", "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                String var24 = var23.replaceAll("\\$\\{insert.constant.store.values}", "insert.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin',UTC_TIMESTAMP(),'ELT_Admin'");
                String var25 = var24.replaceAll("\\$\\{insert.constant.store.types}", "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");
                String var26 = var25.replaceAll("\\$\\{update.constant.columns}", "update.constant.columns=Updated_Date,Updated_User");
                String var27 = var26.replaceAll("\\$\\{update.constant.store.values}", "update.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin'");
                String var28 = var27.replaceAll("\\$\\{update.constant.store.types}", "update.constant.store.types=java.util.Date,java.lang.String");
                String var29 = var28.replaceAll("\\$\\{batch.type}", "batch.type=BATCH_BY_SIZE");
                String var30 = var29.replaceAll("\\$\\{batch.size}", "batch.size=10000");
                String var31 = var30.replaceAll("\\$\\{cleansing.fields}", "cleansing.fields=" + (dimSourceValues.getAllPk() == null ? "DataSource_Id" : dimSourceValues.getAllPk()));
                String var32 = var31.replaceAll("\\$\\{cleansing.values}", "cleansing.values=" + (dimSourceValues.getCleansingValues() == null ? "N/A" : dimSourceValues.getCleansingValues()));
                String var33 = var32.replaceAll("\\$\\{date.formats}", "date.formats=" + (dimSourceValues.getPkDateformats() == null ? "" : dimSourceValues.getPkDateformats()));
                String var34 = var33.replaceAll("\\$\\{cleansing.validation}", "cleansing.validation=" + (dimSourceValues.getCleansingValidations() == null || dimSourceValues.getCleansingValidations().isEmpty() ? "EMPTY" : dimSourceValues.getCleansingValidations()));
        
                // Final result
                String valueFile = var34;
                String fileName = getValueFileName(ilTableName, STG_VALUE_FILE_STRING);
                writeToFile(valueFile, fileName);

                ilTableName = ilTableName + "_Stg";  // Updated Value

                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);

                System.out.println("Final Result: valueFile \n" + valueFile);
            }

            return finalResults;
        }

        private List<Map<String, Object>> transSourceToStagingValuesProcess(Map<String, DimSourceValues> mainData, Map<String, Map<String, String>> constantDataMap, String lhs) {

            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            // Iterate over the map
            for (Map.Entry<String, DimSourceValues> entry : mainData.entrySet()) {
                String key = entry.getKey();
                DimSourceValues dimSourceValues = entry.getValue();
                final String tableSchema = dimSourceValues.getTableSchema();
                String ilTableName = dimSourceValues.getIlTableName();

                // Left Outer Join
                Map<String, String> constantInsertData = constantDataMap.getOrDefault(ilTableName, new HashMap<>()); // key is ilTableName
                String constantFields = constantInsertData.get("ConstantFields");
                String constantValues = constantInsertData.get("ConstantValues");
                String constantTypes = constantInsertData.get("ConstantTypes");

                // Transformations
                // srcHost, srcPort, srcDbName, srcUn, srcPwd, tgtPwd etc. have been initialized in initValueScript()
                String var21 = lhs.replaceAll("\\$\\{sources3csv.aws.access.key.id}", "aws.access.key.id=");
                String var22 = var21.replaceAll("\\$\\{sources3csv.aws.secret.access.key}", "sources3csv.aws.secret.access.key=");
                String var23 = var22.replaceAll("\\$\\{sources3csv.bucket}", "sources3csv.bucket=");
                String var1 = var23.replaceAll("/\\$\\{stg.source.path}", "stg.source.path=");
                String var2 = var1.replaceAll("\\$\\{mapping.constants.fields}", "mapping.constants.fields=" + constantFields);
                String var3 = var2.replaceAll("\\$\\{mapping.constants.fields.types}", "mapping.constants.fields.types=" + constantTypes);
                String var4 = var3.replaceAll("\\$\\{mapping.constants.fields.values}", "mapping.constants.fields.values=" + constantValues);
                String var5 = var4.replaceAll("\\$\\{mapping.constants.fields.positions}", "mapping.constants.fields.positions=");
                String var6 = var5.replaceAll("\\$\\{source.coerce.fields}", "source.coerce.fields=" + dimSourceValues.getAllCols().replaceAll("\\$", "\\\\\\$"));
                String var7 = var6.replaceAll("\\$\\{source.coerce.to}", "source.coerce.to=" + dimSourceValues.getAllJavaDataTypes());
                String var8 = var7.replaceAll("\\$\\{source.coerce.format}", "source.coerce.format=" + dimSourceValues.getAllDateformats());
                String var9 = var8.replaceAll("\\$\\{src.jdbc.url}", "src.jdbc.url=jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDbName);
                String var10 = var9.replaceAll("\\$\\{src.jdbc.driver}", "src.jdbc.driver=com.mysql.jdbc.Driver");
                String var11 = var10.replaceAll("\\$\\{src.db.user}", "src.db.user=" + srcUn);
                String var12 = var11.replaceAll("\\$\\{src.db.password}", "src.db.password=" + srcPwd);
                String var13 = var12.replaceAll("\\$\\{target.table}", "target.table=" + dimSourceValues.getStgTableName().replaceAll("\\$", "\\\\\\$"));
                String var14 = var13.replaceAll("\\$\\{key.fields}", "key.fields=" + dimSourceValues.getAllPk());
                String var15 = var14.replaceAll("\\$\\{key.columns}", "key.columns=" + dimSourceValues.getAllPk());
                String var16 = var15.replaceAll("\\$\\{constant.columns}", "constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                String var17 = var16.replaceAll("\\$\\{constant.store.values}", "constant.store.values=UTC_TIMESTAMP(),'ELT_Admin',UTC_TIMESTAMP(),'ELT_Admin'");
                String var18 = var17.replaceAll("\\$\\{constant.store.types}", "constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");
                String var19 = var18.replaceAll("\\$\\{batch.type}", "batch.type=BATCH_BY_SIZE");
                String var20 = var19.replaceAll("\\$\\{batch.size}", "batch.size=10000");

                // Final result
                String valueFile = var20;
                String fileName = getValueFileName(ilTableName, STG_VALUE_FILE_STRING);
                writeToFile(valueFile, fileName);

                ilTableName = ilTableName + "_Stg";  // Updated Value

                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);
            }

            return finalResults;
        }

        /* Transaction Source To Staging Values script */
        void transSourceToStagingValuesScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Transaction Source To Staging Values..");

            String dimensionTransaction = "T";
            String jobType = "Transaction_src_stg";

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
            dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            for (Map<String, String> map : data) {
                String ilTableName = map.get("IL_Table_Name");
                System.out.println("    iLTableName: " + ilTableName);

                String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
                if (dateFormat == null) {
                    dateFormat = "yyyy-MM-dd";
                }
                System.out.println("dateFormat: " + dateFormat);
                long connectionType =  getIsWebService(connection, ilTableName, connectionId, schemaName);
                System.out.println("connectionType: " + connectionType);
                String query = "";
                if (connectionType == 1) {
                    query = "SELECT time_zone FROM minidwcs_database_connections WHERE Connection_Id = ?";
                }
                else if (connectionType == 0) {
                    query = "SELECT time_zone FROM minidwcs_ws_connections_mst WHERE Id = ?";
                }
                else {
                    System.out.println("connectionType: " + connectionType +  " is not supported. skipping the ilTableName: " + ilTableName);
                    continue;
                }

                TimeZone timeZone = getTimeZone(connection, connectionId, query);
//                System.out.println("timeZone: " + timeZone.toString());

                String writeMode = getWriteMode(ilTableName);
                String lhs = getAggregatedValueNames(connection, jobType, writeMode);
                System.out.println("    lhs: " + lhs);

                Map<String, DimSourceValues> mainData = getTransactionSourceMappingInfo(connection, dimensionTransaction, querySchemaCond, connectionId, ilTableName);
                System.out.println("mainData: " + mainData.size());
                // Similar for Dimension
                Map<String, Map<String, String>> constantDataMap = getConstantInsertDataMap(
                    connection, dimensionTransaction, ilTableName, connectionId, querySchemaCond);

                List<Map<String, Object>> finalData = transSourceToStagingValuesProcess(mainData, constantDataMap, lhs);
                // Update DB
                if (finalData != null && !finalData.isEmpty()) {
                    deleteValuesProperties(connection, ilTableName + "_Stg", connectionId);
                    insertIntoEltValuesProperties(conn, finalData);
                }
            }

        }

        /* Transaction Stage keys Values script */
        void transStagingToStagingKeysValuesScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Transaction Staging to Staging Keys Value..");

            String dimensionTransaction = "T";
            String jobType = "Transaction_stg_keys";

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
            dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);
            for (Map<String, String> map : data) {
                String ilTableName = map.get("IL_Table_Name");
                System.out.println("    iLTableName: " + ilTableName);

                // filename _Stg_Keys_Value_File_
                // String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
                // if (dateFormat == null) {
                //     dateFormat = "yyyy-MM-dd";
                // }
                // System.out.println("dateFormat: " + dateFormat);
                long connectionType =  getIsWebService(connection, ilTableName, connectionId, schemaName);
                System.out.println("connectionType: " + connectionType);
                String query = "";
                if (connectionType == 1) {
                    query = "SELECT time_zone FROM minidwcs_database_connections WHERE Connection_Id = ?";
                }
                else if (connectionType == 0) {
                    query = "SELECT time_zone FROM minidwcs_ws_connections_mst WHERE Id = ?";
                }
                else {
                    System.out.println("connectionType: " + connectionType +  " is not supported. skipping the ilTableName: " + ilTableName);
                    continue;
                }

                TimeZone timeZone = getTimeZone(connection, connectionId, query);

                String writeMode = getWriteMode(ilTableName);
                String lhs = getAggregatedValueNames(connection, jobType, writeMode);
                // System.out.println("    lhs: " + lhs);
                List<Map<String, Object>> finalData = getStagingHashcolumnPick(conn, ilTableName, connectionId, querySchemaCond, writeMode, lhs);

                // Update DB
                if (finalData != null && !finalData.isEmpty()) {
                    deleteValuesProperties(connection, ilTableName + "_Stg_Keys", connectionId);
                    insertIntoEltValuesProperties(conn, finalData);
                }

            }

        }

        /**
         * Fetches staging hash column pick details from the `stg_hashcolumn_pick` table.
         *
         * @param conn            The database connection object.
         * @param tableName       The name of the table 
         * @param connectionId    The connection ID
         * @param querySchemaCond Additional schema condition
         * @return A list of maps containing the query results, where each map represents a row.
         */
        public List<Map<String, Object>> getStagingHashcolumnPick(Connection conn, String tableName, String connectionId, String querySchemaCond, String writeMode, String lhs) {
            List<Map<String, Object>> finalResults = new ArrayList<>();
            String query = "SELECT Table_Name, PK_Columns, Hash_Columns, " +
                           "Tilt_PK_Columns, Tilt_Hash_Columns, " +
                           "ROUND((LENGTH(PK_Columns) - LENGTH(REPLACE(PK_Columns, ',', ''))) / LENGTH(',')) AS Comma_Count, " +
                           "Date_Formats, precisions, scales, Datatypes " +
                           "FROM stg_hashcolumn_pick " +
                           "WHERE Table_Name = ? AND Connection_Id = ? " + querySchemaCond;
        
            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, tableName + "_Stg");
                pstmt.setString(2, connectionId);
        
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String tableNameValue = rs.getString("Table_Name");
                        String pkColumns = rs.getString("PK_Columns");
                        String hashColumns = rs.getString("Hash_Columns");
                        String tiltPKColumns = rs.getString("Tilt_PK_Columns");
                        String tiltHashColumns = rs.getString("Tilt_Hash_Columns");
                        String commaCount = rs.getString("Comma_Count");
                        String dateFormats = rs.getString("Date_Formats");
                        String precisions = rs.getString("precisions");
                        String scales = rs.getString("scales");
                        String datatypes = rs.getString("Datatypes");


                        String var1 = (lhs).replaceAll("\\$\\{src\\.jdbc\\.url}", 
                        "src.jdbc.url=jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDbName);
                        String var2 = var1.replaceAll("\\$\\{src\\.jdbc\\.driver}", 
                        "src.jdbc.driver=com.mysql.jdbc.Driver");
                        String var3 = var2.replaceAll("\\$\\{src\\.db\\.user}", 
                        "src.db.user=" + srcUn);
                        String srcPwd = (this.srcPwd).replaceAll("\\$", "\\\\\\$");
                        String var4 = var3.replaceAll("\\$\\{src\\.db\\.password}", 
                        "src.db.password=" + srcPwd);

                        // Replacing parts of query using dynamic values
                        String stgKeysTiltPkColumns = tiltPKColumns.replaceAll("\\$", "\\\\\\$");
                        String stgKeysTiltHashColumns = tiltHashColumns != null ? tiltHashColumns.replaceAll("\\$", "\\\\\\$") : null ;
                        String stgKeysTableName = tableNameValue.replaceAll("\\$", "\\\\\\$");
                        String sourceQuery = "source.query=Select " + stgKeysTiltPkColumns + "," + stgKeysTiltHashColumns +
                        " FROM `" + stgKeysTableName + "`";
                        String var5 = var4.replaceAll("\\$\\{source\\.query}", sourceQuery);

                        String var6 = var5.replaceAll("\\$\\{source\\.is\\.query}", "source.is.query=True");
                        String var7 = var6.replaceAll("\\$\\{class\\.names}", 
                        "class.names=java.lang.String,com.anvizent.elt.anvizent.util.ChecksumGenerator");
                        String var8 = var7.replaceAll("\\$\\{method\\.names}", "method.names=join,generate");

                        String stgKeysPkColumns = pkColumns != null ? pkColumns.replaceAll("\\$", "\\\\\\$") : null;
                        String stgKeysHashColumns = hashColumns != null ? hashColumns.replaceAll("\\$", "\\\\\\$") : null;
                        String methodArgumentFields = "method.argument.fields=\"underscore_field," + stgKeysPkColumns + "\",\"" + stgKeysHashColumns + "\"";
                        String var9 = var8.replaceAll("\\$\\{method\\.argument\\.fields}", methodArgumentFields);

                        String var10 = var9.replaceAll("\\$\\{return\\.fields}", "return.fields=PKValue,HashValue");
                        String var11 = var10.replaceAll("\\$\\{source\\.coerce\\.fields}", 
                        "source.coerce.fields=" + pkColumns);

                        String varGenerateA = generateText("a", Integer.valueOf(commaCount) + 1);
                        String replaceA = varGenerateA.replaceAll("a", "java.lang.String,");
                        String trim = replaceA.substring(0, replaceA.length() - 1);
                        String var12 = var11.replaceAll("\\$\\{source\\.coerce\\.to}", "source.coerce.to=" + trim);

                        String var13 = var12.replaceAll("\\$\\{target\\.table}", 
                        "target.table=" + tableNameValue + "_Keys".replaceAll("\\$", "\\\\\\$"));
                        String var14 = var13.replaceAll("\\$\\{key\\.fields}", "key.fields=PKValue");
                        String var15 = var14.replaceAll("\\$\\{key\\.columns}", "key.columns=PKValue");
                        String var16 = var15.replaceAll("\\$\\{key\\.fields\\.case\\.sensitive}", "key.fields.case.sensitive=True");
                        String var17 = var16.replaceAll("\\$\\{key\\.columns\\.case\\.sensitive}", "key.columns.case.sensitive=True");
                        String var18 = var17.replaceAll("\\$\\{insert\\.constant\\.columns}", 
                        "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                        String var19 = var18.replaceAll("\\$\\{insert\\.constant\\.store\\.values}", 
                        "insert.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin',UTC_TIMESTAMP(),'ELT_Admin'");
                        String var20 = var19.replaceAll("\\$\\{insert\\.constant\\.store\\.types}", 
                        "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");

                        String var21 = var20.replaceAll("\\$\\{update\\.constant\\.columns}", 
                        "update.constant.columns=Updated_Date,Updated_User");
                        String var22 = var21.replaceAll("\\$\\{update\\.constant\\.store\\.values}", 
                        "update.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin'");
                        String var23 = var22.replaceAll("\\$\\{update\\.constant\\.store\\.types}", 
                        "update.constant.store.types=java.util.Date,java.lang.String");

                        String var24 = var23.replaceAll("\\$\\{batch\\.type}", "batch.type=BATCH_BY_SIZE");
                        String var25 = var24.replaceAll("\\$\\{batch\\.size}", "batch.size=25000");

                        String var26 = var25.replaceAll("\\$\\{mapping\\.constants\\.fields}", 
                        "mapping.constants.fields=underscore_field");
                        String var27 = var26.replaceAll("\\$\\{mapping\\.constants\\.fields\\.types}", 
                        "mapping.constants.fields.types=java.lang.String");
                        String var28 = var27.replaceAll("\\$\\{mapping\\.constants\\.fields\\.values}", 
                        "mapping.constants.fields.values=_");

                        String var29 = var28.replaceAll("\\$\\{mapping\\.retain\\.emit}", 
                        "mapping.retain.emit=underscore_field," + stgKeysPkColumns + "," + stgKeysHashColumns);

                        String varGenerateA1 = generateText("a", Integer.valueOf(commaCount));
                        String replaceA1 = varGenerateA1.replaceAll("a", ",");
                        String trim1 = replaceA1; // No length manipulation here
                        String var30 = var29.replaceAll("\\$\\{source\\.coerce\\.format}", 
                        "source.coerce.format=" + dateFormats);

                        String var31 = var30.replaceAll("\\$\\{checksum\\.fields}", "checksum.fields=HashValue");
                        String var32 = var31.replaceAll("\\$\\{columns}", "columns=" + stgKeysTiltPkColumns + "," + stgKeysTiltHashColumns);
                        String var33 = var32.replaceAll("\\$\\{table\\.name}", 
                        "table.name=`" + stgKeysTableName + "`");
                        String var34 = var33.replaceAll("\\$\\{empty\\.coerce\\.back}", "empty.coerce.back=" + datatypes);
                        String var35 = var34.replaceAll("\\$\\{empty\\.mapping\\.coerce\\.decimal\\.precisions}", 
                        "empty.mapping.coerce.decimal.precisions=" + precisions);
                        String var36 = var35.replaceAll("\\$\\{empty\\.mapping\\.coerce\\.decimal\\.scales}", 
                        "empty.mapping.coerce.decimal.scales=" + scales);
                        String var37 = var36.replaceAll("\\$\\{empty\\.coerce\\.fields}", 
                        "empty.coerce.fields=" + pkColumns);
                        String var38 = var37.replaceAll("\\$\\{empty\\.coerce\\.format}", 
                        "empty.coerce.format=" + dateFormats);

                        String whereCondition = writeMode.equals("overwrite") ? 
                        "where.condition=" : "where.condition=where Updated_Date>=${start.date}";
                        String incrementalCondition = var38.replaceAll("\\$\\{where\\.condition}", 
                        whereCondition.replaceAll("\\$", "\\\\\\$"));
                
                        // Final result
                        String valueFile = incrementalCondition;
                        String fileName = getValueFileName(tableName, STG_KEYS_VALUE_FILE_STRING);
                        writeToFile(valueFile, fileName);

                        String ilTableNameUpdated = tableNameValue + "_Keys";  // Updated Value

                        Map<String, Object> resultRow = new HashMap<>();
                        resultRow.put("Connection_Id", connectionId); // Global
                        resultRow.put("TABLE_SCHEMA", schemaName); // Global
                        resultRow.put("IL_Table_Name", ilTableNameUpdated);
                        resultRow.put("values_file_name", fileName);
                        resultRow.put("Active_Flag", true);
                        resultRow.put("Added_Date", addedDate);
                        resultRow.put("Added_User", addedUser);
                        resultRow.put("Updated_Date", updatedDate);
                        resultRow.put("Updated_User", updatedUser);
                        finalResults.add(resultRow);
                        // System.out.println("valueFile: \n" +  valueFile);

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return finalResults;
        }
        // Helper function to generate repeated Text
        String generateText(String str, int repeatCount) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < repeatCount; i++) {
                sb.append(str);
            }
            String repeatedString = sb.toString();
            return repeatedString;
        }

        /* Transaction Staging To DW Values */
        void dimStageToDWValuesScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Dimension Staging to DW Value..");
            
            String dimensionTransaction = "D";
            String jobType = "Dimension_stg_il";

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
            dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);


            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                System.out.println("    iLTableName: " + ilTableName);
                String fileName = getValueFileName(ilTableName, VALUE_FILE_STRING);

                String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
                if (dateFormat == null) {
                    dateFormat = "yyyy-MM-dd";
                }

                long connectionType =  getIsWebService(connection, ilTableName, connectionId, schemaName);
                System.out.println("connectionType: " + connectionType);
                String query = "";
                if (connectionType == 1) {
                    query = "SELECT time_zone FROM minidwcs_database_connections WHERE Connection_Id = ?";
                }
                else if (connectionType == 0) {
                    query = "SELECT time_zone FROM minidwcs_ws_connections_mst WHERE Id = ?";
                }
                else {
                    System.out.println("connectionType: " + connectionType +  " is not supported. skipping the ilTableName: " + ilTableName);
                    continue;
                }

                TimeZone timeZone = getTimeZone(connection, connectionId, query);
                // System.out.println("timeZone: " + timeZone.toString());
                String writeMode = getWriteMode(ilTableName);
                String lhs = getAggregatedValueNames(connection, jobType, writeMode);
                // System.out.println("    lhs: " + lhs);

                Map<String, DimSourceValues> mainData = getDimStageToDwSourceMappingInfo(connection, dimensionTransaction, querySchemaCond, connectionId, ilTableName);
                System.out.println("mainData: " + mainData.size());

                Map<String, SourceMapValues> constrainedData = getConstrainedFieldsAndValues(connection, dimensionTransaction, ilTableName, connectionId, querySchemaCondition);
                System.out.println("constrainedData: " + constrainedData);
                Map<String, SourceMapValues> nonConstrainedData = getNonConstrainedFieldsAndValues(connection, dimensionTransaction, ilTableName, connectionId, querySchemaCondition);
                System.out.println("c Non onstrainedData: " + constrainedData);

                Map<String, SourceMapValues> joinedData = joinConstrainedNonCnstrainedData(constrainedData, nonConstrainedData);
                List<Map<String, Object>> finalData = dimStageToDWValuesScriptFinalProcess( mainData, joinedData, lhs, writeMode);
                // Update DB
                if (finalData != null && !finalData.isEmpty()) {
                    deleteValuesProperties(connection, ilTableName, connectionId);
                    insertIntoEltValuesProperties(conn, finalData);
                }
            }

        }

        private Map<String, DimSourceValues> getDimStageToDwSourceMappingInfo(Connection conn,
                String dimensionTransaction, String querySchemaCond,
                String connectionId, String ilTableName) throws SQLException {

            Map<String, DimSourceValues> aggregatedSourceValues = new HashMap<>();

            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, "
             + "IL_Data_Type, Constraints, Source_Table_Name, Source_Column_Name, "
             + "LOWER(Source_Data_Type) AS Source_Data_Type_Lower, PK_Constraint, "
             + "PK_Column_Name, FK_Constraint, FK_Column_Name, Dimension_Transaction, "
             + "Dimension_Key, Dimension_Name, Dimension_Join_Condition, Active_Flag, "
             + "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS DataType "
             + "FROM ELT_IL_Source_Mapping_Info_Saved "
             + "WHERE Dimension_Transaction = ? AND IL_Table_Name = ? "
             + "AND Connection_Id = ? " + querySchemaCond;

            try {
                PreparedStatement pstmt = conn.prepareStatement(query);

                pstmt.setString(1, dimensionTransaction);
                pstmt.setString(2, ilTableName);
                pstmt.setString(3, connectionId);

                rs = pstmt.executeQuery();

                String cdcFlag = "";
                while (rs.next()) {
                    String connectionIdValue = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("TABLE_SCHEMA");
                    String ilTableNameValue = rs.getString("IL_Table_Name");
                    String ilColumnName = rs.getString("IL_Column_Name");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String constraints = rs.getString("Constraints");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String sourceColumnName = rs.getString("Source_Column_Name");
                    String sourceDataType = rs.getString("Source_Data_Type_Lower"); // alias used
                    String pkConstraint = rs.getString("PK_Constraint");
                    String pkColumnName = rs.getString("PK_Column_Name");
                    String fkConstraint = rs.getString("FK_Constraint");
                    String fkColumnName = rs.getString("FK_Column_Name");
                    String dimensionTransactionValue = rs.getString("Dimension_Transaction");
                    String dimensionKey = rs.getString("Dimension_Key");
                    String dimensionName = rs.getString("Dimension_Name");
                    String dimensionJoinCondition = rs.getString("Dimension_Join_Condition");
                    boolean activeFlag = rs.getBoolean("Active_Flag");
                    // String cdcFlag = rs.getString("CDC_Flag");
                    String dataType = rs.getString("Datatype");

                    String var1 = "`" + ilColumnName + "`"; // Not used
                    if (constraints == null) {
                        constraints = "";
                    }
                    // the left join with datatypeConversions
                    Map<String, String> conversionDetails = datatypeConversionsGlobal.getOrDefault(dataType, new HashMap<>());
                    String javaDataTypeConversion = (String) conversionDetails.get("javaDataType");
                    String cleansingValueConversion = (String) conversionDetails.get("cleansingValue");

                    // Transformation
                    String allCols = null;
                    String allColsTilt = null;
                    String allExceptPk = null;
                    String allPk = null;
                    String allCleansingValue = null;
                    String pkDateformats = null;
                    String cleansingValidations = null;
                    String allJavaDataTypes = null;
                    String allDateFormats = null;
                    String stgTableName = null;
                    String skColumn = null;
                    String allPkSk = null;
                    String dateFormat = dateFormatValue;

                    allCols = allCols == null ? ilColumnName : (allCols + "," + ilColumnName);
                    String ilColumnNameFormatted = "`" + ilColumnName + "`";
                    allColsTilt = allColsTilt == null ? ilColumnNameFormatted : (allColsTilt + "," + ilColumnNameFormatted);

                    allExceptPk = allExceptPk == null 
                        ? (constraints.equals("PK") ? null : ilColumnNameFormatted) 
                        : (constraints.equals("PK") ? allExceptPk : (allExceptPk + "," + ilColumnNameFormatted));

                    allPk = allPk == null 
                        ? (constraints.equals("PK") ? ilColumnNameFormatted : null) 
                        : (constraints.equals("PK") ? (allPk + "," + ilColumnNameFormatted) : allPk);

                    allJavaDataTypes = allJavaDataTypes == null ? javaDataTypeConversion : (allJavaDataTypes + "," + javaDataTypeConversion);

                    allDateFormats = allDateFormats == null 
                        ? (ilDataType.toLowerCase().contains("date") ? dateFormat : "") 
                        : (allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormat : ""));

                    stgTableName = ilTableName + "_Stg";

                    skColumn = skColumn == null 
                        ? (constraints.equals("SK") ? ilColumnNameFormatted : "") 
                        : (skColumn + (constraints.equals("SK") ? ilColumnNameFormatted : ""));

                    allPkSk = allPk + "," + skColumn;

                    allCleansingValue = allCleansingValue == null 
                        ? cleansingValueConversion 
                        : (allCleansingValue + "," + cleansingValueConversion);

                    String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;

                    // Aggregation
                    DimSourceValues data = aggregatedSourceValues.computeIfAbsent(key,
                    k -> new DimSourceValues(connectionIdValue, tableSchema, ilTableNameValue));
                    // Last
                    data.setAllCols(allCols);
                    data.setAllExceptPk(allExceptPk);
                    data.setAllPk(allPk);
                    data.setAllJavaDataTypes(allJavaDataTypes);
                    data.setAllDateformats(allDateFormats);
                    data.setStgTableName(stgTableName);
                    data.setSkColumn(skColumn);
                    data.setAllPkSk(allPkSk);
                    data.setAllCleansingValue(allCleansingValue);
                    data.setAllColsTilt(allColsTilt);

                    aggregatedSourceValues.put(key, data);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return aggregatedSourceValues;
        }

        public Map<String, SourceMapValues> getConstrainedFieldsAndValues(Connection connection, String dimensionTransaction, String ilTableName, String connectionId, String querySchemaCondition) throws SQLException {
            String query = "SELECT " +
                    "Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, " +
                    "Source_Table_Name, Source_Column_Name, LOWER(Source_Data_Type), PK_Constraint, PK_Column_Name, " +
                    "FK_Constraint, FK_Column_Name, Dimension_Transaction, Dimension_Key, Dimension_Name, " +
                    "Dimension_Join_Condition, Active_Flag, LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) as Datatype " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = ? AND Constraints IN ('SK', 'PK') " +
                    "AND IL_Table_Name = ? AND Connection_Id = ? " + querySchemaCondition;

            Map<String, SourceMapValues> aggregatedSourceMapValues = new HashMap<>();

                    // TODO make it global
            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, ilTableName);
            preparedStatement.setString(3, connectionId);
    
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                String connectionIdValue = rs.getString("Connection_Id");
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String ilTableNameValue = rs.getString("IL_Table_Name");
                String ilColumnName = rs.getString("IL_Column_Name");
                String ilDataType = rs.getString("IL_Data_Type");
                String constraints = rs.getString("Constraints");
                String dataType = rs.getString("Datatype");

                String allDateFormats = null;
                allDateFormats = allDateFormats == null 
                ? (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "") 
                : allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "");

                // the left join with datatypeConversions
                System.out.println("dataType: " + dataType);

                Map<String, String> conversionDetails = datatypeConversionsGlobal.getOrDefault(dataType, new HashMap<>());
                String pkCleansinghValueConversion = (String) conversionDetails.get("pkCleansingValue");

                // Output
                String fields = ilColumnName;
                String values = pkCleansinghValueConversion;
                String dateFormats = allDateFormats;

                // Aggregation
                String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                SourceMapValues data = aggregatedSourceMapValues.computeIfAbsent(key,
                k -> new SourceMapValues(connectionIdValue, tableSchema, ilTableNameValue));
                // Last
                data.setDateformats(dateFormats);
                // List
                data.appendToFields(fields);
                data.appendToValues(values);
            }

            return aggregatedSourceMapValues;
        }

        public Map<String, SourceMapValues> getNonConstrainedFieldsAndValues(Connection connection, String dimensionTransaction, String ilTableName, String connectionId, String querySchemaCondition) throws SQLException {

            String query =  "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, Source_Table_Name, " +
                "Source_Column_Name, LOWER(Source_Data_Type), PK_Constraint, PK_Column_Name, FK_Constraint, FK_Column_Name, " +
                "Dimension_Transaction, Dimension_Key, Dimension_Name, Dimension_Join_Condition, Active_Flag, " +
                "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
                "FROM ELT_IL_Source_Mapping_Info_Saved " +
                "WHERE Dimension_Transaction = ? AND Constraints NOT IN ('SK', 'PK') AND IL_Table_Name = ? " +
                "AND Connection_Id = ? " + querySchemaCondition;

            Map<String, SourceMapValues> aggregatedSourceMapValues = new HashMap<>();

            // TODO make it global
            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, ilTableName);
            preparedStatement.setString(3, connectionId);
    
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                String connectionIdValue = rs.getString("Connection_Id");
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String ilTableNameValue = rs.getString("IL_Table_Name");
                String ilColumnName = rs.getString("IL_Column_Name");
                String ilDataType = rs.getString("IL_Data_Type");
                String constraints = rs.getString("Constraints");
                String dataType = rs.getString("Datatype");

                String allDateFormats = null;
                allDateFormats = allDateFormats == null 
                ? (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "") 
                : allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "");

                // the Inner join with datatypeConversions
                System.out.println("dataType: " + dataType);

                Map<String, String> conversionDetails = datatypeConversionsGlobal.get(dataType);
                if (conversionDetails == null) // if key not found, skip the iteration
                    continue;

                String cleansingValueConversion = (String) conversionDetails.get("cleansingValue");
                System.out.println("cleansingValue: " + cleansingValueConversion);
                // Output
                String fields = ilColumnName;
                String values = cleansingValueConversion;
                String dateFormats = allDateFormats;

                // Aggregation
                String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                SourceMapValues data = aggregatedSourceMapValues.computeIfAbsent(key,
                k -> new SourceMapValues(connectionIdValue, tableSchema, ilTableNameValue));
                // Last
                data.setDateformats(dateFormats);
                // List
                data.appendToFields(fields);
                data.appendToValues(values);
            }

            return aggregatedSourceMapValues;
        }

        private Map<String, SourceMapValues> joinConstrainedNonCnstrainedData(Map<String, SourceMapValues> constrainedData, Map<String, SourceMapValues> nonConstrainedData) {

            Map<String, SourceMapValues> joinedDataMap = new HashMap<>();

            for (Map.Entry<String, SourceMapValues> entry : constrainedData.entrySet()) {
                String key = entry.getKey();
                SourceMapValues constrainedValue = entry.getValue();
                // Left Outer Join
                SourceMapValues nonConstrainedValue = nonConstrainedData.getOrDefault(key, null);

                // merge data
                String fields = (nonConstrainedValue == null || nonConstrainedValue.getFields() == null) ? constrainedValue.getFields() : constrainedValue.getFields() + "," + nonConstrainedValue.getFields();
                String values = (nonConstrainedValue == null || nonConstrainedValue.getValues() == null) ? constrainedValue.getValues() : constrainedValue.getValues() + "," + nonConstrainedValue.getValues();
                String dateformats = (nonConstrainedValue == null || nonConstrainedValue.getDateformats() == null) ? constrainedValue.getDateformats() : constrainedValue.getDateformats() + "," + nonConstrainedValue.getDateformats();

                String connectionId = constrainedValue.getConnectionId();
                String ilTableName = constrainedValue.getIlTableName();
                String tableSchema = constrainedValue.getTableSchema();

                SourceMapValues joinedValue = new SourceMapValues(connectionId, tableSchema, ilTableName);
                joinedValue.setFields(fields);
                joinedValue.setValues(values);
                joinedValue.setDateformats(dateformats);
                joinedDataMap.put(key, joinedValue);
            }
            return joinedDataMap;
        }

        private List<Map<String, Object>> dimStageToDWValuesScriptFinalProcess(Map<String, DimSourceValues> mainData, Map<String, SourceMapValues> joinedData, String lhs, String writeMode) {
            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            for (Map.Entry<String, DimSourceValues> entry : mainData.entrySet()) {
                String mainKey = entry.getKey();
                DimSourceValues mainValue = entry.getValue();
                String connectionId = mainValue.getConnectionId();
                String ilTableName = mainValue.getIlTableName();
                String tableSchema = mainValue.getTableSchema();
                // Left Outer Join
                SourceMapValues joinedValue = joinedData.getOrDefault(mainKey, new SourceMapValues(connectionId, tableSchema, ilTableName));

                // Transformations
                String var1 = lhs.replaceAll("\\$\\{src.jdbc.url}", "src.jdbc.url=jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDbName);
                String var2 = var1.replaceAll("\\$\\{src.jdbc.driver}", "src.jdbc.driver=com.mysql.jdbc.Driver");
                String var3 = var2.replaceAll("\\$\\{src.db.user}", "src.db.user=" + srcUn);
                String srcPwdEscaped = srcPwd.replaceAll("\\$", "\\$");
                String tgtPwdEscaped = tgtPwd.replaceAll("\\$", "\\$");
                String var4 = var3.replaceAll("\\$\\{src.db.password}", "src.db.password=" + srcPwdEscaped);
                String var5 = var4.replaceAll("\\$\\{source.query}", "source.query=SELECT " + mainValue.getAllColsTilt().replaceAll("\\$", "\\\\\\$") + " FROM " + mainValue.getStgTableName().replaceAll("\\$", "\\\\\\$"));
                String var6 = var5.replaceAll("\\$\\{source.is.query}", "source.is.query=True");
                String var7 = var6.replaceAll("\\$\\{cleansing.fields}", "cleansing.fields=" + joinedValue.getFields().replaceAll("\\$", "\\\\\\$"));
                String generateA = generateText("a", joinedValue.getFields().split(",").length + 1);
                String varAReplace = generateA.replaceAll("a", "EMPTY,");
                String varEmpty = varAReplace.substring(0, varAReplace.length() - 1);
                String var8 = var7.replaceAll("\\$\\{cleansing.validation}", "cleansing.validation=" + varEmpty);
                String var9 = var8.replaceAll("\\$\\{cleansing.values}", "cleansing.values=" + joinedValue.getValues());
                String var10 = var9.replaceAll("\\$\\{date.formats}", "date.formats=" + joinedValue.getDateformats());
                String var11 = var10.replaceAll("\\$\\{tgt.jdbc.url}", "tgt.jdbc.url=jdbc:mysql://" + tgtHost + ":" + tgtPort + "/" + tgtDbName);
                String var12 = var11.replaceAll("\\$\\{tgt.jdbc.driver}", "tgt.jdbc.driver=com.mysql.jdbc.Driver");
                String var13 = var12.replaceAll("\\$\\{tgt.db.user}", "tgt.db.user=" + tgtUn);
                String var14 = var13.replaceAll("\\$\\{tgt.db.password}", "tgt.db.password=" + tgtPwdEscaped);
                String var15 = var14.replaceAll("\\$\\{target.table}", "target.table=" + mainValue.getIlTableName().replaceAll("\\$", "\\\\\\$"));
                String var16 = var15.replaceAll("\\$\\{key.fields}", "key.fields=" + mainValue.getAllPkSk().replaceAll("\\$", "\\\\\\$"));
                String var17 = var16.replaceAll("\\$\\{key.columns}", "key.columns=" + mainValue.getAllPkSk().replaceAll("\\$", "\\\\\\$"));
                String var18 = var17.replaceAll("\\$\\{key.fields.case.sensitive}", "key.fields.case.sensitive=True");
                String var19 = var18.replaceAll("\\$\\{insert.constant.columns}", "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                String var20 = var19.replaceAll("\\$\\{insert.constant.store.values}", "insert.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin',UTC_TIMESTAMP(),'ELT_Admin'");
                String var21 = var20.replaceAll("\\$\\{insert.constant.store.types}", "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");
                String var22 = var21.replaceAll("\\$\\{update.constant.columns}", "update.constant.columns=Updated_Date,Updated_User");
                String var23 = var22.replaceAll("\\$\\{update.constant.store.values}", "update.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin'");
                String var24 = var23.replaceAll("\\$\\{update.constant.store.types}", "update.constant.store.types=java.util.Date,java.lang.String");
                String var25 = var24.replaceAll("\\$\\{batch.type}", "batch.type=BATCH_BY_SIZE");
                String var26 = var25.replaceAll("\\$\\{batch.size}", "batch.size=10000");
                String partitionSize = var26.replaceAll("\\$\\{partition.size}", "partition.size=");
                String partitionColumns = partitionSize.replaceAll("\\$\\{columns}", "columns=" + mainValue.getAllColsTilt().replaceAll("\\$", "\\\\\\$"));
                String partitionTableName = partitionColumns.replaceAll("\\$\\{table.name}", "table.name=`" + mainValue.getIlTableName().replaceAll("\\$", "\\\\\\$") + "_Stg`");
                String partitionUpperBound = partitionTableName.replaceAll("\\$\\{partition.upper.bound}", "partition.upper.bound=");
                String numberOfPartitions = partitionUpperBound.replaceAll("\\$\\{number.of.partitions}", "number.of.partitions=");
                String resultFetcherClassNames = numberOfPartitions.replaceAll("\\$\\{resultfetcher.class.names}", "resultfetcher.class.names=com.anvizent.elt.anvizent.util.ChecksumGenerator");
                String resultFetcherMethodNames = resultFetcherClassNames.replaceAll("\\$\\{resultfetcher.method.names}", "resultfetcher.method.names=generate");
                String resultFetcherMethodArgumentFields = resultFetcherMethodNames.replaceAll("\\$\\{resultfetcher.method.argument.fields}", "resultfetcher.method.argument.fields=\"" + mainValue.getAllCols().replaceAll("\\$", "\\\\\\$") + "\"");
                String resultFetcherResultFields = resultFetcherMethodArgumentFields.replaceAll("\\$\\{resultfetcher.return.fields}", "resultfetcher.return.fields=Source_Hash_Value");
                String whereCondition = writeMode.equals("overwrite") ? "where.condition=" : "where.condition=where Updated_Date>=${start.date}";
                String incremental = resultFetcherResultFields.replaceAll("\\$\\{where.condition}", whereCondition.replaceAll("\\$", "\\\\\\$"));


                // Output
                String valueFile = incremental;
                String fileName = getValueFileName(ilTableName, VALUE_FILE_STRING);

                writeToFile(valueFile, fileName);
                
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId); // Global
                resultRow.put("TABLE_SCHEMA", schemaName); // Global
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);

            }
            return finalResults;
        }
        /* Transaction Staging To DW Values */
        void transStageToDWValuesScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Transaction Staging to DW Value..");

            String dimensionTransaction = "T";
            String jobType = "Transaction_stg_il";

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
            dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);


            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                System.out.println("    iLTableName: " + ilTableName);
                String dateFormat = getSettingValue(connection, connectionId, schemaName, "Dateformat");
                if (dateFormat == null) {
                    dateFormat = "yyyy-MM-dd";
                }

                long connectionType =  getIsWebService(connection, ilTableName, connectionId, schemaName);
                System.out.println("connectionType: " + connectionType);
                String query = "";
                if (connectionType == 1) {
                    query = "SELECT time_zone FROM minidwcs_database_connections WHERE Connection_Id = ?";
                }
                else if (connectionType == 0) {
                    query = "SELECT time_zone FROM minidwcs_ws_connections_mst WHERE Id = ?";
                }
                else {
                    System.out.println("connectionType: " + connectionType +  " is not supported. skipping the ilTableName: " + ilTableName);
                    continue;
                }

                TimeZone timeZone = getTimeZone(connection, connectionId, query);

                List<String> SourceTableList = getSourceTableName(connection, connectionId, ilTableName, querySchemaCond);
                System.out.println("SourceTableList: " + SourceTableList.size());
                String SourceName = "";
                if (SourceTableList.size() != 0) {
                    SourceName = SourceTableList.get(0);
                    System.out.println("SourceName: " + SourceName);
                }

                String writeMode = getWriteMode(ilTableName);
                String lhs = getAggregatedValueNames(connection, jobType, writeMode);

                Map<String, DimSourceValues> mainData = getTransStageToDwSourceMappingInfo(connection, dimensionTransaction, querySchemaCond, connectionId, ilTableName);
                System.out.println("Trans mainData: " + mainData.size());

                Map<String, Map<String, String>> hashColumnPick = getHashColumnPick(connection, ilTableName, connectionId, querySchemaCond);
                Map<String, String> keyMapping = getForeignKeyMappings(connection, connectionId, ilTableName, querySchemaCond);
                System.out.println("keyMapping: " + keyMapping);
                List<Map<String, String>> mainFlowData = joinTransMainDataWithHashColumnAndFKMappings(mainData, hashColumnPick, keyMapping);

                Map<String, SourceMapValues> constrainedData = getTransConstrainedFieldsAndValues(connection, dimensionTransaction, ilTableName, connectionId, querySchemaCondition);
                System.out.println("Trans constrainedData: " + constrainedData);
                Map<String, SourceMapValues> nonConstrainedData = getTransNonConstrainedFieldsAndValues(connection, dimensionTransaction, ilTableName, connectionId, querySchemaCondition);
                System.out.println("Trans Non onstrainedData: " + constrainedData);

                Map<String, SourceMapValues> joinedData = joinTransConstrainedNonCnstrainedData(constrainedData, nonConstrainedData);
                List<Map<String, Object>> finalData = transStageToDWValuesScriptFinalProcess(mainFlowData, joinedData, lhs, writeMode);
                // Update DB
                if (finalData != null && !finalData.isEmpty()) {
                    deleteValuesProperties(connection, ilTableName, connectionId);
                    insertIntoEltValuesProperties(conn, finalData);
                }
            }
        }
     
        private Map<String, DimSourceValues> getTransStageToDwSourceMappingInfo(Connection conn,
                String dimensionTransaction, String querySchemaCond,
                String connectionId, String ilTableName) throws SQLException {

            Map<String, DimSourceValues> aggregatedSourceValues = new HashMap<>();

            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

             String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, " +
               "Source_Table_Name, Source_Column_Name, LOWER(Source_Data_Type), PK_Constraint, PK_Column_Name, " +
               "FK_Constraint, FK_Column_Name, Dimension_Transaction, Dimension_Key, Dimension_Name, " +
               "Dimension_Join_Condition, LHS_Join_Condition, RHS_Join_Condition, Active_Flag, " +
               "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
               "FROM ELT_IL_Source_Mapping_Info_Saved " +
               "WHERE Connection_Id = ? " + querySchemaCond + 
               " AND Dimension_Transaction = ? " +
               " AND Constraints NOT IN ('FK', 'SK') " +
               " AND IL_Table_Name = ? " +
               "ORDER BY IL_Table_Name, Constraints DESC";

            try {
                PreparedStatement pstmt = conn.prepareStatement(query);
                pstmt.setString(1, connectionId);
                pstmt.setString(2, dimensionTransaction);
                pstmt.setString(3, ilTableName);

                rs = pstmt.executeQuery();

                String cdcFlag = "";
                while (rs.next()) {
                    String connectionIdValue = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("TABLE_SCHEMA");
                    String ilTableNameValue = rs.getString("IL_Table_Name");
                    String ilColumnName = rs.getString("IL_Column_Name");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String constraints = rs.getString("Constraints");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String sourceColumnName = rs.getString("Source_Column_Name");
                    // String sourceDataType = rs.getString("Source_Data_Type_Lower"); // alias used
                    String pkConstraint = rs.getString("PK_Constraint");
                    String pkColumnName = rs.getString("PK_Column_Name");
                    String fkConstraint = rs.getString("FK_Constraint");
                    String fkColumnName = rs.getString("FK_Column_Name");
                    String dimensionTransactionValue = rs.getString("Dimension_Transaction");
                    String dimensionKey = rs.getString("Dimension_Key");
                    String dimensionName = rs.getString("Dimension_Name");
                    String dimensionJoinCondition = rs.getString("Dimension_Join_Condition");
                    boolean activeFlag = rs.getBoolean("Active_Flag");
                    String dataType = rs.getString("Datatype");

                    // String var1 = "`" + ilColumnName + "`"; // Not used
                    if (constraints == null) { // Not Done
                        constraints = "";
                    }
                    // the left join with datatypeConversions
                    Map<String, String> conversionDetails = datatypeConversionsGlobal.getOrDefault(dataType, new HashMap<>());
                    String javaDataTypeConversion = (String) conversionDetails.get("javaDataType");
                    String cleansingValueConversion = (String) conversionDetails.get("cleansingValue");

                    String allCleansingValue = cleansingValueConversion;
                    // Transformation
                    String allCols = null;
                    String allColsTilt = null;
                    String allExceptPk = null;
                    String allPk = null;
                    String allPkJava = null;
                    String allCleansingValueNew = null;
                    String pkDateformats = null;
                    String cleansingValidations = null;
                    String allJavaDataTypes = null;
                    String allDateFormats = null;
                    String stgTableName = null;
                    String skColumn = null;
                    String allPkSk = null;
                    String precisions = null;
                    String scales = null;
                    String dateFormat = dateFormatValue;

                    // Transformation
                    allCols = (allCols == null) ? ilColumnName : (allCols + "," + ilColumnName);
                    ilColumnName = "`" + ilColumnName + "`";
                    allColsTilt = (allColsTilt == null) ? ilColumnName : (allColsTilt + "," + ilColumnName);
                    
                    allExceptPk = (allExceptPk == null) 
                        ? (constraints.equals("PK") ? null : ilColumnName) 
                        : (constraints.equals("PK") ? allExceptPk : allExceptPk + "," + ilColumnName);
                    
                    allPk = (allPk == null) 
                        ? (constraints.equals("PK") ? ilColumnName : null) 
                        : (constraints.equals("PK") ? allPk + "," + ilColumnName : allPk);
                    
                    allPkJava = (allPkJava == null) 
                        ? (constraints.equals("PK") ? javaDataTypeConversion : null) 
                        : (constraints.equals("PK") ? allPkJava + "," + javaDataTypeConversion : allPkJava);
                    
                    allJavaDataTypes = (allJavaDataTypes == null) 
                        ? javaDataTypeConversion 
                        : (allJavaDataTypes + "," + javaDataTypeConversion);
                    
                    allDateFormats = (allDateFormats == null) 
                        ? (ilDataType.toLowerCase().contains("date") ? dateFormat : "") 
                        : (allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormat : ""));
                    
                    stgTableName = ilTableName + "_Stg";
                    skColumn = sourceTableName + "_Key";
                    allPkSk = allPk + "," + skColumn;
                    
                    String dynamicWhereComponent = null;
                    
                    allCleansingValueNew = (allCleansingValueNew == null) 
                        ? allCleansingValue 
                        : (allCleansingValue + "," + allCleansingValue);
                    allCleansingValue = allCleansingValueNew; // variable referenced later

                    precisions = (precisions == null) 
                        ? (constraints.equals("PK") 
                            ? (ilDataType.toLowerCase().matches(".*(decimal|float|double).*") ? precisions : "") 
                            : null) 
                        : (constraints.equals("PK") 
                            ? (precisions + "," + (ilDataType.toLowerCase().matches(".*(decimal|float|double).*") ? precisions : "")) 
                            : precisions);
                    
                    scales = (scales == null) 
                        ? (constraints.equals("PK") 
                            ? (ilDataType.toLowerCase().matches(".*(decimal|float|double).*") ? scales : "") 
                            : null) 
                        : (constraints.equals("PK") 
                            ? (scales + "," + (ilDataType.toLowerCase().matches(".*(decimal|float|double).*") ? scales : "")) 
                            : scales);
                    
                    String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;

                    // Aggregation
                    DimSourceValues data = aggregatedSourceValues.computeIfAbsent(key,
                    k -> new DimSourceValues(connectionIdValue, tableSchema, ilTableNameValue));
                    // Last value
                    data.setAllCols(allCols);
                    data.setAllExceptPk(allExceptPk);
                    data.setAllPk(allPk);
                    data.setAllJavaDataTypes(allJavaDataTypes);
                    data.setAllDateformats(allDateFormats);
                    data.setStgTableName(stgTableName);
                    data.setSkColumn(skColumn);
                    data.setAllPkSk(allPkSk);
                    data.setDynamicWhereComponent(dynamicWhereComponent);
                    data.setAllCleansingValue(allCleansingValue);
                    data.setAllColsTilt(allColsTilt);
                    data.setAllPkJava(allPkJava);
                    data.setPrecisions(precisions);
                    data.setScales(scales);
                    
                    aggregatedSourceValues.put(key, data);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return aggregatedSourceValues;
        }


        public Map<String, SourceMapValues> getTransConstrainedFieldsAndValues(Connection connection, String dimensionTransaction, String ilTableName, String connectionId, String querySchemaCondition) throws SQLException {

            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, " +
               "Source_Table_Name, Source_Column_Name, Source_Data_Type, PK_Constraint, PK_Column_Name, " +
               "FK_Constraint, FK_Column_Name, Dimension_Transaction, Dimension_Key, Dimension_Name, " +
               "Dimension_Join_Condition, Active_Flag, " +
               "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
               "FROM ELT_IL_Source_Mapping_Info_Saved " +
               "WHERE Dimension_Transaction = ? " +
               "AND Constraints IN ('PK') " +
               "AND IL_Table_Name = ? " +
               "AND Connection_Id = ? " + querySchemaCondition;

            Map<String, SourceMapValues> aggregatedSourceMapValues = new HashMap<>();

            // TODO make it global
            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, ilTableName);
            preparedStatement.setString(3, connectionId);
    
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                String connectionIdValue = rs.getString("Connection_Id");
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String ilTableNameValue = rs.getString("IL_Table_Name");
                String ilColumnName = rs.getString("IL_Column_Name");
                String ilDataType = rs.getString("IL_Data_Type");
                String constraints = rs.getString("Constraints");
                String dataType = rs.getString("Datatype");

                String allDateFormats = null;
                allDateFormats = allDateFormats == null 
                ? (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "") 
                : allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "");

                // the left join with datatypeConversions
                System.out.println("dataType: " + dataType);

                Map<String, String> conversionDetails = datatypeConversionsGlobal.getOrDefault(dataType, new HashMap<>());
                String pkCleansinghValueConversion = (String) conversionDetails.get("pkCleansingValue");

                // Output
                String fields = ilColumnName;
                String values = pkCleansinghValueConversion;
                String dateFormats = allDateFormats;

                // Aggregation
                String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                SourceMapValues data = aggregatedSourceMapValues.computeIfAbsent(key,
                k -> new SourceMapValues(connectionIdValue, tableSchema, ilTableNameValue));
                // Last
                data.setDateformats(dateFormats);
                // List
                data.appendToFields(fields);
                data.appendToValues(values);
            }

            return aggregatedSourceMapValues;
        }
        
        public Map<String, SourceMapValues> getTransNonConstrainedFieldsAndValues(Connection connection, String dimensionTransaction, String ilTableName, String connectionId, String querySchemaCondition) throws SQLException {

            String query =  "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, Source_Table_Name, " +
                "Source_Column_Name, LOWER(Source_Data_Type), PK_Constraint, PK_Column_Name, FK_Constraint, FK_Column_Name, " +
                "Dimension_Transaction, Dimension_Key, Dimension_Name, Dimension_Join_Condition, Active_Flag, " +
                "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
                "FROM ELT_IL_Source_Mapping_Info_Saved " +
                "WHERE Dimension_Transaction = ? AND Constraints NOT IN ('SK', 'PK') AND IL_Table_Name = ? " +
                "AND Connection_Id = ? " + querySchemaCondition;

            Map<String, SourceMapValues> aggregatedSourceMapValues = new HashMap<>();

            // TODO make it global
            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }

            System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
            ResultSet rs = null;

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, dimensionTransaction);
            preparedStatement.setString(2, ilTableName);
            preparedStatement.setString(3, connectionId);
    
            rs = preparedStatement.executeQuery();

            while (rs.next()) {
                String connectionIdValue = rs.getString("Connection_Id");
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String ilTableNameValue = rs.getString("IL_Table_Name");
                String ilColumnName = rs.getString("IL_Column_Name");
                String ilDataType = rs.getString("IL_Data_Type");
                String constraints = rs.getString("Constraints");
                String dataType = rs.getString("Datatype");

                String allDateFormats = null;
                allDateFormats = allDateFormats == null 
                ? (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "") 
                : allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateFormatValue : "");

                // the Inner join with datatypeConversions
                System.out.println("dataType: " + dataType);

                Map<String, String> conversionDetails = datatypeConversionsGlobal.get(dataType);
                if (conversionDetails == null) // if key not found, skip the iteration
                    continue;

                String cleansingValueConversion = (String) conversionDetails.get("cleansingValue");
                System.out.println("cleansingValue: " + cleansingValueConversion);
                // Output
                String fields = ilColumnName;
                String values = cleansingValueConversion;
                String dateFormats = allDateFormats;

                // Aggregation
                String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                System.out.println("key: " + key);
                SourceMapValues data = aggregatedSourceMapValues.computeIfAbsent(key,
                k -> new SourceMapValues(connectionIdValue, tableSchema, ilTableNameValue));
                // Last
                data.setDateformats(dateFormats);
                // List
                data.appendToFields(fields);
                data.appendToValues(values);
                aggregatedSourceMapValues.put(key, data);

            }

            return aggregatedSourceMapValues;
        }

        private Map<String, SourceMapValues> joinTransConstrainedNonCnstrainedData(Map<String, SourceMapValues> constrainedData, Map<String, SourceMapValues> nonConstrainedData) {

            Map<String, SourceMapValues> joinedDataMap = new HashMap<>();

            for (Map.Entry<String, SourceMapValues> entry : constrainedData.entrySet()) {
                String key = entry.getKey();
                SourceMapValues constrainedValue = entry.getValue();
                // Left Outer Join
                SourceMapValues nonConstrainedValue = nonConstrainedData.getOrDefault(key, null);
                System.out.println("key: " + key);
                System.out.println("nonConstrainedValue: " + nonConstrainedValue);

                // merge data
                String fields = (nonConstrainedValue == null || nonConstrainedValue.getFields() == null) ? constrainedValue.getFields() : constrainedValue.getFields() + "," + nonConstrainedValue.getFields();
                String values = (nonConstrainedValue == null || nonConstrainedValue.getValues() == null) ? constrainedValue.getValues() : constrainedValue.getValues() + "," + nonConstrainedValue.getValues();
                String dateformats = (nonConstrainedValue == null || nonConstrainedValue.getDateformats() == null) ? constrainedValue.getDateformats() : constrainedValue.getDateformats() + "," + nonConstrainedValue.getDateformats();

                String connectionId = constrainedValue.getConnectionId();
                String ilTableName = constrainedValue.getIlTableName();
                String tableSchema = constrainedValue.getTableSchema();

                SourceMapValues joinedValue = new SourceMapValues(connectionId, tableSchema, ilTableName);
                joinedValue.setFields(fields);
                joinedValue.setValues(values);
                joinedValue.setDateformats(dateformats);
                joinedDataMap.put(key, joinedValue);
            }
            return joinedDataMap;
        }

        public Map<String, Map<String, String>> getHashColumnPick(Connection connection, String tableName, String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT Table_Name, PK_Columns, Hash_Columns, " +
                           "ROUND((LENGTH(PK_Columns) - LENGTH(REPLACE(PK_Columns, ',', ''))) / LENGTH(',')) AS Comma_Count, " +
                           "Date_Formats, precisions, scales, Datatypes " +
                           "FROM stg_hashcolumn_pick " +
                           "WHERE Table_Name = ? AND Connection_Id = ? " + querySchemaCond;
        
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setString(1, tableName + "_Stg");
                stmt.setString(2, connectionId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> rowMap = new HashMap<>();
                        rowMap.put("PK_Columns", rs.getString("PK_Columns"));
                        rowMap.put("Hash_Columns", rs.getString("Hash_Columns"));
                        rowMap.put("Comma_Count", rs.getString("Comma_Count"));
                        rowMap.put("Date_Formats", rs.getString("Date_Formats"));
                        rowMap.put("precisions", rs.getString("precisions"));
                        rowMap.put("scales", rs.getString("scales"));
                        rowMap.put("Datatypes", rs.getString("Datatypes"));
                        
                        java.lang.String key = rs.getString("Table_Name");
                        resultMap.put(key, rowMap);
                    }
                }
            }
        
            return resultMap;
        }

        public Map<String, String> getForeignKeyMappings(Connection connection, String connectionId, String tableName, String querySchemaCond) throws SQLException {
            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, Constraints, " +
                           "Source_Table_Name, Source_Column_Name, Source_Data_Type, PK_Constraint, PK_Column_Name, " +
                           "FK_Constraint, FK_Column_Name, Dimension_Transaction, Dimension_Key, Dimension_Name, " +
                           "Dimension_Join_Condition, LHS_Join_Condition, RHS_Join_Condition, Active_Flag " +
                           "FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "WHERE Connection_Id = ? " + querySchemaCond + " AND Dimension_Transaction = 'T' " +
                           "AND Constraints = 'FK' AND IL_Table_Name = ? ORDER BY Constraints DESC";
        
            Map<String, String> resultMap = new HashMap<>();
            // StringBuilder aggregatedDynamicWhereComponent = new StringBuilder();
            Map<String, String> aggregatedDynamicWhereComponentMap = new HashMap<>();
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setString(1, connectionId);
                stmt.setString(2, tableName);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> rowMap = new HashMap<>();

                        rowMap.put("IL_Column_Name", rs.getString("IL_Column_Name"));
                        rowMap.put("IL_Data_Type", rs.getString("IL_Data_Type"));
                        rowMap.put("Constraints", rs.getString("Constraints"));
                        rowMap.put("Source_Table_Name", rs.getString("Source_Table_Name"));
                        rowMap.put("Source_Column_Name", rs.getString("Source_Column_Name"));
                        rowMap.put("Source_Data_Type", rs.getString("Source_Data_Type"));
                        rowMap.put("PK_Constraint", rs.getString("PK_Constraint"));
                        rowMap.put("PK_Column_Name", rs.getString("PK_Column_Name"));
                        rowMap.put("FK_Constraint", rs.getString("FK_Constraint"));
                        rowMap.put("FK_Column_Name", rs.getString("FK_Column_Name"));
                        rowMap.put("Dimension_Transaction", rs.getString("Dimension_Transaction"));
                        rowMap.put("Dimension_Key", rs.getString("Dimension_Key"));
                        rowMap.put("Dimension_Name", rs.getString("Dimension_Name"));
                        rowMap.put("Dimension_Join_Condition", rs.getString("Dimension_Join_Condition"));
                        rowMap.put("LHS_Join_Condition", rs.getString("LHS_Join_Condition"));
                        rowMap.put("RHS_Join_Condition", rs.getString("RHS_Join_Condition"));
                        rowMap.put("Active_Flag", rs.getString("Active_Flag"));
        
                        String dimensionNameValue = rs.getString("Dimension_Name");
                        String constraints = rs.getString("Constraints");
                        String dimensionKey = rs.getString("Dimension_Key");
                        String lhsJoinCondition = rs.getString("LHS_Join_Condition");
                        String rhsJoinCondition = rs.getString("RHS_Join_Condition");
                        // Transformation
                        String dimensionName = dimensionNameValue.replace(" ", "_") + "_Lkp";
                        String dynamicWhereComponent = null;

                        if (constraints.equals("FK")) {
                            String lookupTable = dimensionName + ".lookup.table=" + dimensionName + "_Stg\n";
                            String selectColumns = dimensionName + ".select.columns=" + dimensionKey + "\n";
                            String whereFields = dimensionName + ".where.fields=" + lhsJoinCondition + "\n";
                            String whereColumns = dimensionName + ".where.columns=" + rhsJoinCondition;

                            if (dynamicWhereComponent == null) {
                                dynamicWhereComponent = lookupTable + selectColumns + whereFields + whereColumns;
                            } else {
                                dynamicWhereComponent += "\n" + lookupTable + selectColumns + whereFields + whereColumns;
                            }
                        }
                        String connectionIdValue = rs.getString("Connection_Id");
                        String tableSchema = rs.getString("TABLE_SCHEMA");
                        String ilTableNameValue = rs.getString("IL_Table_Name");
                        String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;

                        // if (aggregatedDynamicWhereComponent.length() > 0) {
                        //     aggregatedDynamicWhereComponent.append(",");  // Add a comma to separate components
                        // }
                        String aggregatedDynamicWhereComponent = aggregatedDynamicWhereComponentMap.getOrDefault(key, new String());
                        // String aggregatedDynamicWhereComponent = "";
                        if (aggregatedDynamicWhereComponent == null || aggregatedDynamicWhereComponent.isEmpty()) {
                            aggregatedDynamicWhereComponent = dynamicWhereComponent;
                        } else {
                            aggregatedDynamicWhereComponent += "," + dynamicWhereComponent;
                        }
                        aggregatedDynamicWhereComponentMap.put(key, aggregatedDynamicWhereComponent);
                    }
                }
            }
        
            return aggregatedDynamicWhereComponentMap;
        }

        private List<Map<String, String>> joinTransMainDataWithHashColumnAndFKMappings(Map<String, DimSourceValues> mainData,
                Map<String, Map<String, String>> hashColumnPick, Map<String, String> keyMapping) {
                    
            List<Map<String, String>> finalResults = new ArrayList<>();

            for (Map.Entry<String, DimSourceValues> entry : mainData.entrySet()) {
                String mainKey = entry.getKey();
                DimSourceValues mainValue = entry.getValue();
            
                String keyStageTableName = mainValue.getStgTableName();
                // Let Outer Join
                Map<String, String> columnPickData = hashColumnPick.getOrDefault(mainKey, Collections.emptyMap());
                // Left Outer Join
                String dynamicWhereComponentValue = keyMapping.getOrDefault(keyStageTableName, new String());
                System.out.println("mainKey: " + mainKey);
                System.out.println("keyStageTableName: " + keyStageTableName);
                System.out.println("columnPickData: " + columnPickData);
                System.out.println("dynamicWhereComponentValue: " + dynamicWhereComponentValue);

                String dynamicWhereComponent = (dynamicWhereComponentValue == null) ? null
                        : dynamicWhereComponentValue.replace("$", "\\$");


                // # Output table
                Map<String, String> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", mainValue.getConnectionId());
                resultRow.put("TABLE_SCHEMA", mainValue.getTableSchema());
                resultRow.put("IL_Table_Name", mainValue.getIlTableName());
                resultRow.put("all_cols", mainValue.getAllCols());
                resultRow.put("all_cols_tilt", mainValue.getAllColsTilt());
                resultRow.put("all_except_pk", mainValue.getAllExceptPk());
                resultRow.put("all_pk", mainValue.getAllPk());
                resultRow.put("all_javadata_types", mainValue.getAllJavaDataTypes());
                resultRow.put("all_dateformats", mainValue.getAllDateformats());
                resultRow.put("Stg_Table_Name", mainValue.getStgTableName());
                resultRow.put("SK_Column", mainValue.getSkColumn());
                resultRow.put("all_pk_sk", mainValue.getAllPkSk());
                resultRow.put("dynamic_where_component", dynamicWhereComponent);
                resultRow.put("all_Cleansing_Value", mainValue.getAllCleansingValue());
                resultRow.put("Comma_Count", columnPickData.get("Comma_Count"));
                resultRow.put("Date_Formats", columnPickData.get("Date_Formats"));
                resultRow.put("all_pk_java", mainValue.getAllPkJava());
                resultRow.put("PK_Columns", columnPickData.get("PK_Columns"));
                resultRow.put("precisions", columnPickData.get("precisions"));
                resultRow.put("scales", columnPickData.get("scales"));
                resultRow.put("PK_Datatypes", columnPickData.get("Datatypes"));

                System.out.println(" joinTransMainDataWithHashColumnAndFKMappings:\n" + resultRow );
                finalResults.add(resultRow);

            }
            return finalResults;
        }

        private List<Map<String, Object>> transStageToDWValuesScriptFinalProcess(List<Map<String, String>> mainData, Map<String, SourceMapValues> joinedData, String lhs, String writeMode) {
            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            for (Map<String, String> row : mainData) {

                String connectionId = row.get("Connection_Id");
                String tableSchema = row.get("TABLE_SCHEMA");
                String ilTableName = row.get("IL_Table_Name");
                String dynamicWhereComponent = row.get("dynamic_where_component");

                String mainKey = connectionId + "-" + tableSchema + "-" + ilTableName;
                // Left Outer Join
                SourceMapValues joinedValue = joinedData.getOrDefault(mainKey, new SourceMapValues(connectionId, tableSchema, ilTableName));

                // Transformations
                String var1 = lhs.replace("${src.jdbc.url}",
                        "src.jdbc.url=jdbc:mysql://" + srcHost + ":" + srcPort + "/" + srcDbName);

                String var2 = var1.replace("${src.jdbc.driver}", "src.jdbc.driver=com.mysql.jdbc.Driver");
                String var3 = var2.replace("${src.db.user}", "src.db.user=" + srcUn);

                String src_pwd = srcPwd.replace("$", "\\$");
                String tgt_pwd = tgtPwd.replace("$", "\\$");

                String var4 = var3.replace("${src.db.password}", "src.db.password=" + src_pwd);
                String var5 = var4.replace("${source.query}", "source.query=SELECT " + row.get("all_cols_tilt") + " FROM " + row.get("Stg_Table_Name"));
                String var6 = var5.replace("${source.is.query}", "source.is.query=True");

                String var7 = var6.replace("${cleansing.fields}", "cleansing.fields=" + joinedValue.getFields().replace("$", "\\$"));
                String generateA = generateText("a", joinedValue.getFields().split(",").length + 1);
                // String generate_a = "a".repeat(out31.feilds.split(",").length + 1);
                String var_a_replace = generateA.replace("a", "EMPTY,");
                String var_empty = var_a_replace.substring(0, var_a_replace.length() - 1);

                String var8 = var7.replace("${cleansing.validation}", "cleansing.validation=" + var_empty);
                String var9 = var8.replace("${cleansing.values}", "cleansing.values=" + joinedValue.getValues());
                String var10 = var9.replace("${date.formats}", "date.formats=" + joinedValue.getDateformats());

                String var11 = var10.replace("${tgt.jdbc.url}", 
                    "tgt.jdbc.url=jdbc:mysql://" + tgtHost + ":" + tgtPort + "/" + tgtDbName);

                String var12 = var11.replace("${tgt.jdbc.driver}", "tgt.jdbc.driver=com.mysql.jdbc.Driver");
                String var13 = var12.replace("${tgt.db.user}", "tgt.db.user=" + tgtUn);
                String var14 = var13.replace("${tgt.db.password}", "tgt.db.password=" + tgt_pwd);

                String var15 = var14.replace("${target.table}", "target.table=" + ilTableName.replace("$", "\\$"));
                String var16 = var15.replace("${key.fields}", "key.fields=" + row.get("PK_Columns"));
                String var17 = var16.replace("${key.columns}", "key.columns=" + row.get("PK_Columns"));
                String var18 = var17.replace("${key.fields.case.sensitive}", "key.fields.case.sensitive=True");

                String var19 = var18.replace("${insert.constant.columns}", 
                    "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");

                String var20 = var19.replace("${insert.constant.store.values}", 
                    "insert.constant.store.values=UTC_TIMESTAMP(),'ELT_Admin',UTC_TIMESTAMP(),'ELT_Admin'");

                String var21 = var20.replace("${insert.constant.store.types}", 
                    "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");

                String var22 = var21.replace("${update.constant.columns}", "update.constant.columns=Updated_Date");
                String var23 = var22.replace("${update.constant.store.values}", "update.constant.store.values=UTC_TIMESTAMP()");
                String var24 = var23.replace("${update.constant.store.types}", "update.constant.store.types=java.util.Date");

                String var25 = var24.replace("${batch.type}", "batch.type=BATCH_BY_SIZE");
                String var26 = var25.replace("${batch.size}", "batch.size=10000");

                String var27 = var26.replace("${lookup.table}", "lookup.table=`" + row.get("Stg_Table_Name").replace("$", "\\$") + "_Keys`");
                String var28 = var27.replace("${where.fields}", "where.fields=PKValue");

                String var29 = (dynamicWhereComponent == null || dynamicWhereComponent.equals("NULL")) 
                    ? var28.replace("${where.columns}", "where.columns=PKValue\n")
                    : var28.replace("${where.columns}", "where.columns=PKValue\n" + dynamicWhereComponent);

                String var30 = var29.replace("${sqllookup.key.fields.case.sensitive}", "sqllookup.key.fields.case.sensitive=True");
                String var31 = var30.replace("${select.columns}", "select.columns=Updated_Date,HashValue as Source_Hash_Value");
                String var32 = var31.replace("${select.columns.as.fields}", "select.columns.as.fields=Updated_Date");

                String var33 = var32.replace("${mapping.retain.emit}", "mapping.retain.emit=Updated_Date,underscore_field,PKValue");

                String var34 = var33.replace("${filter.expressions}", 
                    "filter.expressions=$0.after(new java.text.SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss\").parse(${incremental.date}))");

                String var35 = var34.replace("${filter.argument.fields}", "filter.argument.fields=Updated_Date");
                String var36 = var35.replace("${filter.argument.types}", 
                    "filter.argument.types=java.util.Date\nincremental.date=\"0000-00-00 00:00:00\"");

                String var38 = var36.replace("${mapping.constants.fields}", "mapping.constants.fields=underscore_field");
                String var39 = var38.replace("${mapping.constants.fields.types}", "mapping.constants.fields.types=java.lang.String");
                String var40 = var39.replace("${mapping.constants.fields.values}", "mapping.constants.fields.values=_");

                String var41 = var40.replace("${resultfetcher.class.names}", "resultfetcher.class.names=java.lang.String");
                String var42 = var41.replace("${resultfetcher.method.names}", "resultfetcher.method.names=join");

                String var43 = var42.replace("${resultfetcher.method.argument.fields}", 
                    "resultfetcher.method.argument.fields=\"underscore_field," + row.get("PK_Columns") + "\"");

                String var44 = var43.replace("${resultfetcher.return.fields}", "resultfetcher.return.fields=PKValue");
                String var45 = var44.replace("${partition.size}", "partition.size=");
                String var46 = var45.replace("${columns}", "columns=" + row.get("all_cols_tilt").replace("$", "\\$"));

                String var47 = var46.replace("${table.name}", "table.name=`" + ilTableName.replace("$", "\\$") + "_Stg`");
                String var48 = var47.replace("${partition.upper.bound}", "partition.upper.bound=");
                String var49 = var48.replace("${number.of.partitions}", "number.of.partitions=");

                String var50 = var49.replace("${empty.coerce.fields}", "empty.coerce.fields=" + row.get("PK_Columns")); // Global var alias "stg_keys_pk_columns"
                
                String generate_a1 = generateText("a", (row.get("Comma_Count") != null) ? Integer.valueOf(row.get("Comma_Count")) + 1: 1 );
                String var_a_replace1 = generate_a1.replace("a", "java.lang.String,");
                String var_empty1 = var_a_replace1.substring(0, var_a_replace1.length() - 1);

                String var51 = var50.replace("${empty.coerce.to}", "empty.coerce.to=" + var_empty1);
                String var52 = var51.replace("${empty.coerce.format}", "empty.coerce.format=" + row.get("Date_Formats"));

                String var53 = (dynamicWhereComponent == null || dynamicWhereComponent.equals("NULL")) ? var52 
                    : var52 + "\n" + "cache.type=EHCACHE" + "\n" + "cache.mode=LOCAL" + "\n" + "max.elements.in.memory=3000" + "\n" + "time.to.idle.seconds=5";

                String var54 = var53.replace("${empty.coerce.back}", "empty.coerce.back=" + row.get("PK_Datatypes"));
                String var55 = var54.replace("${empty.mapping.coerce.decimal.precisions}", "empty.mapping.coerce.decimal.precisions=" + row.get("precisions"));
                String var56 = var55.replace("${empty.mapping.coerce.decimal.scales}", "empty.mapping.coerce.decimal.scales=" + row.get("scales"));

                String whereCondition = "where.condition=" + 
                    (writeMode.equals("overwrite") ? "" : "where Updated_Date>=${start.date}");

                String IncrementalCondition = var56.replace("${where.condition}", whereCondition.replace("$", "\\$"));



                // Output
                String valueFile = IncrementalCondition;
                String fileName = getValueFileName(ilTableName, VALUE_FILE_STRING);

                writeToFile(valueFile, fileName);
                
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId);
                resultRow.put("TABLE_SCHEMA", schemaName);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);
                finalResults.add(resultRow);

            }
            return finalResults;
        }

        /* Dimension Deletes Values */
        void dimDeleteScriptComplete(Connection connection, String selectiveTables, 
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Dimension Deletes Values..");
            String dimensionTransaction = "D";
            String jobType = "Deletes_Dim";
            System.out.println("dimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            // TODO: it is different in two cases based on dimensionTransaction
            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            Map<String, Map<String, String>> groupedScripts = new HashMap<>();
            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                Boolean deleteFlag = getDeleteFlag(ilTableName);
                System.out.println("deleteFlag: " + deleteFlag);
                if (deleteFlag != true) {
                    System.out.println("    Delete Flag is false for ilTable: " + ilTableName + ". Hence, skipping.");
                    continue;
                }

                // TODO it is different in two cases based on jobType
                String lhs = getValueNameFromJobPropertiesInfo(connection, jobType);
                // System.out.println("    lhs: " + lhs);

                // String fileName = getDeletesValueFileName(ilTableName);
                // System.out.println("deletes value fileName: " + fileName);

                Map<String, AggregatedData> mainDataMap = getAggregatedMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    mainDataMap: " + mainDataMap.size());
                // System.out.println("    mainDataMap: " + mainDataMap);

                Map<String, ConstantField> constantFieldsLookupMap = getConstantFieldsMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap.size());
                // System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap);
    
                Map<String, PKColumnsData> pkColumnsLookupMap = getPKColumnsMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap.size());
                // System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap);
    
            // TODO: it is different in two cases based on mapping data
                List<Map<String, Object>> finalData = dimFinalDataMapping(mainDataMap, constantFieldsLookupMap, pkColumnsLookupMap, lhs);
                System.out.println("    finalData: " + finalData.size());
                // System.out.println("    finalData: " + finalData);
                // TODO delete the properties beforehand
                insertIntoEltValuesProperties(conn, finalData);
            }
        }
        /* Transaction Deletes Values Script */
        void transDeleteScriptComplete(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            System.out.println("\n..Transaction Deletes Values..");
            String dimensionTransaction = "T";
            String jobType = "Deletes_Trans";
            System.out.println("dimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                Boolean deleteFlag = getDeleteFlag(ilTableName);
                System.out.println("deleteFlag: " + deleteFlag);
                if (deleteFlag != true) {
                    System.out.println("    Delete Flag is false for ilTable: " + ilTableName + ". Hence, skipping.");
                    continue;
                }

                // TODO it is different in two cases based on jobType
                String lhs = getValueNameFromJobPropertiesInfo(connection, jobType);
                // System.out.println("    lhs: " + lhs);

                // String fileName = getDeletesValueFileName(ilTableName);
                // System.out.println("deletes value fileName: " + fileName);

                Map<String, AggregatedData> mainDataMap = getAggregatedMappingInfo(connection, ilTableName,
                        connectionId, querySchemaCond);
                System.out.println("    mainDataMap: " + mainDataMap.size());
                // System.out.println("    mainDataMap: " + mainDataMap);

                Map<String, ConstantField> constantFieldsLookupMap = getConstantFieldsMappingInfo(connection,
                        ilTableName, connectionId, querySchemaCond);
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap.size());
                // System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap);

                Map<String, PKColumnsData> pkColumnsLookupMap = getPKColumnsMappingInfo(connection, ilTableName,
                        connectionId, querySchemaCond);
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap.size());
                // System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap);

                // TODO: it is different in two cases based on mapping data
                List<Map<String, Object>> finalData = transFinalDataMapping(mainDataMap, constantFieldsLookupMap,
                        pkColumnsLookupMap, lhs);
                System.out.println("    finalData: " + finalData.size());
                // System.out.println("    finalData: " + finalData);

                insertIntoEltValuesProperties(conn, finalData);
            }
        }

        // Good One, with jobType argument, valid for both Trans and Dim Delete Values
        private String getValueNameFromJobPropertiesInfo(Connection connection, String jobType) throws SQLException {
            String query = "SELECT Value_Name \n" +
                           "FROM ELT_Job_Properties_Info \n" +
                           "WHERE Job_Type = ? \n" +
                           "  AND Active_Flag = 1 \n" +
                           "  AND Dynamic_Flag = 1";
        
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, jobType);
        
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    StringBuilder finalValueName = new StringBuilder();
                    while (resultSet.next()) {
                        String valueName = resultSet.getString("Value_Name");
                        if (finalValueName.length() > 0) {
                            finalValueName.append("\n"); // TODO: newline character?
                        }
                        finalValueName.append(valueName);
                    }
                    return finalValueName.toString();
                }
            }
        }
        // DIM delete Values - main component start
        // row3, row4 (main)
        private Map<String, AggregatedData> getAggregatedMappingInfo(Connection connection, String ilTableName, String connectionId, String querySchemaCond) throws SQLException {
            String mainQuery = "SELECT " +
                           "  Connection_Id, " +
                           "  TABLE_SCHEMA, " +
                           "  IL_Table_Name, " +
                           "  IL_Column_Name, " +
                           "  IL_Data_Type, " +
                           "  LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
                           "FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "WHERE IL_Table_Name = ? " +
                           "  AND Constraints != 'SK' " +
                           "  AND Connection_Id = ? " +
                           querySchemaCond;
        
            Map<String, AggregatedData> aggregatedResult = new HashMap<>();
        
            Map<String, Map<String, String>> dataTypeConversions = getDatatypeConversions(connection);
            System.out.println("dataTypeConversions: " + dataTypeConversions.size());




            try (PreparedStatement preparedStatement = connection.prepareStatement(mainQuery)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
        
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String connectionIdValue = resultSet.getString("Connection_Id");
                        String tableSchema = resultSet.getString("TABLE_SCHEMA");
                        String ilTableNameValue = resultSet.getString("IL_Table_Name");
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String ilDataType = resultSet.getString("IL_Data_Type");
                        String datatype = resultSet.getString("Datatype");
                        String tiltIlColumnName = "`" + ilColumnName + "`"; // Derived field  "`"+row3.IL_Column_Name+"`"
        
                        // aggregation key
                        String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                        // System.out.println("   key   : " + key);
                        AggregatedData data = aggregatedResult.computeIfAbsent(key, k -> new AggregatedData(connectionIdValue, tableSchema, ilTableNameValue));
                        data.addIlColumnName(ilColumnName);
                        data.addIlDataType(ilDataType);
                        data.addDatatype(datatype);
                        data.addTiltIlColumnName(tiltIlColumnName);
                        //System.out.println(data.toString());

                    }
                    System.out.println("aggregatedResult size : " + aggregatedResult.size());

                }
            }
        
            return aggregatedResult;
        }

        private List<Map<String, Object>> dimFinalDataMapping(Map<String, AggregatedData> dataMap, Map<String, ConstantField> constantFieldsLookupMap, Map<String, PKColumnsData> pkColumnsLookupMap, String lhs) {
           
            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            for (Map.Entry<String, AggregatedData> entry : dataMap.entrySet()) {
                String key = entry.getKey();
                AggregatedData value = entry.getValue();
                // Process the key
                System.out.println("Key: " + key);
                final String connectionId = value.getConnectionId();
                final String tableSchema = value.getTableSchema();
                final String ilTableName = value.getIlTableName();
                final String tiltIlColumnName = value.getTiltIlColumnNames();

                ConstantField constantFields = constantFieldsLookupMap.computeIfAbsent(key, k -> new ConstantField(connectionId, tableSchema, ilTableName));
                PKColumnsData pkColumns = pkColumnsLookupMap.computeIfAbsent(key, k -> new PKColumnsData(connectionId, tableSchema, ilTableName));
    
                // Process the value (AggregatedData object)
                if (value != null) {
                    // Example: Access fields or methods of AggregatedData
                    // System.out.println("Value: " + value.toString()); // Adjust as per AggregatedData's fields
                }

                String var1 = lhs;
                String query = "Select " + tiltIlColumnName + ", True as DeleteIndicator from Joined_Output where " 
                            + pkColumns.getWhereCondition();
                String columns = var1.replaceAll("\\$\\{columns}", "columns=" 
                            + tiltIlColumnName.replaceAll("\\$", "\\\\\\$")); // Escaping '$' in a regex
                String tableName = columns.replaceAll("\\$\\{table.name}",
                        "table.name=`" + ilTableName + "`");
                String mappingConstantFields = tableName.replaceAll("\\$\\{mapping.constants.fields}",
                        "mapping.constants.fields=" + constantFields.getIlColumnName());
                String mappingConstantFieldsTypes = mappingConstantFields.replaceAll(
                        "\\$\\{mapping.constants.fields.types}",
                        "mapping.constants.fields.types=" + constantFields.getJavaDataType());
                String mappingConstantFieldsValues = mappingConstantFieldsTypes.replaceAll(
                        "\\$\\{mapping.constants.fields.values}",
                        "mapping.constants.fields.values=" + constantFields.getConstantInsertValue());
                String sourceCoerceFields = mappingConstantFieldsValues.replaceAll(
                        "\\$\\{source.coerce.fields}",
                        "source.coerce.fields=" + pkColumns.getIlColumnName());
                String sourceCoerceTo = sourceCoerceFields.replaceAll("\\$\\{source.coerce.to}",
                        "source.coerce.to=" + pkColumns.getCoerceTo());
                String sourceCoerceFormat = sourceCoerceTo.replaceAll("\\$\\{source.coerce.format}",
                        "source.coerce.format=" + pkColumns.getCoerceFormat());
                String whereStgFields = sourceCoerceFormat.replaceAll("\\$\\{where.stg.fields}",
                        "where.stg.fields=" + pkColumns.getIlColumnName());
                String whereS3Fields = whereStgFields.replaceAll("\\$\\{where.s3.fields}",
                        "where.s3.fields=" + pkColumns.getIlColumnName());
                String deleteFlagQuery = whereS3Fields.replaceAll("\\$\\{deleteflag.query}",
                        "deleteflag.query=" + query);
                String stgTargetTableDelete = deleteFlagQuery.replaceAll("\\$\\{stg.target.table.delete}",
                        "stg.target.table.delete=" + ilTableName + "_Stg");
                String dwTargetTableDelete = stgTargetTableDelete.replaceAll("\\$\\{dw.target.table.delete}",
                        "dw.target.table.delete=" + ilTableName);
                String dwTargetTableInsert = dwTargetTableDelete.replaceAll("\\$\\{dw.target.table.insert}",
                        "dw.target.table.insert=" + ilTableName + "_Deletes");
                String deleteFieldName = dwTargetTableInsert.replaceAll("\\$\\{delete.field.name}",
                        "delete.field.name=DeleteIndicator");
                String mappingRetainEmit = deleteFieldName.replaceAll("\\$\\{mapping.retain.emit}",
                        "mapping.retain.emit=DeleteIndicator");
                String insertConstantColumns = mappingRetainEmit.replaceAll("\\$\\{insert.constant.columns}",
                        "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                String insertConstantStoreValues = insertConstantColumns.replaceAll(
                        "\\$\\{insert.constant.store.values}",
                        "insert.constant.store.values=\"CONVERT_TZ(sysdate(),\"\"UTC\"\",\"\"Africa/Abidjan\"\")\",'ELT_Admin',"
                                +
                                "\"CONVERT_TZ(sysdate(),\"\"UTC\"\",\"\"Africa/Abidjan\"\")\",'ELT_Admin'");
                String insertConstantStoreTypes = insertConstantStoreValues.replaceAll(
                        "\\$\\{insert.constant.store.types}",
                        "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");
                String batchType = insertConstantStoreTypes.replaceAll("\\$\\{batch.type}",
                        "batch.type=BATCH_BY_SIZE");
                String batchSize = batchType.replaceAll("\\$\\{batch.size}", "batch.size=10000");

                // out4
                String valueFile = batchSize;
                // System.out.println("...    valueFile  : " + valueFile);
                String fileName = getValueFileName(ilTableName, DELETES_VALUE_FILE_STRING);
                // System.out.println("deletes value fileName: " + fileName);
                writeToFile(valueFile, fileName);

                // out3
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                resultRow.put("IL_Table_Name", ilTableName);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);

                finalResults.add(resultRow);
            }

            return finalResults;
        }

        // Trans_Deletes_values - tMap3 - out3
        private List<Map<String, Object>> transFinalDataMapping(Map<String, AggregatedData> dataMap, Map<String, ConstantField> constantFieldsLookupMap, Map<String, PKColumnsData> pkColumnsLookupMap, String lhs) {
           
            List<Map<String, Object>> finalResults = new ArrayList<>();

            String addedUser = userName;
            Timestamp addedDate = Timestamp.valueOf(startTime);
            String updatedUser = userName;
            Timestamp updatedDate = Timestamp.valueOf(startTime);

            for (Map.Entry<String, AggregatedData> entry : dataMap.entrySet()) {
                String key = entry.getKey();
                AggregatedData value = entry.getValue();
                // Process the key
                System.out.println("Key: " + key);
                final String connectionId = value.getConnectionId();
                final String tableSchema = value.getTableSchema();
                final String ilTableName = value.getIlTableName();
                final String tiltIlColumnName = value.getTiltIlColumnNames();

                ConstantField constantFields = constantFieldsLookupMap.computeIfAbsent(key, k -> new ConstantField(connectionId, tableSchema, ilTableName));
                PKColumnsData pkColumns = pkColumnsLookupMap.computeIfAbsent(key, k -> new PKColumnsData(connectionId, tableSchema, ilTableName));
    

                String var1 = lhs;
                String mappingConstantsFields = var1.replaceAll("\\$\\{mapping.constants.fields}", "mapping.constants.fields=" + constantFields.getIlColumnName());
                String mappingConstantsFieldsTypes = mappingConstantsFields.replaceAll("\\$\\{mapping.constants.fields.types}", "mapping.constants.fields.types=" + constantFields.getJavaDataType());
                String mappingConstantsFieldsValues = mappingConstantsFieldsTypes.replaceAll("\\$\\{mapping.constants.fields.values}", "mapping.constants.fields.values=" + constantFields.getConstantInsertValue());
                String emptyMappingConstantsFields = mappingConstantsFieldsValues.replaceAll("\\$\\{empty.mapping.constants.fields}", "empty.mapping.constants.fields=underscore_field");
                String emptyMappingConstantsFieldsTypes = emptyMappingConstantsFields.replaceAll("\\$\\{empty.mapping.constants.fields.types}", "empty.mapping.constants.fields.types=java.lang.String");
                String emptyMappingConstantsFieldsValues = emptyMappingConstantsFieldsTypes.replaceAll("\\$\\{empty.mapping.constants.fields.values}", "empty.mapping.constants.fields.values=_");
                String emptyCoerceFields = emptyMappingConstantsFieldsValues.replaceAll("\\$\\{empty.coerce.fields}", "empty.coerce.fields=" + pkColumns.getIlColumnName().replaceAll("\\$", "\\\\\\$"));
                String emptyCoerceTo = emptyCoerceFields.replaceAll("\\$\\{empty.coerce.to}", "empty.coerce.to=" + pkColumns.getCoerceTo());
                String emptyCoerceFormat = emptyCoerceTo.replaceAll("\\$\\{empty.coerce.format}", "empty.coerce.format=" + pkColumns.getCoerceFormat());
                String emptyCoerceBack = emptyCoerceFormat.replaceAll("\\$\\{empty.coerce.back}", "empty.coerce.back=" + pkColumns.getCoerceBack());
                String emptyMappingCoerceDecimalPrecisions = emptyCoerceBack.replaceAll("\\$\\{empty.mapping.coerce.decimal.precisions}", "empty.mapping.coerce.decimal.precisions=" + pkColumns.getDecimalPrecision());
                String emptyMappingCoerceDecimalScales = emptyMappingCoerceDecimalPrecisions.replaceAll("\\$\\{empty.mapping.coerce.decimal.scales}", "empty.mapping.coerce.decimal.scales=" + pkColumns.getDecimalScale());
                String resultFetcherClassNames = emptyMappingCoerceDecimalScales.replaceAll("\\$\\{resultfetcher.class.names}", "resultfetcher.class.names=java.lang.String");
                String resultFetcherMethodNames = resultFetcherClassNames.replaceAll("\\$\\{resultfetcher.method.names}", "resultfetcher.method.names=join");
                String resultFetcherMethodArgumentFields = resultFetcherMethodNames.replaceAll("\\$\\{resultfetcher.method.argument.fields}", "resultfetcher.method.argument.fields=\"underscore_field," + pkColumns.getIlColumnName().replaceAll("\\$", "\\\\\\$") + "\"");
                String resultFetcherReturnFields = resultFetcherMethodArgumentFields.replaceAll("\\$\\{resultfetcher.return.fields}", "resultfetcher.return.fields=PKValue");
                String selectColumns = resultFetcherReturnFields.replaceAll("\\$\\{select.columns}", "select.columns=" + tiltIlColumnName.replaceAll("\\$", "\\\\\\$"));
                String whereColumns = selectColumns.replaceAll("\\$\\{where.columns}", "where.columns=" + pkColumns.getIlColumnName().replaceAll("\\$", "\\\\\\$"));
                String lookupTable = whereColumns.replaceAll("\\$\\{lookup.table}", "lookup.table=`" + ilTableName.replaceAll("\\$", "\\\\\\$") + "`");
                String whereFields = lookupTable.replaceAll("\\$\\{where.fields}", "where.fields=" + pkColumns.getIlColumnName().replaceAll("\\$", "\\\\\\$"));
                
                String deleteSelectQuery = "Select " + tiltIlColumnName + ",True as DeleteIndicator,PKValue from Joined_Output where " + pkColumns.getWhereCondition();
                String deleteStgKeysSelectQuery = "Select True as DeleteIndicator,PKValue from Joined_Output_Stg_Keys where " + pkColumns.getWhereCondition();
                
                String deleteFlagQuery = whereFields.replaceAll("\\$\\{deleteflag.query}", "deleteflag.query=" + deleteSelectQuery.replaceAll("\\$", "\\\\\\$"));
                String deleteFlagStgKeysQuery = deleteFlagQuery.replaceAll("\\$\\{deleteflag_stg_keys.query}", "deleteflag_stg_keys.query=" + deleteStgKeysSelectQuery.replaceAll("\\$", "\\\\\\$"));
                String stgKeysDeleteFieldName = deleteFlagStgKeysQuery.replaceAll("\\$\\{stg.keys.delete.field.name}", "stg.keys.delete.field.name=DeleteIndicator");
                String deleteFieldName = stgKeysDeleteFieldName.replaceAll("\\$\\{delete.field.name}", "delete.field.name=DeleteIndicator");
                String stgKeysTargetTableDelete = deleteFieldName.replaceAll("\\$\\{stgkeys.target.table.delete}", "stgkeys.target.table.delete=" + ilTableName + "_Stg_Keys");
                String dwTargetTableDelete = stgKeysTargetTableDelete.replaceAll("\\$\\{dw.target.table.delete}", "dw.target.table.delete=" + ilTableName);
                String mappingRetainEmit = dwTargetTableDelete.replaceAll("\\$\\{mapping.retain.emit}", "mapping.retain.emit=DeleteIndicator,PKValue");
                String insertConstantColumns = mappingRetainEmit.replaceAll("\\$\\{insert.constant.columns}", "insert.constant.columns=Added_Date,Added_User,Updated_Date,Updated_User");
                String insertConstantStoreValues = insertConstantColumns.replaceAll("\\$\\{insert.constant.store.values}", "insert.constant.store.values=\"CONVERT_TZ(sysdate(),\"\"UTC\"\",\"\"Africa/Abidjan\"\")\",'ELT_Admin',\"CONVERT_TZ(sysdate(),\"\"UTC\"\",\"\"Africa/Abidjan\"\")\",'ELT_Admin'");
                String insertConstantStoreTypes = insertConstantStoreValues.replaceAll("\\$\\{insert.constant.store.types}", "insert.constant.store.types=java.util.Date,java.lang.String,java.util.Date,java.lang.String");
                String deleteAuditTable = insertConstantStoreTypes.replaceAll("\\$\\{delete.audit.table}", "delete.audit.table=" + ilTableName + "_Deletes");            

                // valuefile
                String valueFile = deleteAuditTable;
                // System.out.println("...    valueFile  : " + valueFile);
                String fileName = getValueFileName(ilTableName, DELETES_VALUE_FILE_STRING);
                System.out.println("deletes value fileName: " + fileName);
                writeToFile(valueFile, fileName);

                // out3
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Connection_Id", connectionId);
                resultRow.put("TABLE_SCHEMA", tableSchema);
                String ilTableNameUpdated = ilTableName + "_Deletes";
                resultRow.put("IL_Table_Name", ilTableNameUpdated);
                resultRow.put("values_file_name", fileName);
                resultRow.put("Active_Flag", true);
                resultRow.put("Added_Date", addedDate);
                resultRow.put("Added_User", addedUser);
                resultRow.put("Updated_Date", updatedDate);
                resultRow.put("Updated_User", updatedUser);

                finalResults.add(resultRow);
            }

            return finalResults;
        }
        
        // constantfields lookup
        private Map<String, ConstantField> getConstantFieldsMappingInfo(Connection connection, String ilTableName,
                String connectionId, String querySchemaCond) throws SQLException {
            // Call getDatatypeConversions() to retrieve the datatype conversion mappings
            // TODO make one call of this function
            Map<String, Map<String, String>> datatypeConversions = getDatatypeConversions(connection);

            String query = "SELECT " +
                    "  Connection_Id, " +
                    "  TABLE_SCHEMA, " +
                    "  IL_Table_Name, " +
                    "  IL_Column_Name, " +
                    "  IL_Data_Type, " +
                    "  LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype, " +
                    "  Constant_Insert_Value " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE IL_Table_Name = ? " +
                    "  AND Constant_Insert_Column = 'Y' " +
                    "  AND Connection_Id = ? " +
                    querySchemaCond;
        
            Map<String, ConstantField> constantFieldsAggregationMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String connectionIdValue = resultSet.getString("Connection_Id");
                        String tableSchema = resultSet.getString("TABLE_SCHEMA");
                        String ilTableNameValue = resultSet.getString("IL_Table_Name");
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String ilDataType = resultSet.getString("IL_Data_Type");
                        String datatype = resultSet.getString("Datatype");
                        String constantInsertValue = resultSet.getString("Constant_Insert_Value");

                        // the left join with datatypeConversions
                        Map<String, String> conversionDetails = datatypeConversions.get(datatype);
                        // TODO here value could be null, handle this value in downstream
                        String javaDataType = null;
                        if (conversionDetails != null) {
                            javaDataType = (String) conversionDetails.get("javaDataType"); // TODO set value
                        }

                        // Aggregation key
                        String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                        ConstantField constantFieldsData = constantFieldsAggregationMap.getOrDefault(key, new ConstantField(connectionIdValue, tableSchema, ilTableNameValue));
                        constantFieldsData.ilColumnName.append(constantFieldsData.ilColumnName.length() > 0 ? ", " : "").append(ilColumnName);
                        constantFieldsData.ilDataType.append(constantFieldsData.ilDataType.length() > 0 ? ", " : "").append(ilDataType);
                        constantFieldsData.datatype.append(constantFieldsData.datatype.length() > 0 ? ", " : "").append(datatype);
                        constantFieldsData.constantInsertValue.append(constantFieldsData.constantInsertValue.length() > 0 ? ", " : "").append(constantInsertValue);
                        if (javaDataType != null) { // left outer join
                            constantFieldsData.javaDataType
                                    .append(constantFieldsData.javaDataType.length() > 0 ? ", " : "")
                                    .append(javaDataType);
                        }
                        constantFieldsAggregationMap.put(key, constantFieldsData);
                    }
                }
            }
            return constantFieldsAggregationMap;
        }
        //PKColumns lookup 
        private Map<String, PKColumnsData> getPKColumnsMappingInfo(Connection connection, String ilTableName, String connectionId, String querySchemaCond) throws SQLException {
            
            // Call getDatatypeConversions() to retrieve the datatype conversion mappings
            // TODO make one call of this function
            Map<String, Map<String, String>> datatypeConversions = getDatatypeConversions(connection);
        
            String query = "SELECT " +
                           "  Connection_Id, " +
                           "  TABLE_SCHEMA, " +
                           "  IL_Table_Name, " +
                           "  IL_Column_Name, " +
                           "  IL_Data_Type, " +
                           "  LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype, " +
                           "  SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(IL_Data_Type, '(', -1), ')', 1), ',', 1) AS Precision_Val, " +
                           "  SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(IL_Data_Type, '(', -1), ')', 1), ',', -1) AS Scale_Val " +
                           "FROM ELT_IL_Source_Mapping_Info_Saved " +
                           "WHERE IL_Table_Name = ? " +
                           "  AND Constraints = 'PK' " +
                           "  AND Connection_Id = ? " +
                           querySchemaCond;
        
            Map<String, PKColumnsData> pkColumnsAggregationMap = new HashMap<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
        

                String whereCondition = null;
                String coerceFormat = null;
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        // Extract fields from the result set
                        String connectionIdValue = resultSet.getString("Connection_Id");
                        String tableSchema = resultSet.getString("TABLE_SCHEMA");
                        String ilTableNameValue = resultSet.getString("IL_Table_Name");
                        String ilColumnName = resultSet.getString("IL_Column_Name");
                        String ilDataType = resultSet.getString("IL_Data_Type");
                        String datatype = resultSet.getString("Datatype");

                        String coerceTo = "java.lang.String";

                        String precisionVal = resultSet.getString("Precision_Val");
                        String scaleVal = resultSet.getString("Scale_Val");

                        // the left join with datatypeConversions
                        Map<String, String> conversionDetails = datatypeConversions.get(datatype);
                        String coerceBack = null;
                        if (conversionDetails != null) {
                            String javaDataType = (String) conversionDetails.get("javaDataType");
                            coerceBack = javaDataType;
                        }
        
                        // Determine precision and scale based on IL_Data_Type
                        ilDataType = ilDataType.toLowerCase();
                        String decimalPrecision = (ilDataType.contains("decimal") || ilDataType.contains("float") || ilDataType.contains("double"))
                                ? precisionVal
                                : "";
                        String decimalScale = (ilDataType.contains("decimal") || ilDataType.contains("float") || ilDataType.contains("double"))
                                ? scaleVal
                                : "";

                        // last value of below fields are used
                        String cond = "`rhs_" + ilColumnName + "` is null";
                        whereCondition = (whereCondition == null) 
                                ? cond : whereCondition + " and " + cond;

                        // trim the last character from coerceFormat
                        // Not dependent on any input value
                        coerceFormat = (coerceFormat == null)
                                ? "," : coerceFormat + ",";
                        int len = coerceFormat.length() - 1;
                        coerceFormat = coerceFormat.substring(0, len);

                        // Aggregation key
                        String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;
                        //pkColumnsAggregationMap.computeIfAbsent(key, k -> new PKColumnsData(connectionIdValue, tableSchema, ilTableNameValue));
                        PKColumnsData columnsData = pkColumnsAggregationMap.getOrDefault(key, new PKColumnsData(connectionIdValue, tableSchema, ilTableNameValue));

                        //PKColumnsData columnsData = pkColumnsAggregationMap.get(key);
                        // Aggregating fields
                        columnsData.ilColumnName.append(columnsData.ilColumnName.length() > 0 ? ", " : "").append(ilColumnName);
                        columnsData.ilDataType.append(columnsData.ilDataType.length() > 0 ? ", " : "").append(ilDataType);
                        columnsData.datatype.append(columnsData.datatype.length() > 0 ? ", " : "").append(datatype);
                        columnsData.coerceTo.append(columnsData.coerceTo.length() > 0 ? ", " : "").append(coerceTo);
                        if (coerceBack != null) { // left outer join
                            columnsData.coerceBack.append(columnsData.coerceBack.length() > 0 ? ", " : "")
                                    .append(coerceBack);
                        }
                        columnsData.decimalPrecision.append(columnsData.decimalPrecision.length() > 0 ? ", " : "").append(decimalPrecision);
                        columnsData.decimalScale.append(columnsData.decimalScale.length() > 0 ? ", " : "").append(decimalScale);
                        // last values
                        columnsData.coerceFormat = coerceFormat;
                        columnsData.whereCondition = whereCondition;

                        pkColumnsAggregationMap.put(key, columnsData);
                    }
                }
            }
        
            return pkColumnsAggregationMap;
        }

        /**
         * Inserts multiple rows into the specified table using a dynamically
         * constructed SQL INSERT statement. Should below be replaced.
         */
        // Inserting data into database
        private boolean insertIntoEltValuesProperties(Connection conn, List<Map<String, Object>> rowDetails) {
            String insertSql = "INSERT INTO ELT_VALUES_PROPERTIES (Connection_Id, TABLE_SCHEMA, IL_Table_Name, values_file_name, Active_Flag, Added_Date, Added_User, Updated_Date, Updated_User) " +
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            int rowsAffected = 0;
            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                for (Map<String, Object> row : rowDetails) {

                    insertPs.setString(1, (String) row.get("Connection_Id"));
                    insertPs.setString(2, (String) row.get("TABLE_SCHEMA"));
                    insertPs.setString(3, (String) row.get("IL_Table_Name"));
                    insertPs.setString(4, (String) row.get("values_file_name"));
                    insertPs.setBoolean(5, (Boolean) row.get("Active_Flag").equals(true));
                    insertPs.setTimestamp(6, (Timestamp) row.get("Added_Date"));
                    insertPs.setString(7, (String) row.get("Added_User"));
                    insertPs.setTimestamp(8, (Timestamp) row.get("Updated_Date"));
                    insertPs.setString(9, (String) row.get("Updated_User"));

                    insertPs.addBatch();

                }
                int[] result = insertPs.executeBatch();
                rowsAffected = getNumberOfRowsUpdated(result);
                System.out.println("Total Rows " + rowsAffected + " inserted into the table " + "ELT_VALUES_PROPERTIES");

                // insertPs.executeUpdate();
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        class ConstantField {
            String connectionId = new String();
            String tableSchema = new String();
            String ilTableName = new String();
            StringBuilder ilColumnName = new StringBuilder();;
            StringBuilder ilDataType = new StringBuilder();
            StringBuilder datatype = new StringBuilder() ;
            StringBuilder constantInsertValue = new StringBuilder();
            StringBuilder javaDataType = new StringBuilder();
            // Getters
            public String getIlColumnName() {
                return ilColumnName.toString();
            }
            public String getIlDataType() {
                return ilDataType.toString();
            }
            public String getDatatype() {
                return datatype.toString();
            }
            public String getConstantInsertValue() {
                return constantInsertValue.toString();
            }
            public String getJavaDataType() {
                return javaDataType.toString();
            }
            // ctor
            public ConstantField(String connectionId, String tableSchema, String ilTableName) {
                this.connectionId = connectionId;
                this.tableSchema = tableSchema;
                this.ilTableName = ilTableName;
            }
        }

        class PKColumnsData {
            String connectionId = new String();
            String tableSchema = new String();
            String ilTableName = new String();
            String whereCondition;
            String coerceFormat;
            StringBuilder ilColumnName;
            StringBuilder ilDataType;
            StringBuilder datatype;
            StringBuilder coerceTo;
            StringBuilder coerceBack;
            StringBuilder decimalPrecision;
            StringBuilder decimalScale;

            public String getWhereCondition() {
                return whereCondition;
            }
            public String getCoerceFormat() {
                return coerceFormat;
            }
            public String getIlColumnName() {
                return ilColumnName.toString();
            }
            public String getIlDataType() {
                return ilDataType.toString();
            }
            public String getDatatype() {
                return datatype.toString();
            }
            public String getCoerceTo() {
                return coerceTo.toString();
            }
            public String getCoerceBack() {
                return coerceBack.toString();
            }
            public String getDecimalPrecision() {
                return decimalPrecision.toString();
            }
            public String getDecimalScale() {
                return decimalScale.toString();
            }

            public PKColumnsData(String connectionId, String tableSchema, String ilTableName) {
                this.connectionId = connectionId;
                this.tableSchema = tableSchema;
                this.ilTableName = ilTableName;
                this.whereCondition = new String();
                this.coerceFormat = new String();
                this.ilColumnName = new StringBuilder();
                this.ilDataType = new StringBuilder();
                this.datatype = new StringBuilder();
                this.coerceTo = new StringBuilder();
                this.coerceBack = new StringBuilder();
                this.decimalPrecision = new StringBuilder();
                this.decimalScale = new StringBuilder();
            }
        }

        // Helper class to hold aggregated data
        private class AggregatedData {
            private final String connectionId;
            private final String tableSchema;
            private final String ilTableName;
            private final StringBuilder ilColumnNames = new StringBuilder();
            private final StringBuilder ilDataTypes = new StringBuilder();
            private final StringBuilder datatypes = new StringBuilder();
            private final StringBuilder tiltIlColumnNames = new StringBuilder();

            public AggregatedData(String connectionId, String tableSchema, String ilTableName) {
                this.connectionId = connectionId;
                this.tableSchema = tableSchema;
                this.ilTableName = ilTableName;
            }
            
            public String getConnectionId() {
                return connectionId;
            }
            public String getTableSchema() {
                return tableSchema;
            }
            public String getIlTableName() {
                return ilTableName;
            }
            public String getIlColumnNames() {
                return ilColumnNames.toString();
            }
            public String getIlDataTypes() {
                return ilDataTypes.toString();
            }
            public String getDatatypes() {
                return datatypes.toString();
            }
            public String getTiltIlColumnNames() {
                return tiltIlColumnNames.toString();
            }

            public void addIlColumnName(String value) {
                appendWithComma(ilColumnNames, value);
            }
            public void addIlDataType(String value) {
                appendWithComma(ilDataTypes, value);
            }
            public void addDatatype(String value) {
                appendWithComma(datatypes, value);
            }
            public void addTiltIlColumnName(String value) {
                appendWithComma(tiltIlColumnNames, value);
            }
        
            private void appendWithComma(StringBuilder builder, String value) {
                if (builder.length() > 0) {
                    builder.append(",");
                }
                builder.append(value);
            }
        
            @Override
            public String toString() {
                return "AggregatedData{" +
                       "connectionId='" + connectionId + '\'' +
                       ", tableSchema='" + tableSchema + '\'' +
                       ", ilTableName='" + ilTableName + '\'' +
                       ", ilColumnNames='" + ilColumnNames + '\'' +
                       ", ilDataTypes='" + ilDataTypes + '\'' +
                       ", datatypes='" + datatypes + '\'' +
                       ", tiltIlColumnNames='" + tiltIlColumnNames + '\'' +
                       '}';
            }
        }

        public class SourceMapValues {
            // Key fields
            private final String connectionIdValue;
            private final String tableSchema;
            private final String ilTableNameValue;
        
            public String getConnectionId() {
                return connectionIdValue;
            }

            public String getTableSchema() {
                return tableSchema;
            }

            public String getIlTableName() {
                return ilTableNameValue;
            }

            public SourceMapValues(String connectionIdValue, String tableSchema, String ilTableNameValue) {
                this.connectionIdValue = connectionIdValue;
                this.tableSchema = tableSchema;
                this.ilTableNameValue = ilTableNameValue;
                // Initialize to "", having null doesn't mean anything
                fields = ""; values = ""; dateformats = "";

            }

            public SourceMapValues() {
                connectionIdValue ="";
                tableSchema = "";
                ilTableNameValue = "";
            }

            String fields;
            String values;
            String dateformats;

            public String getFields() {
                return fields;
            }

            public void setFields(String fields) {
                this.fields = fields;
            }

            public String getValues() {
                return values;
            }

            public void setValues(String values) {
                this.values = values;
            }

            public String getDateformats() {
                return dateformats;
            }

            public void setDateformats(String dateformats) {
                this.dateformats = dateformats;
            }

            // Method to append values to aggregated - fields
            public void appendToFields(String value) {
                if (value != null && !value.isEmpty()) {
                    if (this.fields == null || this.fields.isEmpty()) {
                        this.fields = value;
                    } else {
                        this.fields += "," + value;
                    }
                }
            }

            // Methods to append values to aggregated - values
            public void appendToValues(String value) {
                if (value != null && !value.isEmpty()) {
                    if (this.values == null || this.values.isEmpty()) {
                        this.values = value;
                    } else {
                        this.values += "," + value;
                    }
                }
            }
        }

        public class DimSourceValues {
            // Key fields
            private final String connectionIdValue;
            private final String tableSchema;
            private final String ilTableNameValue;
        
            public String getConnectionId() {
                return connectionIdValue;
            }

            public String getTableSchema() {
                return tableSchema;
            }

            public String getIlTableName() {
                return ilTableNameValue;
            }

            // Other fields
            private String dateformat;
            private String allCols;
            private String allExceptPk;
            private String allPk;
            private String cleansingValues;
            private String pkDateformats;
            private String cleansingValidations;
            private String allJavaDataTypes;
            private String allDateformats;
            private String stgTableName;
            private String skColumn;
            private String allPkSk;
            private String allCleansingValue;
            private String allColsTilt;

            private String dynamicWhereComponent;
            private String allPkJava;
            private String precisions;
            private String scales;

            public DimSourceValues(String connectionIdValue, String tableSchema, String ilTableNameValue) {
                this.connectionIdValue = connectionIdValue;
                this.tableSchema = tableSchema;
                this.ilTableNameValue = ilTableNameValue;
                // Initialize to "", having null doesn't mean anything
                dateformat = ""; allCols = ""; allExceptPk = ""; allPk = "";
                cleansingValues = ""; pkDateformats = ""; cleansingValidations = ""; allJavaDataTypes = "";
                allDateformats = ""; stgTableName = ""; skColumn = ""; allPkSk = ""; allCleansingValue = "";
                allColsTilt = "";
                dynamicWhereComponent=""; allPkJava=""; precisions=""; scales="";
            }
        
            public String getDateformat() {
                return dateformat;
            }
        
            public void setDateformat(String dateformat) {
                this.dateformat = dateformat;
            }
        
            public String getAllCols() {
                return allCols;
            }
        
            public void setAllCols(String allCols) {
                this.allCols = allCols;
            }
        
            public String getAllExceptPk() {
                return allExceptPk;
            }
        
            public void setAllExceptPk(String allExceptPk) {
                this.allExceptPk = allExceptPk;
            }
        
            public String getAllPk() {
                return allPk;
            }
        
            public void setAllPk(String allPk) {
                this.allPk = allPk;
            }
        
            public String getCleansingValues() {
                return cleansingValues;
            }
        
            public void setCleansingValues(String cleansingValues) {
                this.cleansingValues = cleansingValues;
            }
        
            public String getPkDateformats() {
                return pkDateformats;
            }
        
            // public void setPkDateformats(String pkDateformats) {
            //     this.pkDateformats = pkDateformats;
            // }
        
            public String getCleansingValidations() {
                return cleansingValidations;
            }
        
            // public void setCleansingValidations(String cleansingValidations) {
            //     this.cleansingValidations = cleansingValidations;
            // }
        
            public String getAllJavaDataTypes() {
                return allJavaDataTypes;
            }
        
            public void setAllJavaDataTypes(String allJavaDataTypes) {
                this.allJavaDataTypes = allJavaDataTypes;
            }
        
            public String getAllDateformats() {
                return allDateformats;
            }
        
            public void setAllDateformats(String allDateformats) {
                this.allDateformats = allDateformats;
            }
        
            public String getStgTableName() {
                return stgTableName;
            }
        
            public void setStgTableName(String stgTableName) {
                this.stgTableName = stgTableName;
            }
        
            public String getSkColumn() {
                return skColumn;
            }
        
            public void setSkColumn(String skColumn) {
                this.skColumn = skColumn;
            }
        
            public String getAllPkSk() {
                return allPkSk;
            }
        
            public void setAllPkSk(String allPkSk) {
                this.allPkSk = allPkSk;
            }

            public String getAllCleansingValue() {
                return allCleansingValue;
            }
        
            public void setAllCleansingValue(String allCleansingValue) {
                this.allCleansingValue = allCleansingValue;
            }
        
            public String getAllColsTilt() {
                return allColsTilt;
            }
        
            public void setAllColsTilt(String allColsTilt) {
                this.allColsTilt = allColsTilt;
            }

            public String getDynamicWhereComponent() {
                return dynamicWhereComponent;
            }

            public void setDynamicWhereComponent(String dynamicWhereComponent) {
                this.dynamicWhereComponent = dynamicWhereComponent;
            }

            public String getAllPkJava() {
                return allPkJava;
            }

            public void setAllPkJava(String allPkJava) {
                this.allPkJava = allPkJava;
            }

            public String getPrecisions() {
                return precisions;
            }

            public void setPrecisions(String precisions) {
                this.precisions = precisions;
            }

            public String getScales() {
                return scales;
            }

            public void setScales(String scales) {
                this.scales = scales;
            }

            // Methods to append values to aggregated fields
            public void appendToPkDateformats(String value) {
                if (value != null && !value.isEmpty()) {
                    if (this.pkDateformats == null || this.pkDateformats.isEmpty()) {
                        this.pkDateformats = value;
                    } else {
                        this.pkDateformats += "," + value;
                    }
                }
            }
        
            public void appendToCleansingValidations(String value) {
                if (value != null && !value.isEmpty()) {
                    if (this.cleansingValidations == null || this.cleansingValidations.isEmpty()) {
                        this.cleansingValidations = value;
                    } else {
                        this.cleansingValidations += "," + value;
                    }
                }
            }
        }

        private List<String> getSourceTableName(Connection connection, String connectionId, String ilTableName, String querySchemaCond) {
            List<String> sourceTableNames = new ArrayList<>();
            String query = "SELECT DISTINCT `ELT_IL_Source_Mapping_Info_Saved`.`Source_Table_Name` " +
                           "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                           "WHERE Connection_Id = ? " + querySchemaCond +
                           " AND IL_Table_Name = ? " +
                           " AND (Source_Table_Name IS NOT NULL AND Source_Table_Name != '')";
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                preparedStatement.setString(2, ilTableName);
    
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        sourceTableNames.add(resultSet.getString("Source_Table_Name"));
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return sourceTableNames;
        }

        /**
         * Retrieves data from ELT_IL_Source_Mapping_Info_Saved based on the provided
         * parameters.
         *
         * @param conn                 The database connection object.
         * @param dimensionTransaction The Dimension_Transaction value (e.g., 'D').
         * @param querySchemaCond      The query schema condition (e.g., "AND
         *                             TABLE_SCHEMA = 'your_schema'").
         * @param connectionId         The Connection_Id value.
         * @param ilTableName          The IL_Table_Name value.
         * @return A ResultSet containing the query results, or null if an error occurs.
         */
        private Map<String, DimSourceValues> getDimensionSourceMappingInfo(Connection conn, String dimensionTransaction, String querySchemaCond,
                String connectionId, String ilTableName) {
            
            Map<String, DimSourceValues> aggregatedSourceValues = new HashMap<>();

            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }
            ResultSet rs = null;

            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type, " +
                    "Constraints, Source_Table_Name, Source_Column_Name, Source_Data_Type, PK_Constraint, " +
                    "PK_Column_Name, FK_Constraint, FK_Column_Name, Dimension_Transaction, Dimension_Key, " +
                    "Dimension_Name, Dimension_Join_Condition, Active_Flag, Constant_Insert_Column, " +
                    "Constant_Insert_Value, LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) as Datatype " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = ? " +
                    "AND Constraints <> 'SK' " +
                    "AND (Constant_Insert_Column <> 'Y' OR Constant_Insert_Column IS NULL) " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;

            try {
                PreparedStatement pstmt = conn.prepareStatement(query);

                pstmt.setString(1, dimensionTransaction);
                pstmt.setString(2, ilTableName);
                pstmt.setString(3, connectionId);

                rs = pstmt.executeQuery();
                System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
                Map<String, Map<String, String>> lookupDataMap = getSourceMappingInfoAsMap(
                        conn, dimensionTransaction, querySchemaCond, connectionId, ilTableName);


                String cdcFlag = "";
                while (rs.next()) {
                    String connectionIdValue = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("TABLE_SCHEMA");
                    String ilTableNameValue = rs.getString("IL_Table_Name");
                    String ilColumnName = rs.getString("IL_Column_Name");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String constraints = rs.getString("Constraints");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String sourceColumnName = rs.getString("Source_Column_Name");
                    String sourceDataType = rs.getString("Source_Data_Type");
                    String pkConstraint = rs.getString("PK_Constraint");
                    String pkColumnName = rs.getString("PK_Column_Name");
                    String fkConstraint = rs.getString("FK_Constraint");
                    String fkColumnName = rs.getString("FK_Column_Name");
                    String dimensionTransactionValue = rs.getString("Dimension_Transaction");
                    String dimensionKey = rs.getString("Dimension_Key");
                    String dimensionName = rs.getString("Dimension_Name");
                    String dimensionJoinCondition = rs.getString("Dimension_Join_Condition");
                    boolean activeFlag = rs.getBoolean("Active_Flag");
                    // String cdcFlag = rs.getString("CDC_Flag");
                    String dataType = rs.getString("Datatype");

                    String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;

                    // the left join with lookupDataMap
                    Map<String, String> lookupData = lookupDataMap.getOrDefault(key, new HashMap<>());
                    String pkDatatype = lookupData.get ("IL_Data_Type");

                    // the left join with datatypeConversions
                    Map<String, String> conversionDetails = datatypeConversionsGlobal.get(dataType);
                    String javaDataTypeConversion = null;
                    String pkCleansinghValueConversion = null;
                    if (conversionDetails != null) {
                        javaDataTypeConversion = (String) conversionDetails.get("javaDataType");
                        pkCleansinghValueConversion = (String) conversionDetails.get("pkCleansingValue");
                    }
                    
                    String javaDataType = javaDataTypeConversion;
                    String cleansingValue = pkCleansinghValueConversion;

                    // Transformation
                    String dateformat = dateFormatValue;
                    String allCols = null;
                    String allExceptPk = null;
                    String allPk = null;
                    String cleansingValues = null;
                    String pkDateformats = null;
                    String cleansingValidations = null;
                    String allJavaDataTypes = null;
                    String allDateformats = null;
                    String stgTableName = null;
                    String skColumn = null;
                    String allPkSk = null;

                    allCols = allCols == null ? ilColumnName : (allCols + "," + ilColumnName);

                    allExceptPk = allExceptPk == null ?
                        (constraints.equals("PK") ? null : ilColumnName) :
                        (constraints.equals("PK") ? allExceptPk : (allExceptPk + "," + ilColumnName));

                    allPk = allPk == null ?
                        (constraints.equals("PK") ? ilColumnName : null) :
                        (constraints.equals("PK") ? (allPk + "," + ilColumnName) : allPk);

                    cleansingValues = cleansingValues == null ?
                        (constraints.equals("PK") ? cleansingValue : null) :
                        (constraints.equals("PK") ? (cleansingValues + "," + cleansingValue) : cleansingValues);

                    pkDateformats = pkDatatype == null ? null :
                        (pkDatatype.toLowerCase().contains("date") ? dateformat : "");

                    cleansingValidations = pkDatatype == null ? null : "EMPTY";

                    allJavaDataTypes = allJavaDataTypes == null ?
                        javaDataType : (allJavaDataTypes + "," + javaDataType);

                    allDateformats = allDateformats == null ?
                        (ilDataType.toLowerCase().contains("date") ? dateformat : "") :
                        (allDateformats + "," + (ilDataType.toLowerCase().contains("date") ? dateformat : ""));

                    stgTableName = ilTableName + "_Stg";
                    skColumn = sourceTableName + "_Key";
                    allPkSk = allPk + "," + skColumn;

                    // Aggregation
                    DimSourceValues data = aggregatedSourceValues.computeIfAbsent(key,
                            k -> new DimSourceValues(connectionIdValue, tableSchema, ilTableNameValue));
                    // Last
                    data.setAllCols(allCols);
                    data.setAllExceptPk(allExceptPk);
                    data.setAllPk(allPk);
                    data.setAllJavaDataTypes(allJavaDataTypes);
                    data.setAllDateformats(allDateformats);
                    data.setStgTableName(stgTableName);
                    data.setSkColumn(skColumn);
                    data.setAllPkSk(allPkSk);
                    data.setCleansingValues(cleansingValues);
                    // List
                    data.appendToPkDateformats(pkDateformats);
                    data.appendToCleansingValidations(cleansingValidations);

                    // aggregatedSourceValues
                    aggregatedSourceValues.put(key, data);

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return aggregatedSourceValues;
        }

        /**
         * Retrieves data from ELT_IL_Source_Mapping_Info_Saved based on the provided
         * parameters.
         *
         * @param conn                 The database connection object.
         * @param dimensionTransaction The Dimension_Transaction value (e.g., 'D').
         * @param querySchemaCond      The query schema condition (e.g., "AND
         *                             TABLE_SCHEMA = 'your_schema'").
         * @param connectionId         The Connection_Id value.
         * @param ilTableName          The IL_Table_Name value.
         * @return A ResultSet containing the query results, or null if an error occurs.
         */
        private Map<String, DimSourceValues> getTransactionSourceMappingInfo(Connection conn, String dimensionTransaction, String querySchemaCond,
                String connectionId, String ilTableName) {
            
            Map<String, DimSourceValues> aggregatedSourceValues = new HashMap<>();

            String dateFormatValue = getSettingValue(conn, connectionId, schemaName, "Dateformat");
            if (dateFormatValue == null) {
                dateFormatValue = "yyyy-MM-dd";
            }
            ResultSet rs = null;
            // The Query is different for Dimension counterpart
            String query = getTransactionSourceMappingInfoForValueScriptQuery() + querySchemaCond;

            try {
                PreparedStatement pstmt = conn.prepareStatement(query);
                pstmt.setString(1, ilTableName);
                pstmt.setString(2, connectionId);

                rs = pstmt.executeQuery();
                System.out.println("datatypeConversionsGlobal: " + datatypeConversionsGlobal.size());
                // Map<String, Map<String, String>> lookupDataMap = getSourceMappingInfoAsMap(
                //         conn, dimensionTransaction, querySchemaCond, connectionId, ilTableName);


                String cdcFlag = "";
                while (rs.next()) {
                    String connectionIdValue = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("TABLE_SCHEMA");
                    String ilTableNameValue = rs.getString("IL_Table_Name");
                    String ilColumnName = rs.getString("IL_Column_Name");
                    String ilDataType = rs.getString("IL_Data_Type");
                    String constraints = rs.getString("Constraints");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String sourceColumnName = rs.getString("Source_Column_Name");
                    String sourceDataType = rs.getString("Source_Data_Type");
                    String pkConstraint = rs.getString("PK_Constraint");
                    String pkColumnName = rs.getString("PK_Column_Name");
                    String fkConstraint = rs.getString("FK_Constraint");
                    String fkColumnName = rs.getString("FK_Column_Name");
                    String dimensionTransactionValue = rs.getString("Dimension_Transaction");
                    String dimensionKey = rs.getString("Dimension_Key");
                    String dimensionName = rs.getString("Dimension_Name");
                    String dimensionJoinCondition = rs.getString("Dimension_Join_Condition");
                    boolean activeFlag = rs.getBoolean("Active_Flag");
                    // String cdcFlag = rs.getString("CDC_Flag");
                    String dataType = rs.getString("Datatype");

                    String key = connectionIdValue + "-" + tableSchema + "-" + ilTableNameValue;

                    // the left join with datatypeConversions
                    Map<String, String> conversionDetails = datatypeConversionsGlobal.get(dataType);
                    String javaDataTypeConversion = null;
                    if (conversionDetails != null) {
                        javaDataTypeConversion = (String) conversionDetails.get("javaDataType");
                    }
                    
                    String javaDataType = javaDataTypeConversion;

                    // Transformation
                    String dateformat = dateFormatValue;
                    String allCols = null;
                    String allExceptPk = null;
                    String allPk = null;
                    String allJavaDataTypes = null;
                    String allDateFormats = null;
                    String stgTableName = null;
                    String skColumn = null;
                    String allPkSk = null;

                    allCols = allCols == null ? ilColumnName : (allCols + "," + ilColumnName);

                    allExceptPk = allExceptPk == null ? (constraints.equals("PK") ? null : ilColumnName)
                            : (constraints.equals("PK") ? allExceptPk : (allExceptPk + "," + ilColumnName));

                    allPk = allPk == null ? (constraints.equals("PK") ? ilColumnName : null)
                            : (constraints.equals("PK") ? (allPk + "," + ilColumnName) : allPk);

                    allJavaDataTypes = allJavaDataTypes == null ? javaDataType
                            : (allJavaDataTypes + "," + javaDataType);

                    allDateFormats = allDateFormats == null
                            ? (ilDataType.toLowerCase().contains("date") ? dateformat : "")
                            : (allDateFormats + "," + (ilDataType.toLowerCase().contains("date") ? dateformat : ""));

                    stgTableName = ilTableNameValue + "_Stg";
                    skColumn = sourceTableName + "_Key";
                    allPkSk = allPk + "," + skColumn;

                    // Aggregation
                    DimSourceValues data = aggregatedSourceValues.computeIfAbsent(key,
                            k -> new DimSourceValues(connectionIdValue, tableSchema, ilTableNameValue));
                    // Last
                    data.setAllCols(allCols);
                    data.setAllExceptPk(allExceptPk);
                    data.setAllPk(allPk);
                    data.setAllJavaDataTypes(allJavaDataTypes);
                    data.setAllDateformats(allDateFormats);
                    data.setStgTableName(stgTableName);
                    data.setSkColumn(skColumn);
                    data.setAllPkSk(allPkSk);

                    // aggregatedSourceValues
                    aggregatedSourceValues.put(key, data);

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return aggregatedSourceValues;
        }

        /**
         * Returns the query with placeholders for Transaction type.
         *
         * @return The concise query as a String.
         */
        private String getTransactionSourceMappingInfoForValueScriptQuery() {
            return "SELECT " +
                    "Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, " +
                    "IL_Data_Type, Constraints, Source_Table_Name, Source_Column_Name, " +
                    "LOWER(Source_Data_Type) AS Source_Data_Type, PK_Constraint, " +
                    "PK_Column_Name, FK_Constraint, FK_Column_Name, Dimension_Transaction, " +
                    "Dimension_Key, Dimension_Name, Dimension_Join_Condition, Active_Flag, " +
                    "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS Datatype " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = 'T' " +
                    "AND Constraints NOT IN ('SK', 'FK') " +
                    "AND (Constant_Insert_Column <> 'Y' OR Constant_Insert_Column IS NULL) " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? ";
        }

        /**
         * Retrieves data from ELT_IL_Source_Mapping_Info_Saved and returns it as a map
         * of maps.
         * The key is a combination of Connection_Id, TABLE_SCHEMA, IL_Table_Name, and
         * IL_Column_Name.
         * The value is a map containing the IL_Data_Type field.
         *
         * @param conn                 The database connection object.
         * @param dimensionTransaction The Dimension_Transaction value (e.g., 'D').
         * @param querySchemaCond      The query schema condition (e.g., "AND
         *                             TABLE_SCHEMA = 'your_schema'").
         * @param connectionId         The Connection_Id value.
         * @param ilTableName          The IL_Table_Name value.
         * @return A map of maps containing the query results, or an empty map if no
         *         records are found.
         */
        private Map<String, Map<String, String>> getSourceMappingInfoAsMap(
                Connection conn, String dimensionTransaction, String querySchemaCond, String connectionId,
                String ilTableName) {
            Map<String, Map<String, String>> resultMap = new HashMap<>();

            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, IL_Data_Type " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = ? " +
                    "AND Constraints = 'PK' " +
                    "AND (Constant_Insert_Column <> 'Y' OR Constant_Insert_Column IS NULL) " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, dimensionTransaction);
                pstmt.setString(2, ilTableName);
                pstmt.setString(3, connectionId);

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        // the key
                        String key = rs.getString("Connection_Id") + "-" +
                                rs.getString("TABLE_SCHEMA") + "-" +
                                rs.getString("IL_Table_Name") + "-" +
                                rs.getString("IL_Column_Name");

                        Map<String, String> rowData = new HashMap<>();
                        rowData.put("IL_Data_Type", rs.getString("IL_Data_Type"));

                        resultMap.put(key, rowData);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return resultMap;
        }

        /**
         * Executes the query and returns the results as a list of maps.
         *
         * @param conn                 The database connection object.
         * @param dimensionTransaction The Dimension_Transaction value (e.g., 'D').
         * @param ilTableName          The IL_Table_Name value.
         * @param connectionId         The Connection_Id value.
         * @param querySchemaCond      The query schema condition (e.g., "AND
         *                             TABLE_SCHEMA = 'your_schema'").
         * @return A list of maps, where each map represents a row in the result set.
         */
        private Map<String, Map<String, String>> getConstantInsertDataMap(
                Connection conn, String dimensionTransaction, String ilTableName, String connectionId,
                String querySchemaCond) {
            List<Map<String, String>> resultList = new ArrayList<>();

            String query = "SELECT Connection_Id, TABLE_SCHEMA, IL_Table_Name, IL_Column_Name, " +
                    "LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) as IL_Data_Type, Constraints, " +
                    "Source_Table_Name, Source_Column_Name, LOWER(Source_Data_Type) as Source_Data_Type, " +
                    "PK_Constraint, PK_Column_Name, FK_Constraint, FK_Column_Name, Dimension_Transaction, " +
                    "Dimension_Key, Dimension_Name, Dimension_Join_Condition, Active_Flag, " +
                    "Constant_Insert_Column, Constant_Insert_Value " +
                    "FROM ELT_IL_Source_Mapping_Info_Saved " +
                    "WHERE Dimension_Transaction = ? " +
                    "AND Constant_Insert_Column = 'Y' " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;
            Map<String, Map<String, String>> aggregatedMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setString(1, dimensionTransaction);
                pstmt.setString(2, ilTableName);
                pstmt.setString(3, connectionId);

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {

                        String ilTableNameValue = rs.getString("IL_Table_Name");
                        String ilDataType = rs.getString("IL_Data_Type"); // key for datatype Conversion
                        String constantInsertValue = rs.getString("Constant_Insert_Value");
                        String ilColumnName = rs.getString("IL_Column_Name");

                        // the left join with datatypeConversions
                        Map<String, String> conversionDetails = datatypeConversionsGlobal.get(ilDataType);
                        String javaDataTypeConversion = conversionDetails.getOrDefault("javaDataType", new String());

                        // Output
                        String constantFields = ilColumnName;
                        String constantTypes = javaDataTypeConversion;
                        String constantValues = constantInsertValue;

                        // Get or initialize the nested map for the current table name
                        Map<String, String> tableAggregations = aggregatedMap.getOrDefault(ilTableNameValue,
                                new HashMap<>());

                        // Initialize or retrieve StringBuilder objects
                        String constantFieldsBuilder = (String) tableAggregations.getOrDefault("ConstantFields",
                                new String());
                        String constantValuesBuilder = (String) tableAggregations.getOrDefault("ConstantValues",
                                new String());
                        String constantTypesBuilder = (String) tableAggregations.getOrDefault("ConstantTypes",
                                new String());

                        // Aggregate fields, values, and types with a separator
                        if (constantFieldsBuilder.length() > 0) {
                            constantFieldsBuilder = constantFieldsBuilder + ", ";
                            constantValuesBuilder = constantValuesBuilder + ", ";
                            constantTypesBuilder = constantTypesBuilder + ", ";
                        }
                        constantFieldsBuilder = constantFieldsBuilder + constantFields;
                        constantValuesBuilder = constantValuesBuilder + constantValues;
                        constantTypesBuilder = constantTypesBuilder + constantTypes;
                        // Update the aggregated values
                        tableAggregations.put("ConstantFields", constantFieldsBuilder);
                        tableAggregations.put("ConstantValues", constantValuesBuilder);
                        tableAggregations.put("ConstantTypes", constantTypesBuilder);
                        // Update the outer map with the nested map
                        aggregatedMap.put(ilTableNameValue, tableAggregations);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println("aggregatedMap: \n" + aggregatedMap);
            return aggregatedMap;
        }

        /**
         * Retrieves and aggregates the Value_Name field from ELT_Job_Properties_Info
         *
         * @param conn      The database connection object.
         * @param jobType   The Job_Type value.
         * @param writeMode The Write_Mode_Type value.
         * @return A string containing all Value_Name values separated by newline
         *         characters, or null if no records are found.
         */
        private String getAggregatedValueNames(Connection conn, String jobType, String writeMode) {
            StringBuilder aggregatedValues = new StringBuilder();
            String query = "SELECT Value_Name " +
                    "FROM ELT_Job_Properties_Info " +
                    "WHERE Job_Type = ? " +
                    "AND Active_Flag = 1 " +
                    "AND Dynamic_Flag = 1 " +
                    "AND Write_Mode_Type = ?";

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, jobType);
                pstmt.setString(2, writeMode);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        if (aggregatedValues.length() > 0) {
                            aggregatedValues.append("\n");
                        }
                        aggregatedValues.append(rs.getString("Value_Name"));
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return aggregatedValues.length() > 0 ? aggregatedValues.toString() : null;
        }

        private void deleteValuesProperties(Connection connection, String ilTableName, String connectionId)
                throws SQLException {
            String query = "DELETE FROM ELT_VALUES_PROPERTIES WHERE IL_Table_Name = ? AND Connection_Id = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);

                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println("ELT_VALUES_PROPERTIES rows deleted for '" + ilTableName + "' : " + rowsAffected);
            } catch (SQLException e) {
                throw new SQLException("Error while deleting values properties", e);
            }
        }

        private String getDeletesValueFileName(String ilTableName) {
            String suffix = getTimeStamp();
            String valueFileName = filePath + ilTableName + DELETES_VALUE_FILE_STRING + suffix + ".values.properties"; // filePath is the directory name
            return valueFileName;
        }
        private String getValueFileName(String ilTableName, String valueFileString) {
            String suffix = getTimeStamp();
            String valueFileName = filePath + ilTableName + valueFileString + clientId + suffix + ".values.properties";
            return valueFileName;
        }

        /**
         * Retrieves the distinct value of IsWebService from
         * ELT_Selective_Source_Metadata based on the provided parameters.
         *
         * @param ilTableName  The IL_Table_Name value.
         * @param connectionId The Connection_Id value.
         * @param schemaName   The Schema_Name value.
         * @return The distinct value of IsWebService, or null if no matching record is
         *         found.
         */
        private long getIsWebService(Connection connection, String ilTableName, String connectionId,
                String schemaName) {
            long isWebService = -1;

            String query = "SELECT DISTINCT ssm.IsWebService " +
                    "FROM ELT_Selective_Source_Metadata ssm " +
                    "INNER JOIN ELT_IL_Source_Mapping_Info_Saved smis " +
                    "ON smis.Source_Table_Name = ssm.Table_Name " +
                    "WHERE smis.IL_Table_Name = ? " +
                    "AND smis.Connection_Id = ? " +
                    "AND smis.Table_Schema = ?";

            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.setString(1, ilTableName);
                pstmt.setString(2, connectionId);
                pstmt.setString(3, schemaName);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        isWebService = rs.getLong("IsWebService");
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return isWebService;
        }
        
        /**
         * Retrieves the time_zone from minidwcs_database_connections
         * or minidwcs_ws_connections_mst based on query
         *
         * @param connectionId The connection ID to query.
         * @return The time_zone value, or null if no matching record is found.
         */
        private TimeZone getTimeZone(Connection conn, String connectionId, String query) {
            TimeZone timeZone = null;
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {

                pstmt.setString(1, connectionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        String sourceTimeZone = rs.getString("time_zone");
                        String db = "\"\"UTC\"\"";
                        String client = "\"\"" + timeZone + "\"\"";
                        String insert = "\"CONVERT_TZ(sysdate()," + db + "," + client + ")\",'ELT_Admin'," +
                                        "\"CONVERT_TZ(sysdate()," + db + "," + client + ")\",'ELT_Admin'";
                        String update = "\"CONVERT_TZ(sysdate()," + db + "," + client + ")\",'ELT_Admin'";
                        String targetTimeZone = insert;
                        timeZone = new TimeZone(sourceTimeZone, targetTimeZone, insert, update);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return timeZone;
        }
        public class TimeZone {
            private String srcTimeZone;
            private String tgtTimeZone;
            private String Insert;
            private String Update;
        
            public TimeZone() {
            }
        
            public TimeZone(String srcTimeZone, String tgtTimeZone, String Insert, String Update) {
                this.srcTimeZone = srcTimeZone;
                this.tgtTimeZone = tgtTimeZone;
                this.Insert = Insert;
                this.Update = Update;
            }
        
            public String getSrcTimeZone() {
                return srcTimeZone;
            }
        
            public void setSrcTimeZone(String srcTimeZone) {
                this.srcTimeZone = srcTimeZone;
            }
        
            public String getTgtTimeZone() {
                return tgtTimeZone;
            }
        
            public void setTgtTimeZone(String tgtTimeZone) {
                this.tgtTimeZone = tgtTimeZone;
            }
        
            public String getInsert() {
                return Insert;
            }
        
            public void setInsert(String Insert) {
                this.Insert = Insert;
            }
        
            public String getUpdate() {
                return Update;
            }
        
            public void setUpdate(String Update) {
                this.Update = Update;
            }
        
            @Override
            public String toString() {
                return "TimeZone{" +
                        "SRC_Time_Zone='" + srcTimeZone + '\'' +
                        ", TGT_Time_Zone='" + tgtTimeZone + '\'' +
                        ", Insert='" + Insert + '\'' +
                        ", Update='" + Update + '\'' +
                        '}';
            }
        }
    }
    
    public class DWTableInfoScriptsGenerator {

        // Constructor
        public DWTableInfoScriptsGenerator() {
            // Initialization logic if needed
        }
        // Generating the table info script
        public Status generateTableInfoScript() {
            try {              
                updateIlTableInfo(conn);
                Map<String, AggregatedResult> savedDataMap = executeSourceMappingInfoQuery(conn,  SQLQueries.queryILSourceMappingInfoSaved);
                Map<String, AggregatedResult> infoDataMap = executeSourceMappingInfoQuery(conn,  SQLQueries.queryILSourceMappingInfo);
                // Mapping_info
                Map<String, Map<String, Object>> selectiveMetadataMap = executeSelectiveSourceMetadataQuery(conn);
                // Metadata Info
                Map<String, Map<String, Object>> customMetadataMap = executeCustomSourceMetadataQuery(conn);
                // Mapping Info saved
                Map<String, Map<String, Object>> ilMappingInfoSavedMap = executeILSourceMappingInfoQuery(conn);
               
                Map<String, String> databaseConnectionsData = executeDatabaseConnectionQuery(conn);

                Map<String, String> wsConnectionsData = executeWSConnectionsQuery(conn);

                Map<String, String> remoteConnectionsData = executeWSConnectionsQuery(conn);

                Map<String, Map<String, String>> eltILTableInfoData = executeILTableInfoQuery(conn);

                // Iterating the main component data
                List<Map<String, Object>> innerJoinResultList = new ArrayList<>(); // Inner Join Result List, to be inserted
                List<Map<String, Object>> antiJoinResultList = new ArrayList<>(); // Anti Join Result List, to be updated

                for (Map.Entry<String, AggregatedResult> entry : savedDataMap.entrySet()) {
                    String key = entry.getKey();
                    AggregatedResult saved = entry.getValue();
                
                    AggregatedResult infoData = infoDataMap.getOrDefault(key, new AggregatedResult());
                    Map<String, Object> selectiveMetadata = selectiveMetadataMap.getOrDefault(key, new HashMap<>());
                    Map<String, Object> customMetadata = customMetadataMap.getOrDefault(key, new HashMap<>());
                    Map<String, Object> ilMappingInfoSaved = ilMappingInfoSavedMap.getOrDefault(key, new HashMap<>());


                    // // Status logic
                    //         String status = (compareDates((String) saved.getUpdatedDate(), (String) infoData.getUpdatedDate()) > 0) 
                    //         ? "Saved" 
                    //         : "Created";

                    String status;
                    // if (compareDates((String) saved.getUpdatedDate(), (String) infoData.getUpdatedDate()) > 0) {
                    Timestamp savedUpdatedDate = saved.getMaxUpdatedDate();
                    Timestamp infoUpdatedDate = infoData.getMaxUpdatedDate();
                    if (infoUpdatedDate == null || savedUpdatedDate.compareTo(infoUpdatedDate) > 0) {
                        status = "Saved";
                    } else {
                        status = "Created";
                    }

                    String source;
                    if (selectiveMetadata.get("Custom_Type") == null) {
                        if (!Boolean.TRUE.equals(saved.getIsFileUpload())) {
                            source = "dbSource";
                        } else {
                            source = "importFile";
                        }
                    } else {
                        if ("Common".equals(selectiveMetadata.get("Custom_Type")) && !Boolean.TRUE.equals(saved.getIsFileUpload())) {
                            source = "dbSource";
                        } else if ("Common".equals(selectiveMetadata.get("Custom_Type")) && Boolean.TRUE.equals(saved.getIsFileUpload())) {
                            source = "importFile";
                        } else {
                            source = (String) selectiveMetadata.get("Custom_Type");
                        }
                    }
                        
                    // // Source logic
                    // String source = (selectiveMetadata.get("Custom_Type") == null)
                    // ? (!Boolean.TRUE.equals(saved.getIsFileUpload()) ? "dbSource" : "importFile")
                    // : ("Common".equals(selectiveMetadata.get("Custom_Type")) && !Boolean.TRUE.equals(saved.getIsFileUpload()) ? "dbSource"
                    // : "Common".equals(selectiveMetadata.get("Custom_Type")) && Boolean.TRUE.equals(saved.getIsFileUpload()) ? "importFile"
                    // : (String) selectiveMetadata.get("Custom_Type"));

                    // // Custom Type logic
                    // String customType = (customMetadata.get("Custom_Type") == null)
                    // ? (selectiveMetadata.get("Custom_Type") == null ? ""
                    // : ("common".equalsIgnoreCase((String) selectiveMetadata.get("Custom_Type"))
                    // && ((Integer) selectiveMetadata.get("File_Id") != 0) ? "metadata_file"
                    // : "common".equalsIgnoreCase((String) selectiveMetadata.get("Custom_Type"))
                    // && ((Integer) selectiveMetadata.get("File_Id") == 0) ? "dbSource"
                    // : (String) selectiveMetadata.get("Custom_Type")))
                    // : (String) customMetadata.get("Custom_Type");

                    String customCustomType = (String) customMetadata.get("Custom_Type");
                    String selectiveCustomType = (String) selectiveMetadata.get("Custom_Type");
                    Integer fileId = (Integer) selectiveMetadata.get("File_Id");
                    Boolean isWebService = Boolean.TRUE.equals(selectiveMetadata.get("IsWebService"));
                    String connectionTypeFromCustomMetadata = (String) customMetadata.get("connection_type");

                    String customType;
                    if (customCustomType == null) {
                        if (selectiveCustomType == null) {
                            customType = "";
                        } else {
                            // String selectiveCustomType = (String) selectiveMetadata.get("Custom_Type");
                            // Integer fileId = (Integer) selectiveMetadata.get("File_Id");
                            
                            if ("common".equalsIgnoreCase(selectiveCustomType) && fileId != 0) {
                                customType = "metadata_file";
                            } else if ("common".equalsIgnoreCase(selectiveCustomType) && fileId == 0) {
                                customType = "dbSource";
                            } else {
                                customType = selectiveCustomType;
                            }
                        }
                    } else {
                        customType = customCustomType;
                    }
                    

                    String sourceType;

                    // Extract common variables for readability
                    // String customCustomType = (String) customMetadata.get("Custom_Type");
                    // String selectiveCustomType = (String) selectiveMetadata.get("Custom_Type");
                    // Integer fileId = (Integer) selectiveMetadata.get("File_Id");


                    if (customCustomType == null) {
                        if (selectiveCustomType == null) {
                            sourceType = "";
                        } else if ("common".equalsIgnoreCase(selectiveCustomType)) {
                            if (fileId != 0 && isWebService) {
                                sourceType = "web_service";
                            } else if (fileId == 0 && !isWebService) {
                                sourceType = "dbSource";
                            } else if (fileId != 0 && !isWebService) {
                                sourceType = "dbSource";
                            } else {
                                sourceType = selectiveCustomType;
                            }
                        } else if ("metadata_file".equalsIgnoreCase(selectiveCustomType)) {
                            if (fileId != 0 && isWebService) {
                                sourceType = "web_service";
                            } else if (fileId == 0 && !isWebService) {
                                sourceType = "dbSource";
                            } else if ("metadata_file".equalsIgnoreCase(selectiveCustomType)) {
                                sourceType = "shared_folder";
                            } else {
                                sourceType = selectiveCustomType;
                            }
                        } else if ("dbSource".equalsIgnoreCase(selectiveCustomType)) {
                            sourceType = "dbSource";
                        } else if ("web_service".equalsIgnoreCase(selectiveCustomType)) {
                            sourceType = "web_service";
                        } else {
                            sourceType = selectiveCustomType;
                        }
                    } else {
                        sourceType = connectionTypeFromCustomMetadata;
                    }

                    // Output (out1)
                    String connectionId = saved.getConnectionId();
                    String connectionName; // Default i.e. null
                    String tableSchema = saved.getTableSchema();
                    String ilTableName = saved.getIlTableName();
                    String sourceTableName = (String) ilMappingInfoSaved.get("Source_Table_Name");
                    String dimensionTransaction;
                    if (infoData.getDimensionTransaction() != null) {
                        dimensionTransaction = infoData.getDimensionTransaction();
                    } else {
                        dimensionTransaction = saved.getDimensionTransaction();
                    }
                    String savedfileId = saved.getFileId();
                    Boolean isFileUpload = saved.getIsFileUpload();
                    String Source = sourceType;
                    String addedUser = "0"; // Hardcoded
                    Timestamp addedDate = Timestamp.valueOf(startTime);
                    String updatedUser = userName;
                    Timestamp updatedDate = Timestamp.valueOf(startTime);

                    // tMap_3, out
                    String dbConnectionName = databaseConnectionsData.getOrDefault(connectionId, new String());
                    String wsConnectionName = wsConnectionsData.getOrDefault(key, new String());
                    String remoteConnectionName = remoteConnectionsData.getOrDefault(connectionId, new String());

                    String sourceLC = (source == null) ? "" : source.toLowerCase();
                    if (sourceLC.equals("db") || sourceLC.equals("dbsource")) {
                        connectionName = dbConnectionName;
                    } else if (sourceLC.equals("web_service") || sourceLC.equals("onedrive") || sourceLC.equals("sageintacct")) {
                        connectionName = wsConnectionName;
                    } else {
                        connectionName = remoteConnectionName;
                    }

                    // tMap_2
                    String keyTableInfo = connectionId + "-" + tableSchema + "-" + ilTableName + "-" + sourceTableName; // TODO: verify key
                    if (eltILTableInfoData.containsKey(keyTableInfo)) { // Inner Join
                        Map<String, String> eltILTableInfo = eltILTableInfoData.get(keyTableInfo); // Semi Join, Not Used
                        Map<String, Object> resultMap = new HashMap<>();
                        resultMap.put("Connection_Id", connectionId);
                        resultMap.put("Connection_Name", connectionName);
                        resultMap.put("Table_Schema", tableSchema);
                        resultMap.put("IL_Table_Name", ilTableName);
                        resultMap.put("Source_Table_Name", sourceTableName);
                        resultMap.put("Dimension_Transaction", dimensionTransaction);
                        resultMap.put("File_Id", savedfileId);
                        resultMap.put("IsFileUpload", isFileUpload);
                        resultMap.put("IsWebService", isWebService);
                        resultMap.put("Saved_Updated_Date", savedUpdatedDate);
                        resultMap.put("Info_Updated_Date", infoUpdatedDate);
                        resultMap.put("Status", status);
                        resultMap.put("Source", Source);
                        resultMap.put("Custom_Type", customType);
                        resultMap.put("Added_Date", addedDate);
                        resultMap.put("Added_User", addedUser);
                        resultMap.put("Updated_Date", updatedDate);
                        resultMap.put("Updated_User", updatedUser);

                        innerJoinResultList.add(resultMap);
                    } else { // Anti Join (!eltILTableInfoData.containsKey(keyTableInfo))
                        Map<String, String> eltILTableInfo = new HashMap<>();  // Semi Join, Not Used
                        Map<String, Object> resultMap = new HashMap<>();
                        resultMap.put("Connection_Id", connectionId);
                        resultMap.put("Connection_Name", connectionName);
                        resultMap.put("Table_Schema", tableSchema);
                        resultMap.put("IL_Table_Name", ilTableName);
                        resultMap.put("Source_Table_Name", sourceTableName);
                        resultMap.put("Dimension_Transaction", dimensionTransaction);
                        resultMap.put("File_Id", savedfileId);
                        resultMap.put("IsFileUpload", isFileUpload);
                        resultMap.put("IsWebService", isWebService);
                        resultMap.put("Saved_Updated_Date", savedUpdatedDate);
                        resultMap.put("Info_Updated_Date", infoUpdatedDate);
                        resultMap.put("Status", status);
                        resultMap.put("Source", Source);
                        resultMap.put("Custom_Type", customType);
                        resultMap.put("Added_Date", addedDate);
                        resultMap.put("Added_User", addedUser);
                        resultMap.put("Updated_Date", updatedDate);
                        resultMap.put("Updated_User", updatedUser);

                        antiJoinResultList.add(resultMap);
                    }
                }
                
                saveAntiInnerJoinedData(conn, antiJoinResultList);
                updateInnerJoinedData(conn, innerJoinResultList);
                // saveInnerJoinedData(conn, innerJoinResultList);
                // updateAntiInnerJoinedData(conn, antiJoinResultList);

                deleteFromIlTableInfo(conn);
            } catch (SQLException e) {
                e.printStackTrace();
                return Status.FAILURE;
            }
            return Status.SUCCESS;
        }
// 
        public Map<String, Map<String, Object>> executeSelectiveSourceMetadataQuery(Connection connection) {
            String query = "SELECT DISTINCT Connection_Id, Schema_Name, Table_Name, IsWebService, Custom_Type, File_Id "
                         + "FROM ELT_Selective_Source_Metadata";
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String schemaName = rs.getString("Schema_Name");
                    String tableName = rs.getString("Table_Name");
                    boolean isWebService = rs.getBoolean("IsWebService");
                    String customType = rs.getString("Custom_Type");
                    String fileId = rs.getString("File_Id");

                    // key
                    String key = connectionId + "_" + schemaName + "_" + tableName;

                    Map<String, Object> rowData = new HashMap<>();
                    rowData.put("IsWebService", isWebService);
                    rowData.put("Custom_Type", customType);
                    rowData.put("File_Id", fileId);

                    resultMap.put(key, rowData);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        public Map<String, Map<String, Object>> executeCustomSourceMetadataQuery(Connection connection) {
            String query = "SELECT ELT_Custom_Source_Metadata_Info.Connection_Id, ELT_Custom_Source_Metadata_Info.Schema_Name, "
                         + "ELT_Custom_Source_Metadata_Info.Table_Name, ELT_Custom_Source_Metadata_Info.Source_Table_Name, "
                         + "ELT_Custom_Source_Metadata_Info.Custom_Type, ELT_Custom_Source_Metadata_Info.connection_type "
                         + "FROM ELT_Custom_Source_Metadata_Info";
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String schemaName = rs.getString("Schema_Name");
                    String tableName = rs.getString("Table_Name");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String customType = rs.getString("Custom_Type");
                    String connectionType = rs.getString("connection_type");
                    // key
                    String key = connectionId + "-" + schemaName + "-" + tableName;

                    Map<String, Object> rowData = new HashMap<>();
                    rowData.put("Source_Table_Name", sourceTableName);
                    rowData.put("Custom_Type", customType);
                    rowData.put("Connection_Type", connectionType);

                    resultMap.put(key, rowData);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        public Map<String, Map<String, Object>> executeILSourceMappingInfoQuery(Connection connection) {
            String query = "SELECT DISTINCT ELT_IL_Source_Mapping_Info_Saved.Connection_Id, ELT_IL_Source_Mapping_Info_Saved.Table_Schema, "
                         + "ELT_IL_Source_Mapping_Info_Saved.IL_Table_Name, ELT_IL_Source_Mapping_Info_Saved.Source_Table_Name "
                         + "FROM ELT_IL_Source_Mapping_Info_Saved WHERE Source_Table_Name IS NOT NULL AND Source_Table_Name != ''";
    
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("Table_Schema");
                    String ilTableName = rs.getString("IL_Table_Name");
                    String sourceTableName = rs.getString("Source_Table_Name");
                    // key
                    String key = connectionId + "-" + tableSchema + "-" + ilTableName;
    
                    Map<String, Object> rowData = new HashMap<>();
                    rowData.put("Source_Table_Name", sourceTableName);
    
                    resultMap.put(key, rowData);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        public Map<String, String> executeDatabaseConnectionQuery(Connection connection) {
            String query = "SELECT minidwcs_database_connections.Connection_Id, minidwcs_database_connections.connection_name "
                         + "FROM minidwcs_database_connections";
    
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String connectionName = rs.getString("connection_name");
    
                    // key is Connection_Id
                    resultMap.put(connectionId, connectionName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        public Map<String, String> executeWSConnectionsQuery(Connection connection) {
            String query = "SELECT minidwcs_ws_connections_mst.id, minidwcs_ws_connections_mst.web_service_con_name "
                         + "FROM minidwcs_ws_connections_mst";
    
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String id = rs.getString("id");
                    String webServiceConName = rs.getString("web_service_con_name");
                    // key is id
                    resultMap.put(id, webServiceConName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        public Map<String, String> executeRemoteConnectionsQuery(Connection connection) {
            String query = "SELECT minidwcs_remote_connections.id, minidwcs_remote_connections.connection_name "
                         + "FROM minidwcs_remote_connections";
    
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String id = rs.getString("id");
                    String connectionName = rs.getString("connection_name");
                    // key is id
                    resultMap.put(id, connectionName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        // row3 ELT_IL_Table_Info
        public Map<String, Map<String, String>> executeILTableInfoQuery(Connection connection) {
            String query = "SELECT ELT_IL_Table_Info.Connection_Id, ELT_IL_Table_Info.Table_Schema, "
                         + "ELT_IL_Table_Info.IL_Table_Name, ELT_IL_Table_Info.Source_Table_Name "
                         + "FROM ELT_IL_Table_Info";
    
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("Table_Schema");
                    String ilTableName = rs.getString("IL_Table_Name");
                    String sourceTableName = rs.getString("Source_Table_Name");
    
                    // key 
                    String key = connectionId + "-" + tableSchema + "-" + ilTableName + "-" + sourceTableName;
    
                    Map<String, String> rowData = new HashMap<>();
                    rowData.put("Connection_Id", connectionId);
                    rowData.put("Table_Schema", tableSchema);
                    rowData.put("IL_Table_Name", ilTableName);
                    rowData.put("Source_Table_Name", sourceTableName);
    
                    resultMap.put(key, rowData);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }

        private int updateIlTableInfo(Connection connection) {
            String query = "UPDATE ELT_IL_Table_Info SET Added_User = 1";
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                int rowsUpdated = preparedStatement.executeUpdate();
                System.out.println("Rows updated in table ELT_IL_Table_Info: " + rowsUpdated);
                return rowsUpdated;
            } catch (SQLException e) {
                System.err.println("Error executing update query: \n" + e.getMessage());
                e.printStackTrace();
            }
            return -1;
        }

        public int deleteFromIlTableInfo(Connection connection) throws SQLException {
            String sql = "DELETE FROM ELT_IL_Table_Info WHERE Added_User = 1";
        
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {                
                int rowsDeleted = preparedStatement.executeUpdate();
                System.out.println("Rows deleted from table ELT_IL_Table_Info: " + rowsDeleted);
                return rowsDeleted;
            } catch (SQLException e) {
                System.err.println("Error executing delete query: \n" + e.getMessage());
                e.printStackTrace();
            }
            return -1;
        }
        
        // Save Anti Inner join data into the table
        private void saveAntiInnerJoinedData(Connection dbConnection, List<Map<String, Object>> rowsData) throws SQLException {
            String dbTable ="ELT_IL_Table_Info";
            if (rowsData != null && !rowsData.isEmpty()) {
                int rowsAdded = saveDataIntoDB(dbConnection, dbTable, rowsData);
                System.out.println("Anti InnerJoinedData - Data loaded into the target " + dbTable);
            }
            else {
                System.out.println("Anti InnerJoinedData - No data for the target " + dbTable);
            }
        }
    
        // Update Inner join data into the table
        private void updateInnerJoinedData(Connection dbConnection, List<Map<String, Object>> rowsData) throws SQLException {
            String dbTable ="ELT_IL_Table_Info";
            if (rowsData != null && !rowsData.isEmpty()) {
                int rowsAdded = updateDataIntoDB(dbConnection, dbTable, rowsData);
                System.out.println("InnerJoinedData - Data loaded into the target " + dbTable);
            }
            else {
                System.out.println("InnerJoinedData - No data for the target " + dbTable);
            }
        }

        public int updateDataIntoDB(Connection connection, String tableName,  List<Map<String, Object>> data) throws SQLException {
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("Update data cannot be null or empty");
            }
            int rowsAffected;

            String sql = "UPDATE " + tableName + " SET " +
                    "Connection_Name = ?, " +
                    "Dimension_Transaction = ?, " +
                    "File_Id = ?, " +
                    "IsFileUpload = ?, " +
                    "IsWebService = ?, " +
                    "Saved_Updated_Date = ?, " +
                    "Info_Updated_Date = ?, " +
                    "Status = ?, " +
                    "Source = ?, " +
                    "Custom_Type = ?, " +
                    "Added_Date = ?, " +
                    "Added_User = ?, " +
                    "Updated_Date = ?, " +
                    "Updated_User = ? " +
                    "WHERE Connection_Id = ? AND Table_Schema = ? AND IL_Table_Name = ? AND Source_Table_Name = ?";
        
            boolean originalAutoCommit = connection.getAutoCommit();
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                connection.setAutoCommit(false); // Enable transaction for batch update

                for (Map<String, Object> updateData : data) {
                    preparedStatement.setString(1, (String) updateData.get("Connection_Name"));
                    preparedStatement.setString(2, (String) updateData.get("Dimension_Transaction"));
                    preparedStatement.setString(3, (String) updateData.get("File_Id"));
                    preparedStatement.setBoolean(4, (Boolean) updateData.get("IsFileUpload"));
                    preparedStatement.setBoolean(5, (Boolean) updateData.get("IsWebService"));
                    preparedStatement.setTimestamp(6, (java.sql.Timestamp) updateData.get("Saved_Updated_Date"));
                    preparedStatement.setTimestamp(7, (java.sql.Timestamp) updateData.get("Info_Updated_Date"));
                    preparedStatement.setString(8, (String) updateData.get("Status"));
                    preparedStatement.setString(9, (String) updateData.get("Source"));
                    preparedStatement.setString(10, (String) updateData.get("Custom_Type"));

                    preparedStatement.setTimestamp(11, (Timestamp) updateData.get("Added_Date"));
                    preparedStatement.setString(12, (String) updateData.get("Added_User"));
                    preparedStatement.setTimestamp(13, (Timestamp) updateData.get("Updated_Date"));
                    preparedStatement.setString(14, (String) updateData.get("Updated_User"));

                    preparedStatement.setString(15, (String) updateData.get("Connection_Id"));
                    preparedStatement.setString(16, (String) updateData.get("Table_Schema"));
                    preparedStatement.setString(17, (String) updateData.get("IL_Table_Name"));
                    preparedStatement.setString(18, (String) updateData.get("Source_Table_Name"));
                    preparedStatement.addBatch();
                }

                int[] result = preparedStatement.executeBatch();
                rowsAffected = getNumberOfRowsUpdated(result);
                System.out.println("Total Rows " + rowsAffected + " updated in the table " + tableName);
                connection.commit();

            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
            return rowsAffected;
        }
        
        public Map<String, AggregatedResult> executeSourceMappingInfoQuery(Connection connection, String query) {
    
            Map<String, AggregatedResult> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {
    
                while (rs.next()) {
                    String connectionId = rs.getString("Connection_Id");
                    String tableSchema = rs.getString("Table_Schema");
                    String ilTableName = rs.getString("IL_Table_Name");
                    String key = connectionId + "-" + tableSchema + "-" + ilTableName;
    
                    String sourceTableName = rs.getString("Source_Table_Name");
                    String fileId = rs.getString("File_Id");
                    boolean isFileUpload = rs.getBoolean("IsFileUpload");
                    Timestamp updatedDate = rs.getTimestamp("Updated_Date");
                    String dimensionTransaction = rs.getString("Dimension_Transaction");
    
                    AggregatedResult aggregatedResult = resultMap.getOrDefault(key, new AggregatedResult());
                    aggregatedResult.setConnectionId(connectionId);
                    aggregatedResult.setTableSchema(tableSchema);
                    aggregatedResult.setIlTableName(ilTableName);
                    aggregatedResult.setSourceTableName(sourceTableName);
                    aggregatedResult.setFileId(fileId);
                    aggregatedResult.setIsFileUpload(isFileUpload);
                    aggregatedResult.updateMaxUpdatedDate(updatedDate);
                    aggregatedResult.setDimensionTransaction(dimensionTransaction);
                    
                    resultMap.put(key, aggregatedResult);
                }    
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return resultMap;
        }
    
        class AggregatedResult {
            private String connectionId;
            private String tableSchema;
            private String ilTableName;
            private String sourceTableName;
            private String fileId;
            private boolean isFileUpload;
            private java.sql.Timestamp maxUpdatedDate;
            private String dimensionTransaction;
    
            public String getConnectionId() {
                return connectionId;
            }
            public void setConnectionId(String connectionId) {
                this.connectionId = connectionId;
            }
            public String getTableSchema() {
                return tableSchema;
            }
            public void setTableSchema(String tableSchema) {
                this.tableSchema = tableSchema;
            }
            public String getIlTableName() {
                return ilTableName;
            }
            public void setIlTableName(String ilTableName) {
                this.ilTableName = ilTableName;
            }
            public String getSourceTableName() {
                return sourceTableName;
            }
            public void setSourceTableName(String sourceTableName) {
                this.sourceTableName = sourceTableName;
            }
            public String getFileId() {
                return fileId;
            }
            public void setFileId(String fileId) {
                this.fileId = fileId;
            }
            public boolean getIsFileUpload() {
                return isFileUpload;
            }
            public void setIsFileUpload(boolean isFileUpload) {
                this.isFileUpload = isFileUpload;
            }
            public Timestamp getMaxUpdatedDate() {
                return maxUpdatedDate;
            }
            public void updateMaxUpdatedDate(Timestamp updatedDate) {
                if (this.maxUpdatedDate == null || (updatedDate != null && updatedDate.after(this.maxUpdatedDate))) {
                    this.maxUpdatedDate = updatedDate;
                }
            }
            public String getDimensionTransaction() {
                return dimensionTransaction;
            }
            public void setDimensionTransaction(String dimensionTransaction) {
                this.dimensionTransaction = dimensionTransaction;
            }
        }
    }

    public class DWSourceInfoScriptsGenerator {
        public DWSourceInfoScriptsGenerator() {
        }
    
        // Generating the source info script
        public Status generateSourceInfoScript() {
            try {
                deletefromILSourceMappingInfo(conn, selectTables, connectionId, querySchemaCondition);
                List<Map<String, Object>> ilMappingInfoSavedDataList = getILSourceMappingInfoSavedData(conn, selectTables,
                        connectionId, querySchemaCondition);
                String tableName = "ELT_IL_Source_Mapping_Info";
                if (ilMappingInfoSavedDataList.size() > 0) {
                    saveDataIntoDB(conn, tableName, ilMappingInfoSavedDataList);
                    System.out.println("IL Source Mapping Info: Data loaded into the target " + tableName);
                } else {
                    System.out.println("IL Source Mapping Info: No data for the target " + tableName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return Status.FAILURE;
            }
            return Status.SUCCESS;
        }

        private List<Map<String, Object>> getILSourceMappingInfoSavedData(Connection connection, String selectiveTables,
                 String connectionId, String querySchemaCond) throws SQLException {
            String query = "SELECT `Connection_Id`, `Table_Schema`, `IL_Table_Name`, `IL_Column_Name`, `IL_Data_Type`, "
                    +
                    "`Constraints`, `Source_Table_Name`, `Source_Column_Name`, `Source_Data_Type`, `PK_Constraint`, " +
                    "`PK_Column_Name`, `FK_Constraint`, `FK_Column_Name`, `Dimension_Transaction`, `Dimension_Key`, " +
                    "`Dimension_Name`, `Dimension_Join_Condition`, `Constant_Insert_Column`, `Constant_Insert_Value`, "
                    +
                    "`Incremental_Column`, `History_Track`, `Null_Replacement_Value`, `Column_Type`, `Active_Flag`, " +
                    "`LHS_Join_Condition`, `RHS_Join_Condition`, `Isfileupload`, `File_Id`, `Added_Date`, `Added_User`, "
                    +
                    "`Updated_Date`, `Updated_User` " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                    "WHERE IL_Table_Name IN (" + selectiveTables + ") " +
                    "AND Connection_Id = ? " + querySchemaCond;

            List<Map<String, Object>> resultList = new ArrayList<>();

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (resultSet.next()) {
                        Map<String, Object> rowMap = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            rowMap.put(metaData.getColumnName(i), resultSet.getObject(i));
                        }
                        resultList.add(rowMap);
                    }
                }
            }
            return resultList;
        }
        private int deletefromILSourceMappingInfo(Connection connection, String selectiveTables, String connectionId, String querySchemaCondition) throws SQLException {
            String query = "DELETE FROM ELT_IL_Source_Mapping_Info " +
                           "WHERE IL_Table_Name IN (" + selectiveTables + ") " +
                           "AND Connection_Id = ? " + querySchemaCondition;
    

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                int rowsDeleted = preparedStatement.executeUpdate();
                System.out.println(rowsDeleted + " rows deleted from ELT_IL_Source_Mapping_Info where IL_Table_Name = " + selectiveTables);
                return rowsDeleted;
            } catch (SQLException e) {
                System.err.println("Error while deleting from ELT_IL_Source_Mapping_Info: " + e.getMessage());
                return 0;
            }
        }
    }

    public static class SQLQueries {

        public static String queryILSourceMappingInfoSaved = "SELECT Connection_Id, Table_Schema, IL_Table_Name, Source_Table_Name, File_Id, IsFileUpload, Updated_Date, Dimension_Transaction "
                + "FROM ELT_IL_Source_Mapping_Info_Saved "
                + "WHERE Source_Table_Name IS NOT NULL AND Source_Table_Name <> '' "
                + "ORDER BY IL_Table_Name, Source_Table_Name ASC";

        public static String queryILSourceMappingInfo = "SELECT ELT_IL_Source_Mapping_Info.Connection_Id, ELT_IL_Source_Mapping_Info.Table_Schema, "
                + "ELT_IL_Source_Mapping_Info.IL_Table_Name, ELT_IL_Source_Mapping_Info.Source_Table_Name, "
                + "ELT_IL_Source_Mapping_Info.File_Id, ELT_IL_Source_Mapping_Info.IsFileUpload, "
                + "ELT_IL_Source_Mapping_Info.Updated_Date, Dimension_Transaction "
                + "FROM ELT_IL_Source_Mapping_Info";

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
        public static Connection getConnection(DataSourceType dataSourceType, String dbDetails) throws SQLException {
            Connection connection = null;

            // JSONObject jsonDbDetails = new JSONObject(dbDetails);
            // String serverIP = jsonDbDetails.getString("appdb_hostname");
            // String serverPort = jsonDbDetails.getString("appdb_port");
            // String serverIPAndPort = serverIP + ":" + serverPort;
            // String schema = jsonDbDetails.getString("appdb_schema");
            // String userName = jsonDbDetails.getString("appdb_username");
            // String password = jsonDbDetails.getString("appdb_password");

            switch (dataSourceType) {
                case MYSQL:

                    // String mysqlUrl = "jdbc:mysql://" + serverIPAndPort + "/" + schema + "?noDatetimeStringSync=true";

                    // MySQL Connection Dummy
                    String mysqlUrl = "jdbc:mysql://172.25.25.124:4475/Mysql8_2_1009427_appdb?noDatetimeStringSync=true";
                    String mysqlUser = "root";
                    String mysqlPassword = "Explore@09";
                    connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                                    
                    // auto-commit
                    connection.setAutoCommit(true);
                    if (connection != null) {
                        System.out.println("DB Connection established");
                    }
                    break;
                case SNOWFLAKE:
                case SQLSERVER:
                    throw new IllegalArgumentException(dataSourceType + " is not supported yet.");
                default:
                    throw new IllegalArgumentException("Unsupported DataSourceType: " + dataSourceType);
            }

            return connection;
        }


        // Helper function to return a Target DB Connection object
        public static Connection getTargetDBConnection(DataSourceType dataSourceType, String dbDetails) throws SQLException {
            Connection connection = null;

            // JSONObject jsonDbDetails = new JSONObject(dbDetails);
            // String serverIP = jsonDbDetails.getString("datadb_hostname");
            // String serverPort = jsonDbDetails.getString("datadb_port");
            // String serverIPAndPort = serverIP + ":" + serverPort;
            // String schema = jsonDbDetails.getString("datadb_schema");
            // String userName = jsonDbDetails.getString("datadb_username");
            // String password = jsonDbDetails.getString("datadb_password");

            switch (dataSourceType) {
                case MYSQL:

                    // String mysqlUrl = "jdbc:mysql://" + serverIPAndPort + "/" + schema + "?noDatetimeStringSync=true&allowMultiQueries=true"; // Note the additional params

                    // MySQL Connection
                    String mysqlUrl = "jdbc:mysql://172.25.25.124:4475/Mysql8_2_1009427?noDatetimeStringSync=true&allowMultiQueries=true"; // Note the additional params
                    String mysqlUser = "root";
                    String mysqlPassword = "Explore@09";
                    connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                                    
                    // auto-commit
                    connection.setAutoCommit(true);
                    if (connection != null) {
                        System.out.println("DB Connection established");
                    }
                    break;
                case SNOWFLAKE:
                case SQLSERVER:
                    throw new IllegalArgumentException(dataSourceType + " is not supported yet.");
                default:
                    throw new IllegalArgumentException("Unsupported DataSourceType: " + dataSourceType);
            }

            return connection;
        }

        public static void closeDBConnection(Connection connection) {
            if (connection != null) {
                try {
                    if (!connection.isClosed()) {
                        connection.close();
                        System.out.println("DB Connection closed - " + connection.toString());
                    }
                } catch (SQLException e) {
                    System.err.println("Error while closing the connection: " + e.getMessage());
                }
            }
        }
    }
    public static void main(String[] args) {
        // Input values
        String clientId = "1009427";  // client ID
        // String connectionId =  "41"; // '41', '2' AbcAnalysis_Mysql8
        // String selectTables =  "'Finished_Goods_BOM', 'AbcAnalysis_Mysql8', 'Monthly_Forecasted_Qty_FQ'"; // AbcAnalysis_Mysql8

        // Set 2 for Dimension Transaction 'T'
        String connectionId =  "2"; // '109', '3', '2' AbcAnalysis_Mysql8
        // '2', 'SorDetail_Mysql8', 'SorDetail'             dbo
        // '3', 'AdmFormData_RedshiftSync'                dbo
        // '109', ACTB_HISTORY, STTM_CUSTOMER           TECUFEBTRI
        String selectTables =  "'SorDetail_Mysql8', 'SorDetail'"; // AbcAnalysis_Mysql8
        String filePath = "E:\\";
        // connectionId =  "114"; // AbcAnalysis_Mysql8
        // selectTables =  "'SorMaster_Spark3'"; // AbcAnalysis_Mysql8
        String schemaName = "dbo";

        String multiIlConfigFile = "Y";  // Set one of below
        multiIlConfigFile = "N";
        // String dbDetails = "{ \"appdb_username\": \"localuser\" }";
        String dbDetails = "{ \n" +
                "    \"appdb_username\": \"localuser\", \n" +
                "    \"stagingdb_hostname\": \"localhost\", \n" +
                "    \"stagingdb_port\": \"3306\", \n" +
                "    \"stagingdb_username\": \"staginguser\", \n" +
                "    \"stagingdb_password\": \"stagingpassword\", \n" +
                "    \"stagingdb_schema\": \"stagingdb\", \n" +
                "    \"datadb_hostname\": \"localhost\", \n" +
                "    \"datadb_port\": \"3306\", \n" +
                "    \"datadb_username\": \"datauser\", \n" +
                "    \"datadb_schema\": \"Mysql8_2_1009427\", \n" +
                "    \"datadb_password\": \"datapassword\" \n" +
                "}";
        String historicalDataFlag = "";
        String loadType = "";
        String dataSourceName = "";


        System.out.println("########################## Program Starts ####################");
        System.out.println("Inputs: clientId: " + clientId);
        DWScriptsGenerator generator = new DWScriptsGenerator(clientId,
                schemaName,
                connectionId,
                dataSourceName,
                loadType,
                multiIlConfigFile,
                historicalDataFlag,
                selectTables,
                dbDetails,
                filePath);
        generator.generateScripts();
    }
        

}
