package com.anvizent.datamart;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.json.JSONObject;

import com.anvizent.datamart.DWScriptsGenerator.DWConfigScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWSourceInfoScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWTableInfoScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWValueScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWValueScriptsGenerator.ConstantField;
import com.anvizent.datamart.DWScriptsGenerator.DWValueScriptsGenerator.PKColumnsData;
import com.anvizent.datamart.DataMartStructureScriptGenerator.DataMartValueScriptGenerator.SinkAggregationData;
import com.anvizent.datamart.DataMartStructureScriptGenerator.DataMartValueScriptGenerator.SourceFilterByAggregationData;

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

     

    private long clientId;
    private DataSourceType dataSourceType;
    private long dlId;
    private long jobId;
    private String dlName;
    private String filePath;
    private String dbDetails;
    private String tmpTableName;    
    private LocalDateTime startTime;
    private String startTimeString;


    private String userName = "ETL Admin"; // default user
    private String schemaName;
    private String customType;
    private String companyId;
    private String connectionId;
    private String dataSourceName;
    private int customFlag;
    private String selectTables;
    private String querySchemaCondition = ""; // based on TABLE_SCHEMA
    private String querySchemaCondition1 = ""; // based on Schema_Name

    SQLQueries sqlQueries;
    Connection conn;
    
    public DWScriptsGenerator(long clientId, DataSourceType type, long dlId, long jobId, String dlName) {
        this.clientId = clientId;
        this.dataSourceType = type;
        this.dlId = dlId;
        this.jobId = jobId;
        this.dlName = dlName;
        init();
    }

    // public DataMartStructureScriptGenerator(long clientId, DataSourceType type, long dlId, long jobId, String dlName, String filePath, String dbDetails) {
    //     this.clientId = clientId;
    //     this.dataSourceType = type;
    //     this.dlId = dlId;
    //     this.jobId = jobId;
    //     this.dlName = dlName;
    //     this.filePath = filePath;
    //     this.dbDetails = dbDetails;
    //     init();
    // }

    private void init() {
        // TODO test data 
        connectionId =  "41"; // '41', '2' AbcAnalysis_Mysql8
        // TODO use multiple selectTables
        selectTables =  "'Finished_Goods_BOM', 'AbcAnalysis_Mysql8', 'Monthly_Forecasted_Qty_FQ'"; // AbcAnalysis_Mysql8

        // Set 2 for Dimension Transaction 'T'
        connectionId =  "2"; // '109', '3', '2' AbcAnalysis_Mysql8
        // TODO use multiple selectTables
        // '2', 'SorDetail_Mysql8', 'SorDetail'             dbo
        // '3', 'AdmFormData_RedshiftSync'                dbo
        // '109', ACTB_HISTORY, STTM_CUSTOMER           TECUFEBTRI
        selectTables =  "'SorDetail_Mysql8', 'SorDetail'"; // AbcAnalysis_Mysql8

        // connectionId =  "114"; // AbcAnalysis_Mysql8
        // selectTables =  "'SorMaster_Spark3'"; // AbcAnalysis_Mysql8
        // schemaName = "dbo";
        tmpTableName = dlName + dlId + jobId;
        startTime = LocalDateTime.now();
        startTimeString = getCurrentDateFormatted(startTime);
        sqlQueries = new SQLQueries();
        try {
            // App DB connection
            conn = DBHelper.getConnection(dataSourceType);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // TODO - integration
        // JSONObject jsonDbDetails = new JSONObject(dbDetails);
        // userName = jsonDbDetails.getString("appdb_username");

        // The Below function shall create two Schema Conditions which are used
        // throughout the application
        Map<String, String> conditions = createQueryConditions(null, schemaName);
        this.querySchemaCondition = conditions.get("query_schema_cond");
        this.querySchemaCondition1 = conditions.get("query_schema_cond1");
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
    private void generateScripts() {

        // TODO - update comments
            // The DB scripts generator
        
        if (false && new DWDBScriptsGenerator().generateDBScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The config script generator
        if (new DWConfigScriptsGenerator().generateConfigScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The value script generator
        if (false && new DWValueScriptsGenerator().generateValueScript() != Status.SUCCESS) {
            System.out.println("Value script generation failed. Stopping process.");
            return;
        }

        // IL_Source_Info
        if (new DWSourceInfoScriptsGenerator().generateSourceInfoScript() != Status.SUCCESS) {
            System.out.println("Value script generation failed. Stopping process.");
            return;
        }
        // IL_Table_Info
        if (new DWTableInfoScriptsGenerator().generateTableInfoScript() != Status.SUCCESS) {
            System.out.println("Value script generation failed. Stopping process.");
            return;
        }
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'generateScripts'");
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
                    System.out.println(settingsJson);
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

    public class DWDBScriptsGenerator {

        private static final String CONDITIONAL_DATE = "Conditional_Date";
        private static final String CONDITIONAL_LIMIT = "Conditional_Limit";
        private static final String TRAILING_MONTHS = "Trailing_Months";
        private static final String HISTORICAL2 = "historical";
        private static final String INCREMENTAL_DATE = "Incremental_Date";
        private static final String INCREMENTAL_ID = "Incremental_Id";
        private static final String CONDITIONAL_FILTER = "Conditional_Filter";

        // Constructor
        public DWDBScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the DB script
        public Status generateDBScript() {
            // Placeholder logic for the method
            // Replace this with actual implementation
            try {
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
                    ilTableName = "AbcAnalysis_Spark3";
                    connectionId = "114";
                    tableSchema = "dbo";
                    schemaName = "dbo";
                    System.out.println("Connection_Id: " + connectionIdResult +
                            ", Table_Schema: " + tableSchema +
                            ", IL_Table_Name: " + ilTableName);
                     Boolean deleteFlag = getDeleteFlag(ilTableName);

                     Map<String, Map<String, Object>>  masterData = getMasterSourceMappingInfoData(conn, ilTableName, connectionId, querySchemaCondition);
                     System.out.println("Size of master data: " + masterData.size());
                     System.out.println("master data: " + masterData);

                    doCoreTransformations(conn, connectionId,tableSchema, ilTableName, querySchemaCondition);


                    // USed inside above funciton
                    //  Map<String, String> data = getSymbolValueforConnectionId(conn, connectionId);
                    //  System.out.println("Size of Symbol value data: " + data.size());
                    //  System.out.println("data: " + data);
                     
                     // TODO have single copy of the below
                     Map<String, Object> loadProperties = getLoadProperties(conn, connectionId,  ilTableName);
                     System.out.println("loadProperties: " + loadProperties);
                     Map<String, Object> dateParam = getConditionalDateParam(ilTableName, loadProperties);
                     System.out.println("conditional date: " + dateParam);
                     Map<String, Object> limitParam = getConditionalLimitParam(ilTableName, loadProperties);
                     System.out.println("conditional Limit: " + limitParam);
                     Map<String, Object> monthParam = getConditionalMonthParam(ilTableName, loadProperties);
                     System.out.println("conditional Month: " + monthParam);
                     Map<String, Object> filterParam = getConditionalFilterParam(ilTableName, loadProperties);
                     System.out.println("conditional Filter: " + filterParam);
                     Map<String, Object> historyDateParam = getHistoricalDateParam(ilTableName, loadProperties);
                     System.out.println("historical date: " + historyDateParam);
                     Map<String, Object> incrementalParam = getIncrementalParam(ilTableName, loadProperties);
                     System.out.println("incremental name/type: " + incrementalParam);

                     Map<Integer, Map<String, Object>> dwSettings = getDWSettings(conn, Integer.valueOf(connectionId));
                     System.out.println("DW settings: " + dwSettings);

                     // row6 (lookup)
                     Map<String, String> joinedData = aggregateColumnNames(conn, ilTableName, Integer.valueOf(connectionId), querySchemaCondition);
                     System.out.println("Aggregated data: " + joinedData);

                     // Deletes(lookup)
                     String query = getCoreSourceMappingInfoQuery(querySchemaCondition);
                     Map<String, Map<String, Object>> res = processSourceMapping(conn, ilTableName, connectionId, query);
                     System.out.println("Ptocess Source mapping: " + res);
                    // ELT_Selective_Source_Metadata`
                     Map<String, Map<String, Object>> selectivesourcemeta = getSelectiveSourceMetadata(conn, connectionId, querySchemaCondition);
                     System.out.println("Selective Source metadata size: " + selectivesourcemeta.size());
                    //  System.out.println("Selective Source metadata: " + selectivesourcemeta);
                     // ELT_Custom_Source_Metadata_Info
                     Map<String, Map<String, Object>> sourcemeta = getCustomSourceMetadata(conn);
                     System.out.println("Custom Source metadata size : " + sourcemeta.size());
                    //  System.out.println("Custom Source metadata : " + sourcemeta);






                }
            // Another Script
            // Create Script transaction group
            List<Map<String, Object>> createScripData = GetCreateScriptScriptTransactionData(conn, selectTables, connectionId, querySchemaCondition);
            System.out.println("Create Script : " + createScripData);

            } catch (SQLException e) {
                // TODO return failure status
                e.printStackTrace();
            }

            return Status.SUCCESS;
        }

        private void doCoreTransformations(Connection connection, String connectionId, String tableSchema, String ilTableName, String querySchemaCond) throws SQLException {

            System.out.println("");
            System.out.println("");
            Map<String, Map<String, Object>>  masterData = getMasterSourceMappingInfoData(conn, ilTableName, connectionId, querySchemaCondition);
            Map<String, Object> masterDataForConId = masterData.get(ilTableName);
            String row1Symbol = (String) masterDataForConId.get("Symbol");
            String row1Columns = (String) masterDataForConId.get("Columns");
            String row1SourceTableName = (String) masterDataForConId.get("Source_Table_Name");
            // String row1SourceTableName = (String) masterDataForConId.get("Source_Table_Name");

            System.out.println("symbol: " + row1Symbol);
            System.out.println("Columns: " + row1Columns);
            System.out.println("Columns: " + row1Columns);


            // Initialize variables with null checks and ternary operators where applicable.
            //delete_flag
            Boolean deleteFlag = getDeleteFlag(ilTableName);
            // String deleteFlag = (String) globalMap.get("delete_flag");

            Map<String, Object> loadProperties = getLoadProperties(conn, connectionId,  ilTableName);
            System.out.println("loadProperties: " + loadProperties);
            // row2
            // DW Settings - server name
            Map<Integer, Map<String, Object>> row6DWSettings = getDWSettings(conn, Integer.valueOf(connectionId));
            // System.out.println("row6DWSettings value: " + row6DWSettings);
            String row2Name = (String) row6DWSettings.get(Integer.parseInt(connectionId)).get("name");
            System.out.println("name: " + row2Name);

            // row6 (lookup)
            Map<String, String> joinedData = aggregateColumnNames(conn, ilTableName, Integer.valueOf(connectionId), querySchemaCond);
            String row6LookupKey = connectionId + "-" + tableSchema + "-" + ilTableName;
            String row6ILColumnName = joinedData.get(row6LookupKey);
            System.out.println("Aggregated data: " + joinedData);
            System.out.println("row6LookupKey: " + row6LookupKey);
            System.out.println("row6ILColumnName: " + row6ILColumnName);

            // server
            String server = row2Name == null ? ""
                    : row2Name.equals("SQL Server") ? " Order by  " + (row6ILColumnName == null ? " Company_Id " : row6ILColumnName) : "";
            System.out.println("server: " + server);

            // conditional_limit
            Map<String, Object> limitParam = getConditionalLimitParam(ilTableName, loadProperties);
            System.out.println("conditional Limit: " + limitParam);
            String limitParamSettingsCategory = (String) limitParam.get("Settings_Category");
            String limitParamSettingsValue = (String) limitParam.get("Setting_Value");
            // Var_conditionallimit
            String conditionalLimitSettingCategory = limitParamSettingsCategory == null ? "" : limitParamSettingsCategory;
            // Var_limit
            String limitValue = limitParamSettingsCategory == null ? ""
                    : limitParamSettingsCategory.equals("Conditional_Limit") ? limitParamSettingsValue : "";
            System.out.println("conditional Limit: " + conditionalLimitSettingCategory + ", " + limitValue);

            // dates (lookup)
            Map<String, Object> datesDWSettings = row6DWSettings.get(Integer.parseInt(connectionId));
            System.out.println("datesDWSettings: " + datesDWSettings);
            String datesIncrementalDate = (String) datesDWSettings.get("Incremental_Date");
            String datesConditionalDate = (String) datesDWSettings.get("Conditional_Date");

            // conditional (lookup)
            Map<String, Object> conditional = getConditionalDateParam(ilTableName, loadProperties);
            System.out.println("conditional date: " + conditional);
            // String dateParamConditionalLimit = (String) conditional.get("Settings_Category");
            // Var_ConditionalLimitsett
            String conditionalLimitSetting = (String) datesDWSettings.get("Conditional_Limit") == null ? "limit" : (String) datesDWSettings.get("Conditional_Limit");
            System.out.println("conditionalLimitSetting: " + conditionalLimitSetting);
            // Var_conditional
            String conditionalSetting = conditional.get("Settings_Category") == null ? "" : (String) conditional.get("Settings_Category");
            System.out.println("conditionalSetting: " + conditionalSetting);

            // incremental1
            Map<String, Object> incremental1 = getIncrementalParam(ilTableName, loadProperties);
            System.out.println("incremental name/type: " + incremental1);
            String incremental1SettingCategory = (String) incremental1.get("Settings_Category");
            String incremental1SettingValue = (String) incremental1.get("Setting_Value");
            // Var_Incremental
            String incrementalSetting = incremental1SettingCategory == null ? "" : incremental1SettingCategory;

            // Conditional_filter1 (Lookup), Conditional_filter
            Map<String, Object> conditionalFilter1 = getConditionalFilterParam(ilTableName, loadProperties);
            System.out.println("conditional Filter: " + conditionalFilter1);
            // Conditionalfilter
            String conditionalFilter = conditionalFilter1.get("Settings_Category") == null ? "" : (String) conditionalFilter1.get("Settings_Category");
            System.out.println("conditionalFilter: " + conditionalFilter);
            //IL_Column_Name
            String ilColumnName = row1Symbol == null ? " {Schema_Company} as Company_Id ," :
                row1Symbol.equals("\"") ? " {Schema_Company} as \"Company_Id\" ," : " {Schema_Company} as Company_Id ,";
            System.out.println("ilColumnName: " + ilColumnName);

            // Conditional_limit, Limit
            Map<String, Object> conditionalLimit = getConditionalLimitParam(ilTableName, loadProperties);
            System.out.println("conditional Limit: " + limitParam);

            String statement = conditionalLimitSetting.toLowerCase().contains("limit") ? " Select "
                    : conditionalLimitSetting.toLowerCase().contains("top")
                            && conditionalLimit.get("Settings_Category") == null ? " Select  "
                                    : conditionalLimitSetting.toLowerCase().contains(
                                            "top") && conditionalLimit.get("Settings_Category").equals("Conditional_Limit") ? " Select TOP " + limitValue : " Select ";
            System.out.println("statement: " + statement);

            // Deletes(lookup)
            String query = getCoreSourceMappingInfoQuery(querySchemaCondition);
            Map<String, Map<String, Object>> deletesAggregateData = processSourceMapping(conn, ilTableName, connectionId, query);
            System.out.println("Ptocess Source mapping: " + deletesAggregateData);
            Map<String, Object> deletes = deletesAggregateData.get(ilTableName);

            String columns = row1Columns;
            String deleteColumns = (String) deletes.get("Columns");

            String ending = " from " + (row1Symbol == null ? ("[" + row1SourceTableName + "]") :
                row1Symbol.equals("[") ? ("[" + schemaName + "].[" + row1SourceTableName + "]") :
                row1Symbol.equals("`") ? ("`" + schemaName + "`.`" + row1SourceTableName + "`") :
                row1Symbol.equals("\"") ? ("\"" + schemaName + "\".\"" + row1SourceTableName + "\"") :
                ("[" + schemaName + "].[" + row1SourceTableName + "]"));
            System.out.println("deleteColumns: " + deleteColumns);
            System.out.println("ending: " + ending);

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
            System.out.println("months1: " + months1);


            String finalConditionalMonth = finalConditionalDate == null ? "" : finalConditionalDate.replace("Months", (String) months1.get("Setting_Value"));
            System.out.println("finalConditionalMonth: " + finalConditionalMonth);

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
                
                System.out.println("finalConditionsLimit: " + finalConditionsLimit);

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
                System.out.println("finalConditionsTop: " + finalConditionsTop);

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
                System.out.println("finalIncrementalLimit: " + finalIncrementalLimit);

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

                System.out.println("finalIncrementalTop: " + finalIncrementalTop);

                // Hostory_Date (lookup)
                Map<String, Object> historyDate = getHistoricalDateParam(ilTableName, loadProperties);
                System.out.println("historical date: " + historyDate);
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

                    System.out.println("historicalCond: " + historicalCond);
                    System.out.println("condition: " + condition);


                    // ELT_Selective_Source_Metadata
                    Map<String, Map<String, Object>> selectiveSourceMetadata = getSelectiveSourceMetadata(conn, connectionId, querySchemaCondition);
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
                    
                        System.out.println("finalScript: " + finalScript);
                        System.out.println("finalizedScript: " + finalizedScript);
                        System.out.println("customType: " + customType);
                        System.out.println("maxSelectScript: " + maxSelectScript);

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

                        System.out.println("historicalScript: " + historicalScript);
                        System.out.println("finalHistoricalQuery: " + finalHistoricalQuery);

                        System.out.println("customTypeInfo: " + customTypeInfo);

                        System.out.println("sourceType: " + sourceType);

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

            // Continue converting the remaining logic and add comments for clarity where needed.
            System.out.println("");
            System.out.println("");

        }
         /**
         * Executes a SQL query to fetch master data from `ELT_IL_Source_Mapping_Info_Saved`and returns the result as a list of maps.
         */
        // TODO main flow master data
        public Map<String, Map<String, Object>> getMasterSourceMappingInfoData(Connection connection, String ilTableName,
                String connectionId, String querySchemaCond) throws SQLException {
            
            // TODO It should be single value at max
            Map<String, String> symbolValues = getSymbolValueforConnectionId(conn, connectionId);
            System.out.println("Size of Symbol value data:  " + symbolValues.size());
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

        // TODO: loadProperties make a singleton
        private Boolean getDeleteFlag(String ilTableName) throws SQLException {
            Map<String, Object> result = getLoadProperties(conn, connectionId,  ilTableName);
             Boolean deleteFlag = (Boolean) result.get("delete_flag");
             deleteFlag = null;
             if (deleteFlag == null) {
                 deleteFlag = false;
             }
             System.out.println("Delete Flag: " + deleteFlag);
            //  Map<String, Object> result = getLoadProperties(conn, "114",  "SorMaster_Spark3");
            //  System.out.println("Result Map: " + (String) result.get("write_mode"));
            //  System.out.println("Result Map: " + (Boolean) result.get("delete_flag"));
            return deleteFlag;
        }
        private Boolean getDeleteFlag(String ilTableName) throws SQLException {
            Map<String, Object> result = getLoadProperties(conn, connectionId,  ilTableName);
             Boolean deleteFlag = (Boolean) result.get("delete_flag");
             deleteFlag = null;
             if (deleteFlag == null) {
                 deleteFlag = false;
             }
             System.out.println("Delete Flag: " + deleteFlag);
            //  Map<String, Object> result = getLoadProperties(conn, "114",  "SorMaster_Spark3");
            //  System.out.println("Result Map: " + (String) result.get("write_mode"));
            //  System.out.println("Result Map: " + (Boolean) result.get("delete_flag"));
            return deleteFlag;
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
                        System.out.println(settingsJson);
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

    /*
     * Create_Script_Transactioon Group - main left outer Join
     */
    public List<Map<String, Object>> GetCreateScriptScriptTransactionData(Connection dbConnection, String selectiveTables,
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
                "  AND `Dimension_Transaction` = 'T' " +
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
                "  AND `Dimension_Transaction` = 'T' " +
                "  AND `Connection_Id` = ? " +
                querySchemaCond;

        // SQL for LEFT OUTER JOIN
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
            // Set parameters for Connection_Id
            ps.setString(1, connectionId);
            ps.setString(2, connectionId);

            try (ResultSet rs = ps.executeQuery()) {
                // Process Result Set
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

    //#####################################################
        // Alter JOb
        // Note String return type
        // Simple timestamp may also work
        // global variable set as "max_updated_date"
        private String getMaxUpdatedDate(Connection connection, String selectiveTables, String connectionId, String querySchemaCond) throws Exception {
            String query = "SELECT MAX(Updated_Date) FROM ELT_IL_Source_Mapping_Info WHERE IL_Table_Name IN (" + selectiveTables + ") AND Connection_Id = ? " + querySchemaCond;
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        Timestamp maxUpdatedDate = resultSet.getTimestamp(1);
                        if (maxUpdatedDate == null) {
                            return null;
                        } else {
                            return "'" + maxUpdatedDate + "'"; // Transformation: Format as a string with single quotes
                        }
                    }
                }
            } catch (Exception e) {
                throw new Exception("Error while executing getMaxUpdatedDate query.", e);
            }
            // Returning null if no row is found
            return null;
        }
        /* row1 - to call alter_delete iterative job*/
        private List<Map<String, String>> getDistinctMappingInfo(Connection connection, String selectiveTables, String connectionId, String querySchemaCond) throws Exception {
            String query = "SELECT DISTINCT `ELT_IL_Source_Mapping_Info_Saved`.`Connection_Id`, `ELT_IL_Source_Mapping_Info_Saved`.`Table_Schema`, `ELT_IL_Source_Mapping_Info_Saved`.`IL_Table_Name` " +
                           "FROM `ELT_IL_Source_Mapping_Info_Saved` WHERE IL_Table_Name IN (" + selectiveTables + ") AND Connection_Id = ? " + querySchemaCond;
    
            List<Map<String, String>> result = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, String> row = new HashMap<>();
                        row.put("Connection_Id", resultSet.getString("Connection_Id"));
                        row.put("Table_Schema", resultSet.getString("Table_Schema"));
                        row.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));
                        result.add(row);
                    }
                }
            } catch (Exception e) {
                throw new Exception("Error while executing getDistinctMappingInfo query.", e);
            }
    
            return result;
        }
        /* to check if table exists but check it in target DB */
        // Do we need dtabase name here 
        // Function to check if a table exists in the target database, excluding views
        public boolean doesTableExist(Connection targetConnection, String tableName, String databaseName) {
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
        private List<Map<String, String>> executeAntiJoinQuery(Connection connection, String ilTableName, String connectionId, String querySchemaCond) throws Exception {
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
                        row.put("IL_Column_Name", "Drop Column `" + resultSet.getString("IL_Column_Name") + "`"); // Transformation
                        result.add(row);
                    }
                }
            } catch (Exception e) {
                throw new Exception("Error while executing anti-join query.", e);
            }
        
            return result;
        }
// main flow of alter delete
        void alterDeleteScript (String ilTableName) {

            // TODO try catch block
            try {
                List<Map<String, String>> data = executeAntiJoinQuery(conn, ilTableName, connectionId, querySchemaCondition);

                String finalDropColumn = new String(); 
                for (Map<String, String> map : data) {
                    String ilColumnName = map.get("IL_Column_Name");
                    
                    System.out.println("IL_Column_Name: " + ilColumnName);

                    // Initialize variables
                    String dropColumn = ilColumnName; // Corresponds to copyOfResult.IL_Column_Name
                    // TODO check below original conversion is valid? As we are doing aggregation later. It is same effect.
                    // String finalDropColumn = (finalDropColumn == null) ? dropColumn + "," : finalDropColumn + dropColumn + ",";
                    finalDropColumn = (finalDropColumn == null) ? dropColumn + "," : finalDropColumn + dropColumn + ",";


                    // Construct the main script
                    String script = "ALTER TABLE `" + ilTableName + "` " + finalDropColumn; // Corresponds to copyOfResult.IL_Table_Name
                    String ilAlterScript = script.substring(0, script.length() - 1) + ";"; // Remove the last comma and append a semicolon

                    // Construct the staging script
                    String stgScript = "ALTER TABLE `" + ilTableName + "_Stg` " + finalDropColumn;
                    String stgAlterScript = stgScript.substring(0, stgScript.length() - 1) + ";"; // Remove the last comma and append a semicolon

                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
        // With all the aggregation
        void alterDeleteScriptComplete(String ilTableName) throws Exception {

            List<Map<String, String>> data = executeAntiJoinQuery(conn, ilTableName, connectionId, querySchemaCondition);
            Map<String, Map<String, String>> groupedScripts = new HashMap<>();
        
            for (Map<String, String> map : data) {
                String connectionId = map.get("Connection_Id");
                String tableSchema = map.get("Table_Schema");
                String tableName = map.get("IL_Table_Name");
                String ilColumnName = map.get("IL_Column_Name");
        
                System.out.println("IL_Column_Name: " + ilColumnName);
        
                // Construct group key
                String groupKey = connectionId + "-" + tableSchema + "-" + tableName;
        
                // Retrieve or initialize the group entry
                Map<String, String> group = groupedScripts.getOrDefault(groupKey, new HashMap<>());
        
                // Initialize or append drop columns
                String finalDropColumn = group.getOrDefault("Final_Drop_Column", "") + ilColumnName + ",";
        
                // Construct the main and staging scripts
                String ilAlterScript = "ALTER TABLE `" + tableName + "` " + finalDropColumn;
                ilAlterScript = ilAlterScript.substring(0, ilAlterScript.length() - 1) + ";"; // Remove the last comma and append a semicolon
        
                String stgAlterScript = "ALTER TABLE `" + tableName + "_Stg` " + finalDropColumn;
                stgAlterScript = stgAlterScript.substring(0, stgAlterScript.length() - 1) + ";"; // Remove the last comma and append a semicolon
        
                // Update the group entry
                group.put("Final_Drop_Column", finalDropColumn);
                group.put("IL_Alter_Script", group.getOrDefault("IL_Alter_Script", "") + ilAlterScript + "\n");
                group.put("Stg_Alter_Script", group.getOrDefault("Stg_Alter_Script", "") + stgAlterScript + "\n");
        
                // Store back the group entry
                groupedScripts.put(groupKey, group);
            }
        
            // Print aggregated results
            for (Map.Entry<String, Map<String, String>> entry : groupedScripts.entrySet()) {
                String groupKey = entry.getKey();
                Map<String, String> group = entry.getValue();
        
                System.out.println("Group: " + groupKey);
                System.out.println("IL_Alter_Script:\n" + group.get("IL_Alter_Script"));
                System.out.println("Stg_Alter_Script:\n" + group.get("Stg_Alter_Script"));
            }
        }
        
        
 //   }
    
/*
 * ######################################################################################################
 * ELT Delete Jobs - start
 */
        

 

        // With Group_Concat - compaer with StringBuilder alternative
        private String getValueNameFromJobPropertiesInfo(Connection connection) throws SQLException {
            String query = "SELECT GROUP_CONCAT(Value_Name SEPARATOR ',') AS Value_Names \n" +
                    "FROM ELT_Job_Properties_Info \n" +
                    "WHERE Job_Type='Deletes_Dim' \n" +
                    "  AND Active_Flag=1 \n" +
                    "  AND Dynamic_Flag=1";
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("Value_Names");
                }
                return "";
            }
        }
        // StringBuilder alternative
        // value of global map lhs set
        private String getValueNameFromJobPropertiesInfo2(Connection connection) throws SQLException {
            String query = "SELECT Value_Name \n" +
                    "FROM ELT_Job_Properties_Info \n" +
                    "WHERE Job_Type='Deletes_Dim' \n" +
                    "  AND Active_Flag=1 \n" +
                    "  AND Dynamic_Flag=1";
    
            try (PreparedStatement preparedStatement = connection.prepareStatement(query);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                StringBuilder finalValueName = new StringBuilder();
                while (resultSet.next()) {
                    String valueName = resultSet.getString("Value_Name");
                    if (finalValueName.length() > 0) {
                        finalValueName.append(",");
                    }
                    finalValueName.append(valueName);
                }
                return finalValueName.toString();
            }
        }

        
 /*
 * ELT Delete Jobs - End
 * ######################################################################################################
 */

}
    public class DWConfigScriptsGenerator {

        String limitFunction;

        // Constructor
        public DWConfigScriptsGenerator() {
            // Initialization logic if needed
            // TODO: INTEGRATION: initialize limitFunc from input parameters
            String multiIlConfigFile = "Y";
            multiIlConfigFile = "N";
            //limitFunction = new String();
            if (multiIlConfigFile.equals("Y")) {
                limitFunction = "";
            } else {
                limitFunction = " limit 1";
            }
            // globalMap.put("limitFunction", limitFunction);             
        }
    
        // Method to generate the configuration script
        public Status generateConfigScript() {
            System.out.println("### Generating Config Scripts ...");

            // Placeholder logic for the method
            // Replace this with actual implementation


            // Dim_SRC_STG


            // Dim_STG_IL


            // Trans_SRC_STG


            // Trans_STG_Keys


            // Trans_STG_IL


            // Deletes_Dim


            // Deletes_Trans 




            // Only Delete Logic as of now
            // TODO specific to DIM delete Values
            String dimensionTransaction = "D";
            try {
                // TODO - testing purpose only
                List<Map<String, String>> tableNames = getILTableNamesWithDimentionTransactionFilter(
                        conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition, limitFunction);
                System.out.println("list of tableNames: " + tableNames);

                // TODO - testing purpose only
                dimensionTransaction = "T";
                tableNames = getILTableNamesWithDimentionTransactionFilter(
                        conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition, limitFunction);
                System.out.println("list of tableNames: " + tableNames);

                dimDeleteConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);
                
                transDeleteConfigScript(conn, selectTables, connectionId, querySchemaCondition, limitFunction);
                // dimensionTransaction = "D";
                // dimDeleteScriptComplete(conn, selectTables, connectionId,
                // querySchemaCondition);

                // transDeleteScriptComplete(conn, selectTables, connectionId, querySchemaCondition);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            return Status.SUCCESS;
        }

        void dimSrcToStgConfigScript(Connection connection, String selectiveTables, 
        String connectionId, String querySchemaCond, String limitFunct) throws SQLException {

            String dimensionTransaction = "D";
            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("\n    list of data (dimSrcToStgConfigScript): " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                // String multiIlConfigFile = context.MultiIlConfigFile;
                // TODO: integration fix above
                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }
            }


        }

        void dimDeleteConfigScript(Connection connection, String selectiveTables, 
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {
            
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

                // String multiIlConfigFile = context.MultiIlConfigFile;
                // TODO: integration fix above
                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }
                // globalMap.put("ilTable", ilTable);

                // todo : initilize properly
                String tableName2 = "", ilColumnName = "";
                System.out.println("    iLTableName: " + ilTableName);
                System.out.println("        ilTable: " + ilTable);

                Map<String, AggregatedData> mainDataMap = processConfigJobProperties(connection, connectionId, tableSchema, jobType);

                final String fileName = getDeletesConfigFileName(ilTableName);
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
                    saveDataIntoDB(connection, tableName, finalResults);
                }

            }
        }

        void transDeleteConfigScript(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond, String limitFunct) throws SQLException {

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

                // String multiIlConfigFile = context.MultiIlConfigFile;
                // TODO: integration fix above
                String multiIlConfigFile = "Y";
                String ilTable = "";
                if (multiIlConfigFile.equals("Y")) {
                    ilTable = ilTableName + "_Stg";
                } else {
                    ilTable = "DIM_SRC_STG_All";
                }
                // globalMap.put("ilTable", ilTable);

                // todo : initilize properly
                String tableName2 = "", ilColumnName = "";
                System.out.println("    iLTableName: " + ilTableName);
                System.out.println("        ilTable: " + ilTable);

                Map<String, AggregatedData> mainDataMap = processConfigJobProperties(connection, connectionId,
                        tableSchema, jobType);

                final String fileName = getDeletesConfigFileName(ilTableName);
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
                    saveDataIntoDB(connection, tableName, finalResults);
                }

            }
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

            // Output aggregated data
            // TODO: removal required
            aggregatedDataMap.forEach((key, data) -> {
                System.out.println("Key: " + key);
                System.out.println("Aggregated Script: " + data.getScript());
            });

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
                           + ilTable + "_Stg_Keys' AND Connection_Id = '" + connectionId + "'";
            try (Statement statement = connection.createStatement()) {
                int rowsDeleted = statement.executeUpdate(query);
                System.out.println(rowsDeleted + " rows deleted from ELT_CONFIG_PROPERTIES.");
            } catch (SQLException e) {
                System.err.println("Error while deleting from ELT_CONFIG_PROPERTIES: " + e.getMessage());
            }
        }
        
        private String getDeletesConfigFileName(String ilTableName) {
            String suffix = getTimeStamp();
            String configFileName = filePath + ilTableName + "_Deletes_Config_File_" + suffix + ".config.properties"; // filePath is the directory name
            return configFileName;
        }
    }
    
    public class DWValueScriptsGenerator {
        String limitFunction;
        // Constructor
        public DWValueScriptsGenerator() {
            // Initialization logic if needed
            limitFunction = "";

        }
    
        // Method to generate the value script
        public Status generateValueScript() {
            System.out.println("### Generating Value Scripts ...");
            // Placeholder logic for the method
            // Replace with actual implementation

            // Only Delete Logic as of now
            // TODO specific to DIM delete Values
            String dimensionTransaction = "D";
            try {
                // TODO - testing purpose only
                String limitFunct = "";
                List<Map<String, String>> tableNames = getILTableNamesWithDimentionTransactionFilter(
                        conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition, limitFunction);
                System.out.println("list of tableNames: " + tableNames);

                // TODO - testing purpose only
                dimensionTransaction = "T";
                tableNames = getILTableNamesWithDimentionTransactionFilter(
                    conn, selectTables, dimensionTransaction, connectionId, querySchemaCondition, limitFunction);
                System.out.println("list of tableNames: " + tableNames);

                //dimensionTransaction = "D";
                //dimDeleteScriptComplete(conn, selectTables, connectionId, querySchemaCondition);
                
                transDeleteScriptComplete(conn, selectTables, connectionId, querySchemaCondition);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


            return Status.SUCCESS; // Return true for success, false for failure
        }

        void dimDeleteScriptComplete(Connection connection, String selectiveTables, 
                String connectionId, String querySchemaCond) throws SQLException {
            
            String dimensionTransaction = "D";
            String jobType = "Deletes_Dim";
            String limitFunct = "";
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            // TODO: Change limitFunct appropriately for the case - ( )
            // TODO: it is different in two cases based on dimensionTransaction
            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            Map<String, Map<String, String>> groupedScripts = new HashMap<>();
            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                // todo : initilize properly
                String tableName = "", ilColumnName = "";
                System.out.println("    iLTableName: " + ilTableName);

                // TODO it is different in two cases based on jobType
                String lhs = getValueNameFromJobPropertiesInfo2(connection, jobType);
                System.out.println("    lhs: " + lhs);

                // String fileName = getDeletesValueFileName(ilTableName);
                // System.out.println("deletes value fileName: " + fileName);

                Map<String, AggregatedData> mainDataMap = getAggregatedMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    mainDataMap: " + mainDataMap.size());
                System.out.println("    mainDataMap: " + mainDataMap);

                Map<String, ConstantField> constantFieldsLookupMap = getConstantFieldsMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap.size());
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap);
    
                Map<String, PKColumnsData> pkColumnsLookupMap = getPKColumnsMappingInfo(connection, ilTableName, connectionId, querySchemaCond);
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap.size());
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap);
    
            // TODO: it is different in two cases based on mapping data
                List<Map<String, Object>> finalData = dimFinalDataMapping(mainDataMap, constantFieldsLookupMap, pkColumnsLookupMap, lhs);
                System.out.println("    finalData: " + finalData.size());
                System.out.println("    finalData: " + finalData);

                insertIntoEltValuesProperties(conn, finalData);
            }
        }
        
        void transDeleteScriptComplete(Connection connection, String selectiveTables,
                String connectionId, String querySchemaCond) throws SQLException {

            String dimensionTransaction = "T";
            String jobType = "Deletes_Trans";
            String limitFunct = ""; // TODO shall be taken from outside
            System.out.println("\ndimensionTransaction: " + dimensionTransaction + ", jobType: " + jobType);

            List<Map<String, String>> data = getILTableNamesWithDimentionTransactionFilter(connection, selectiveTables,
                    dimensionTransaction, connectionId, querySchemaCond, limitFunct);
            System.out.println("    list of data: " + data);

            for (Map<String, String> map : data) {
                String tableSchema = map.get("Table_Schema");
                String ilTableName = map.get("IL_Table_Name");

                // todo : initilize properly
                String tableName = "", ilColumnName = "";
                System.out.println("    iLTableName: " + ilTableName);

                // TODO it is different in two cases based on jobType
                String lhs = getValueNameFromJobPropertiesInfo2(connection, jobType);
                System.out.println("    lhs: " + lhs);

                // String fileName = getDeletesValueFileName(ilTableName);
                // System.out.println("deletes value fileName: " + fileName);

                Map<String, AggregatedData> mainDataMap = getAggregatedMappingInfo(connection, ilTableName,
                        connectionId, querySchemaCond);
                System.out.println("    mainDataMap: " + mainDataMap.size());
                System.out.println("    mainDataMap: " + mainDataMap);

                Map<String, ConstantField> constantFieldsLookupMap = getConstantFieldsMappingInfo(connection,
                        ilTableName, connectionId, querySchemaCond);
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap.size());
                System.out.println("    constantFieldsLookupMap: " + constantFieldsLookupMap);

                Map<String, PKColumnsData> pkColumnsLookupMap = getPKColumnsMappingInfo(connection, ilTableName,
                        connectionId, querySchemaCond);
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap.size());
                System.out.println("    pkColumnsLookupMap: " + pkColumnsLookupMap);

                // TODO: it is different in two cases based on mapping data
                List<Map<String, Object>> finalData = transFinalDataMapping(mainDataMap, constantFieldsLookupMap,
                        pkColumnsLookupMap, lhs);
                System.out.println("    finalData: " + finalData.size());
                System.out.println("    finalData: " + finalData);

                insertIntoEltValuesProperties(conn, finalData);
            }
        }

        // Good One, with jobType argument, valid for both Trans and Dim Delete Values
        private String getValueNameFromJobPropertiesInfo2(Connection connection, String jobType) throws SQLException {
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
                            finalValueName.append(",");
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
                    System.out.println("Value: " + value.toString()); // Adjust as per AggregatedData's fields
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
                System.out.println("...    valueFile  : " + valueFile);
                String fileName = getDeletesValueFileName(ilTableName);
                System.out.println("deletes value fileName: " + fileName);
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
                System.out.println("...    valueFile  : " + valueFile);
                String fileName = getDeletesValueFileName(ilTableName);
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

        // row7, datatype_conversion
        private Map<String, Map<String, String>> getDatatypeConversions(Connection connection) throws SQLException {
            String query = "SELECT " +
                           "  Id, " +
                           "  LOWER(Source_Data_Type) AS sourceDataType, " +
                           "  LOWER(SUBSTRING_INDEX(IL_Data_Type, '(', 1)) AS ilDataType, " +
                           "  Java_Data_Type AS javaDataType, " +
                           "  Cleansing_Value AS cleansingValue, " +
                           "  PK_Cleansing_Value AS pkCleansingValue " +
                           "FROM ELT_Datatype_Conversions";
        
            Map<String, Map<String, String>> datatypeConversionsMap = new HashMap<>();
        
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
            }
            return datatypeConversionsMap;
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
                            javaDataType = (String) conversionDetails.get("javaDataType");
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
                    insertPs.setBoolean(5, (Boolean) row.get("Active_Flag").equals("1"));
                    insertPs.setTimestamp(6, (Timestamp) row.get("Added_Date")); // TODO change the values appropriately
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

        public void deleteValuesProperties(Connection connection, String ilTableName, String connectionId)
                throws SQLException {
            String query = "DELETE FROM ELT_VALUES_PROPERTIES WHERE IL_Table_Name = ? AND Connection_Id = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName + "_Deletes");
                preparedStatement.setString(2, connectionId);

                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println("ELT_VALUES_PROPERTIES rows deleted for '" + ilTableName + "' : " + rowsAffected);
            } catch (SQLException e) {
                throw new SQLException("Error while deleting values properties", e);
            }
        }

        private String getDeletesValueFileName(String ilTableName) {
            String suffix = getTimeStamp();
            String valueFileName = filePath + ilTableName + "_Deletes_Value_File_" + suffix + ".values.properties"; // filePath is the directory name
            return valueFileName;
        }
    }
    
    public class DWSourceInfoScriptsGenerator {

        // Constructor
        public DWSourceInfoScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the source info script
        public Status generateSourceInfoScript() {
            // Placeholder logic for the method
            // Replace with actual implementation
            return Status.SUCCESS; // Return true for success, false for failure
        }
    }
    public class DWTableInfoScriptsGenerator {

        // Constructor
        public DWTableInfoScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the table info script
        public Status generateTableInfoScript() {
            // Placeholder logic for the method
            // Replace with actual implementation
            return Status.SUCCESS; // Return true for success, false for failure
        }

        
    }

    public static class SQLQueries {
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
    }
    public static void main(String[] args) {
        // Input values
        long clientId = 1009427;  // client ID
        long tableId = 9;  //  table ID (DL_Id)
        String dlName = "test_1";  // DataMart Name
        long jobId = 11;  // job ID

        System.out.println("########################## Program Starts ####################");
        System.out.println("Inputs: clientId: " + clientId);
        System.out.println("Inputs: dlId: " + tableId + ", JobId: " + jobId + ", dlName: " + dlName);
        DWScriptsGenerator generator = new DWScriptsGenerator(clientId, DataSourceType.MYSQL, tableId, jobId, dlName);
        generator.generateScripts();
    }
        

}
