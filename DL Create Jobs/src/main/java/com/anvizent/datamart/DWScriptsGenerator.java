package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

import com.anvizent.datamart.DWScriptsGenerator.DWConfigScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWSourceInfoScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWTableInfoScriptsGenerator;
import com.anvizent.datamart.DWScriptsGenerator.DWValueScriptsGenerator;

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
        connectionId =  "41"; // AbcAnalysis_Mysql8
        selectTables =  "'Finished_Goods_BOM'"; // AbcAnalysis_Mysql8
        // connectionId =  "114"; // AbcAnalysis_Mysql8
        // selectTables =  "'SorMaster_Spark3'"; // AbcAnalysis_Mysql8

        tmpTableName = dlName + dlId + jobId;
        startTime = LocalDateTime.now();
        // startTimeString = getCurrentDateFormatted(startTime);
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

            // The DB scripts generator
        if (new DWDBScriptsGenerator().generateDBScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The config script generator
        if (new DWConfigScriptsGenerator().generateConfigScript() != Status.SUCCESS) {
            System.out.println("Create script generation failed. Stopping process.");
            return;
        }

        // The value script generator
        if (new DWValueScriptsGenerator().generateValueScript() != Status.SUCCESS) {
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
                    ilTableName = "SorMaster_Spark3";
                    connectionId = "114";
                    System.out.println("Connection_Id: " + connectionIdResult +
                            ", Table_Schema: " + tableSchema +
                            ", IL_Table_Name: " + ilTableName);
                     Boolean deleteFlag = getDeleteFlag(ilTableName);

                     List<Map<String, Object>> masterData = getMasterSourceMappingInfoData(conn, ilTableName, connectionId, querySchemaCondition);
                     System.out.println("Size of master data: " + masterData.size());
                     Map<String, String> data = getSymbolValueforConnectionId(conn, ilTableName);
                     System.out.println("Size of Symbol value data: " + data.size());
                     System.out.println("data: " + data);
                     
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


                }
            } catch (SQLException e) {
                // TODO return failure status
                e.printStackTrace();
            }

            return Status.SUCCESS;
        }

         /**
         * Executes a SQL query to fetch master data from `ELT_IL_Source_Mapping_Info_Saved`and returns the result as a list of maps.
         */
        public List<Map<String, Object>> getMasterSourceMappingInfoData(Connection connection, String ilTableName,
                String connectionId, String querySchemaCond) throws SQLException {
            
            // TODO It should be single value at max
            Map<String, String> data = getSymbolValueforConnectionId(conn, ilTableName);

            String query = "SELECT Connection_Id, Table_Schema, IL_Table_Name, IL_Column_Name, IL_Data_Type, " +
                    "Constraints, Source_Table_Name, Source_Column_Name, PK_Constraint, PK_Column_Name, " +
                    "FK_Constraint, FK_Column_Name " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved` " +
                    "WHERE (Constant_Insert_Column IS NULL OR Constant_Insert_Column != 'Y') " +
                    "AND Constraints NOT IN ('FK', 'SK') " +
                    "AND IL_Column_Name != 'Company_Id' " +
                    "AND IL_Table_Name = ? " +
                    "AND Connection_Id = ? " + querySchemaCond;

            List<Map<String, Object>> resultList = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, ilTableName);
                preparedStatement.setString(2, connectionId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Connection_Id", resultSet.getString("Connection_Id"));
                        row.put("Table_Schema", resultSet.getString("Table_Schema"));
                        row.put("IL_Table_Name", resultSet.getString("IL_Table_Name"));
                        row.put("IL_Column_Name", resultSet.getString("IL_Column_Name"));
                        row.put("IL_Data_Type", resultSet.getString("IL_Data_Type"));
                        row.put("Constraints", resultSet.getString("Constraints"));
                        row.put("Source_Table_Name", resultSet.getString("Source_Table_Name"));
                        row.put("Source_Column_Name", resultSet.getString("Source_Column_Name"));
                        row.put("PK_Constraint", resultSet.getString("PK_Constraint"));
                        row.put("PK_Column_Name", resultSet.getString("PK_Column_Name"));
                        row.put("FK_Constraint", resultSet.getString("FK_Constraint"));
                        row.put("FK_Column_Name", resultSet.getString("FK_Column_Name"));

                        resultList.add(row);
                    }
                }
            }

            return resultList;
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
    }
    
    public class DWConfigScriptsGenerator {

        // Constructor
        public DWConfigScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the configuration script
        public Status generateConfigScript() {
            // Placeholder logic for the method
            // Replace this with actual implementation
            return Status.SUCCESS;
        }
    }
    
    public class DWValueScriptsGenerator {

        // Constructor
        public DWValueScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the value script
        public Status generateValueScript() {
            // Placeholder logic for the method
            // Replace with actual implementation
            return Status.SUCCESS; // Return true for success, false for failure
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
