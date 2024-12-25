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

public class DWScriptsGenerator {

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
        connectionId =  "41";
        selectTables =  "'Finished_Goods_BOM'";

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

        // Constructor
        public DWDBScriptsGenerator() {
            // Initialization logic if needed
        }
    
        // Method to generate the DB script
        public Status generateDBScript() {
            // Placeholder logic for the method
            // Replace this with actual implementation
            List<Map<String, Object>> tableSchemaList =  getTableSchemasFromMapInfo(conn, selectTables, connectionId, querySchemaCondition);
                
            // Step 2 - iteration (ELT_Select_Query)
            for (Map<String, Object> schema : tableSchemaList) {
                String connectionIdResult = (String) schema.get("Connection_Id");
                String tableSchema = (String) schema.get("Table_Schema"); //  This is used
                String ilTableName = (String) schema.get("IL_Table_Name");
                System.out.println("Connection_Id: " + connectionIdResult +
                        ", Table_Schema: " + tableSchema +
                        ", IL_Table_Name: " + ilTableName);

                        

            }

            return Status.SUCCESS;
        }

        public List<Map<String, Object>> getTableSchemasFromMapInfo(Connection connection,
                String selectiveTables,
                String connectionId,
                String querySchemaCond) {
            // Construct the query dynamically
            String query = "SELECT DISTINCT " +
                    "`Connection_Id`, " +
                    "`Table_Schema`, " +
                    "`IL_Table_Name` " +
                    "FROM `ELT_IL_Source_Mapping_Info_Saved`" +
                    "WHERE `IL_Table_Name` IN (" + selectiveTables + ") " +
                    "AND `Connection_Id` = ? " + querySchemaCond;

            // Prepare and execute the statement
                    List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, connectionId); // Set the Connection_Id parameter

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    // Process the result set
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
                // Handle or rethrow exception as needed
            }
            return results;
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
