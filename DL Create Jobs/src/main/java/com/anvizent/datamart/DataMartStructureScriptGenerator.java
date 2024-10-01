package com.anvizent.datamart;

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
import java.util.stream.Collectors;

public class DataMartStructureScriptGenerator {

    private static final String COMPONENT_EMPTY = "empty";
    private static final String COMPONENT_EMPTY_RECOERCING = "empty_recoercing";
    private static final String COMPONENT_EMPTY_REMIT = "empty_remit";
    private static final String COMPONENT_EMPTY_RENAME = "empty_rename";
    private static final String COMPONENT_EXECUTESQL = "executesql";
    private static final String COMPONENT_EXECUTESQL_CLENSING = "executesql_clensing";
    private static final String COMPONENT_EXECUTESQL_SOURCE = "executesql_source";
    private static final String COMPONENT_EXECUTESQL_FILTERGROUPBY = "executesqlfiltergroupby";
    private static final String COMPONENT_EXPRESSION = "expression";
    private static final String COMPONENT_GROUPBY = "groupby";
    private static final String COMPONENT_JOIN = "join";
    private static final String COMPONENT_PARTITIONSOURCESQL_DL = "partitionsourcesql_dl";
    private static final String COMPONENT_SQLSINK = "sqlsink";
    private static final String COMPONENT_SOURCESQL = "sourcesql";
    
    private static final String END_OF_SCRIPT_TEXT = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"; // Newer Charset
    //private static final String END_OF_SCRIPT_TEXT = ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"; // Old and deprecated

    private long clientId;
    private DataSourceType dataSourceType;
    private long dlId;
    private long jobId;
    private String dlName;
    private String tmpTableName;
    private String startTimeString;


    SQLQueries sqlQueries;
    Connection conn;

    public DataMartStructureScriptGenerator(long clientId, DataSourceType type, long dlId, long jobId, String dlName) {
        this.clientId = clientId;
        this.dataSourceType = type;
        this.dlId = dlId;
        this.jobId = jobId;
        this.dlName = dlName;
        init();
    }

    private void init() {
        tmpTableName = dlName + dlId + jobId;
        startTimeString = getCurrentDateFormatted();
        sqlQueries = new SQLQueries(); // Creating instance of SQLQueries inner class //TODO
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
    public long getDlId() {
        return dlId;
    }
    public long getJobId() {
        return jobId;
    }
    public String getDlName() {
        return dlName;
    }   
    public String getTmpTableName() {
        return tmpTableName;
    }
    public String getTimeStamp() {
        return startTimeString;
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
        executeDropTableQuery(conn, getTmpTableName());
        System.exit(1);

    }

    public class DataMartConfigScriptGenerator {
        private static final String EMPTY_RETAIN_RENAME = "EmptyRetainRename";

        public DataMartConfigScriptGenerator() {
         }

        public Status generateConfigScript() {
            System.out.println("\nGenerating config script for DL_ID: " + dlId);
            boolean status = false;

            // Script Job 1 Source
            System.out.println("============================================");
            System.out.println("============================================");
            System.out.println("Script Component: " + "Source");
            String sourceComponent = ""; // TO name Case
            String previousComponent = ""; // Initialization
            try {
                // component = executesql_source ??
                // 1
                String settings = fetchLoadConfigsSettings(conn, dlId, jobId);
                System.out.println("settings: " + settings);
                String table = getTmpTableName();

                //String component = "'" + COMPONENT_PARTITIONSOURCESQL_DL + "'" + ", "+ "'" + COMPONENT_SOURCESQL + "'";// ('partitionsourcesql_dl','sourcesql') 
                String component = COMPONENT_PARTITIONSOURCESQL_DL + ", "+ COMPONENT_SOURCESQL;// ('partitionsourcesql_dl','sourcesql') 
                // TODO COMPONENT_SOURCESQL is not returning any value; Change it to new SQL 
                
                // 2
                //String tt = dlName + dlId + jobId; // TODO repalce with a single function
                //Map<String, Map<String, String>> tmpTableData = getTmpTableData(conn, tt, false); //`property` != 'db'

                List<Map<String, Object>> list = executeAndJoinTablesSource(conn, String.valueOf(dlId), String.valueOf(jobId));
                boolean tableCreated = createNewTable(conn, table);
                if (tableCreated) {
                    insertDataIntoTable(conn, table, list);
                }

                // 3
                Map<String, Map<String, String>> tableDataMap = getTmpTableData(conn, table, false); //`property` != 'db'

                // Iterate over the table names
                for (String tableName : tableDataMap.keySet()) {
                    System.out.println("Table Name: " + tableName);
                    String orignalTable = tableName; // TODO data from the Context input or surrounding loop
                    Map<String, String> config = getSpecialCharReplaceConfig(table, orignalTable); // Likely only one entry
                    insertTableDataIntoDB(conn, table, config);
                }
                
                // 4
                // TODO MULTIPLE components used
                // Data OK used in next component
                // TODO merge the two components values
                System.out.println(component);
                sourceComponent = fetchAndFormatProperties(conn, COMPONENT_PARTITIONSOURCESQL_DL);
                //System.out.println("sourceComponent: " +sourceComponent);

                // 5 table ELT_DL_Driving_Table_Info and ELT_DL_Lookup_Table_Info have data - two
                // data is good OK
                Map<String, Object> result = getSourceAndPreviousComponent(sourceComponent);
                previousComponent = (String) result.get("PreviousComponent");
                sourceComponent = (String) result.get("SourceComponent");

            //    System.out.println("previousComponent: " + previousComponent);
            //    System.out.println("sourceComponent: \n" + sourceComponent);
                
               // 6
               // Table ELT_Job_Properties_Info has data...
               // OK data coming
                String propsExecutesqlSource = fetchAndFormatProperties(conn, COMPONENT_EXECUTESQL_SOURCE);
                String executesqlSource = propsExecutesqlSource;
                System.out.println("6. executesqlSource: ");
                System.out.println(executesqlSource);

                // 7
                // NO data
                String tmpTable = getTmpTableName();
                Map<String, Map<String, String>> tmpTableDataMap = getTmpTableData(conn, tmpTable, false); // `property` != 'db'
                // TODO data processing Pending

            } catch (SQLException e) {
                e.printStackTrace();
            }

            System.out.println("Script Component: " + "Source");
            System.out.println("============================================");
            //#########################
            //#########################
            // Script Job 2 lkp/join
            // OK NOW
            System.out.println("============================================");
            System.out.println("Script Component: " + COMPONENT_JOIN);
            String componentJoin = "join";
            String propsJoin = fetchAndFormatProperties(conn, COMPONENT_JOIN);
            //String joinScript = propsJoin.replace("Dynamic_Join_Name", previousComponent);
            //previousComponent = "Join";

            // Both tables carry same name that is context.DL_Name+context.DL_Id+context.Job_Id
            // Trying to use simplified Join
            String table = dlName + dlId + jobId;
            // String finalQuery = SQLQueries.buildFullJoinQuery(table);   
            //String finalQuery = SQLQueries.buildFullJoinQuery(table, table); If two tables are joined separatly.

            String joinComponent = propsJoin; //Output
            //String previousJoinName = ""; //Output
            try {
                // TODO commented below for testing
                System.out.println("table: " + table);
                Map<String, String> retObjJoin = executeJoinQueryAndBuildJoinComponent(conn, table, dlId, jobId, joinComponent);
                if (retObjJoin.size() != 0) {
                    String previousJoinName = retObjJoin.get("PreviousJoinName");
                    String component = retObjJoin.get("JoinComponent");
                    if (previousJoinName != null) {
                        previousComponent = previousJoinName;
                    }
                    if (component != null) {
                        joinComponent = component;
                    } else {
                        joinComponent = " ";
                    }
                }
                //previousComponent = ; //TBD returned from above function
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //Output from previous funciton
            // PreviousJoinName
            // JoinComponent
            System.out.println("propsJoin: ");
            System.out.println(propsJoin);
            System.out.println("JoinComponent: ");
            System.out.println(joinComponent);
            System.out.println( "previousComponent: " + previousComponent);
            System.out.println("Script Component: " + COMPONENT_JOIN);
            System.out.println("============================================");
            //#########################
            //#########################
            // Script Job 3 Recoercing
            System.out.println("============================================");
            System.out.println("Script Component: " + COMPONENT_EMPTY_RECOERCING);
            String componentEmptyRecoercing = "empty_recoercing";
            String propsEmptyRecoercing = fetchAndFormatProperties(conn, COMPONENT_EMPTY_RECOERCING);
            String recoercingScript = propsEmptyRecoercing.replace("Dynamic_Join_Name", previousComponent);
            previousComponent = "Recoercing"; // Hardcoded
            System.out.println("propsEmptyRecoercing: ");
            System.out.println(propsEmptyRecoercing);
            System.out.println(recoercingScript);
            System.out.println( "previousComponent: " + previousComponent);
            System.out.println("Script Component: " + COMPONENT_EMPTY);
            System.out.println("============================================");

            //#########################
            //#########################
            // Script Job 4 NullReplacement/Empty
            System.out.println("============================================");
            System.out.println("Script Component: " + COMPONENT_EMPTY);
            String componentNullReplacement = "empty";
            String propsNullReplacement = fetchAndFormatProperties(conn, COMPONENT_EMPTY);
            String emptyScript = propsNullReplacement.replace("Dynamic_Join_Name", previousComponent); // Output //Effectively no impact of replace
            previousComponent = "Cleansing_Fields";
            System.out.println("propsNullReplacement: ");
            System.out.println(propsNullReplacement);
            System.out.println(emptyScript);
            System.out.println( "previousComponent: " + previousComponent);
            System.out.println("Script Component: " + COMPONENT_EMPTY);
            System.out.println("============================================");
            //#########################
            //#########################

            // SubJob 5 - GroupbyJoin
            String componentFilterGroupBy = "executesqlfiltergroupby";
            String filterGroupBy = fetchAndFormatProperties(conn, componentFilterGroupBy);
            String dynamicFilterGroupByName = filterGroupBy.replace("Dynamic_FilterGroupby_Name", "Join_Aggregation");
            String dynamicFilterGroupBySource = dynamicFilterGroupByName.replace("Dynamic_FilterGroupby_Source", previousComponent);
            String previousName = "Join_Aggregation";
            String joinDynamicGroupbyFilterConfig = dynamicFilterGroupBySource; //output
            System.out.println("Previous Name: " + previousName);
            System.out.println("Derived Dynamic Groupby Filter Config: \n" + joinDynamicGroupbyFilterConfig);

            System.out.println("filterGroupBy: ");
            System.out.println(filterGroupBy);
            System.out.println("Script Component: " + "Derived Sub Job");

            //#########################
            //#########################

            // Job 6 Derived
                    // Subjob "Child"
            String expressionComponent = "";
            try {
                List<Map<String, String>> results = getDerivedColumnInfoByJobAndDLId(conn, jobId, dlId);
                System.out.println();
                System.out.println(results.size());
                //System.exit(1);

                    // Iterating child Job
                    String finalDerivedValue = "";
                    StringBuilder finalExpressionComponentBuilder = new StringBuilder();

                    for (Map<String, String> row : results) {
                        String level = row.get("Level");

                        previousComponent = ""; // TBD
                        // Expression Type Java
                        final String componentExpression = "expression";
                        String expressionType = "JAVA";
                        // TBD: Below  function seems constant. Can be moved out of the loop. Verify?
                        String jobPropertiesJava = fetchAndFormatProperties(conn, componentExpression);
                        List<Map<String, Object>> result = fetchDerivedColumnInfoByLevel(conn, expressionType, level, jobId, dlId);
                        expressionComponent = processDerivedColumnInfoForTypeSQL(result, jobPropertiesJava);

                        // Expression_Data type SQL
                        final String componentExecuteSql = "executesql";
                        expressionType = "SQL";
                        String jobPropertiesEexecuteSql = fetchAndFormatProperties(conn, componentExecuteSql);
                        List<Map<String, Object>> result2 = fetchDerivedColumnInfoByLevel(conn, expressionType, level, jobId, dlId);
                        String executeSqlComponent = processDerivedColumnInfoForTypeSQL(result2, jobPropertiesEexecuteSql);

                        System.out.println(expressionComponent);
                        System.out.println(executeSqlComponent);

                        String derivedComponentConfig = expressionComponent + "\n" + executeSqlComponent;

                        System.out.println(derivedComponentConfig);


                        finalDerivedValue = derivedComponentConfig;

                        // TBD: rechech that Talend "Iteration" component does other than this 
                        // TBD: Set the Previous_Component; Need to see how it used
                        if (finalExpressionComponentBuilder.length() > 0) {
                            finalExpressionComponentBuilder.append("\n");
                        }
                        finalExpressionComponentBuilder.append(finalDerivedValue);

                        System.out.println("Final_Derived_Value: " + finalDerivedValue);
                    }

                    System.out.println("Final_Derived_Value: " + finalDerivedValue);
                    // Output of the "Derived" component
                    expressionComponent = finalExpressionComponentBuilder.toString();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            //#########################
            //#########################

            // SubJob 7: GroupbyDerived
            String previousComponent2 = ""; // TBD
            // Same maybe reused from job 5 GroupbyJoin
            //String componentFilterGroupBy = "executesqlfiltergroupby";
            //String filterGroupBy = fetchAndFormatProperties(conn, componentFilterGroupBy);
            String dynamicFilterGroupByName2 = filterGroupBy.replace("Dynamic_FilterGroupby_Name", "Derived_Aggregation");
            String dynamicFilterGroupBySource2 = dynamicFilterGroupByName2.replace("Dynamic_FilterGroupby_Source", previousComponent2);
            String previousName2 = "Derived_Aggregation";
            String derivedDynamicGroupbyFilterConfig = dynamicFilterGroupBySource2;
            System.out.println("Previous Name: " + previousName2);
            System.out.println("Derived Dynamic Groupby Filter Config: " + derivedDynamicGroupbyFilterConfig);
            
            //#########################
            //#########################
            // Job 8 remit
            String componentEmptyRemit = "empty_remit";
            printComponent(COMPONENT_EMPTY_REMIT, previousComponent);
            String propsEmptyRemit = fetchAndFormatProperties(conn, componentEmptyRemit);
            String emptyRemitComponent = propsEmptyRemit; // output of this componentp; input to next // "empty_Component"

            final String defaultJoinNameRemit = "Emit_UnWanted_Columns"; // Input
            // TBD: Cases Yes or No Derived Columns
            try {
                Map<String, String> retObjRemit = processDerivedColumnInfoInRemit(conn, jobId, dlId, defaultJoinNameRemit, emptyRemitComponent);
                if (retObjRemit.size() != 0) {
                    String emptyRetainRename = retObjRemit.get(EMPTY_RETAIN_RENAME);
                    String previousJoinNameRemit = retObjRemit.get("PreviousJoinName");
                    emptyRemitComponent = emptyRetainRename; // TODO check it is null impact on next statement. ditto for rename
                    // For Global Map
                    emptyRemitComponent = emptyRemitComponent.replace("Dynamic_Previous_Component", previousComponent); // What if it is null
                    // For SharedMap
                    if (emptyRetainRename == null) {
                        emptyRemitComponent = " ";
                    }
                    if (previousJoinNameRemit != null) {
                        previousComponent = previousJoinNameRemit; // output of this component // If null do not do anything "Previous_Component"
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
           
            //#########################
            //#########################
            // Job 9 Rename
            String componentEmptyRename = "empty_rename";
            printComponent(COMPONENT_EMPTY_RENAME, previousComponent);
            String propsEmptyRename = fetchAndFormatProperties(conn, componentEmptyRename);
            String EmptyRenameComponent = propsEmptyRename; /// output of this component

            final String defaultJoinNameRename = "Rename_Derived_Columns"; // Input
            String globalEmptyComponent = EmptyRenameComponent; // input  TODO where is it coming from "empty_Component" - from previous remit 
            //previousJoinName = ""; // TBD or NULL; see above
            // TBD: Below funciton/query seems not doing any actual processing. Seems redundent. Check!!
            try {
                Map<String, String> retObjRename = processDerivedColumnInfoInRename(conn, jobId, dlId, defaultJoinNameRename, EmptyRenameComponent);
                //String EmptyRenameScript = propsEmptyRename.replace("Dynamic_Previous_Component", previousComponent); // not done here rather previous step
                if (retObjRename.size() != 0) {
                    String emptyRetainRename = retObjRename.get(EMPTY_RETAIN_RENAME);
                    String previousJoinNameRename = retObjRename.get("PreviousJoinName");
                    EmptyRenameComponent = emptyRetainRename;
                    // For Global Map
                    EmptyRenameComponent = EmptyRenameComponent.replace("Dynamic_Previous_Component", previousComponent); // What if it is null
                    // For SharedMap
                    if (emptyRetainRename == null) {
                        EmptyRenameComponent = " "; // Sharedmap "Empty_Rename"
                    }
                    if (previousJoinNameRename != null) {
                        previousComponent = previousJoinNameRename; // output of this component // If null do not do anything "Previous_Component"
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //#########################
            //#########################
            // Job 10 Sink
            String componentSink = "sqlsink";
            printComponent(COMPONENT_SQLSINK, previousComponent);
            String propsSink = fetchAndFormatProperties(conn, componentSink);
            String sinkScript = propsSink.replace("Dynamic_Sink_Source", previousComponent);
            String sinkComponent = sinkScript;
            printScript(COMPONENT_SQLSINK, sinkComponent);
            //######################################

            //Output: Config.properties
            StringBuffer output = new StringBuffer();
            if (sourceComponent != "") { // 1
                output.append(sourceComponent);
            }
            if (joinComponent != "") { // 2
                output.append(joinComponent);
            }
            if (recoercingScript != "") { // 3
                output.append(recoercingScript);
            }
            if (emptyScript != "") { // 4
                output.append(emptyScript);
            }
            if (joinDynamicGroupbyFilterConfig != "") { // 5
                output.append(joinDynamicGroupbyFilterConfig);
            }
            if (expressionComponent != "") { // 6
                output.append(expressionComponent);
            }
            if (derivedDynamicGroupbyFilterConfig != "") { // 7
                output.append(derivedDynamicGroupbyFilterConfig);
            }
            if (emptyRemitComponent != "") { // 8
                output.append(emptyRemitComponent);
            }
            if (EmptyRenameComponent != "") { // 9
                output.append(EmptyRenameComponent);
            }
            if (sinkComponent != "") { // 10
                output.append(sinkComponent);
            }

            //System.out.println("Output: Config.properties ######################## \n\n" + output.toString());
            System.out.println("#################");
            String configFileName = getConfigFileName();
            String configScript = output.toString();
            Map<String, String> rowDetails = selectActiveEltDlTableInfo(conn, dlId);
            if (rowDetails == null) {
                System.out.println("No record found with DL_Id: " + dlId);
                return Status.FAILURE;
            }
            rowDetails.put("Job_Id", String.valueOf(jobId));
            rowDetails.put("config_file_name", configFileName);
            rowDetails.remove("DL_Version");
            deleteFromEltDlConfigProperties(conn, dlId, jobId);
            status = insertIntoEltDlConfigProperties(conn, rowDetails);

            writeToFile(configScript, configFileName);

            return status ? Status.SUCCESS : Status.FAILURE;
        }

        private String getConfigFileName() {
            String suffix = getTimeStamp();
            String configFileName = clientId + dlName + "_Config_File_" + suffix + ".config.properties";
            System.out.println(configFileName);
            return configFileName;
        }

        // TODO sourceComponent is an input
        private Map<String, Object> getSourceAndPreviousComponent(String sourceComponent) throws SQLException {
            String query = SQLQueries.SELECT_TABLE_NAME_FROM_DRIVING_LOOKUP_TABLE;
            String previousComponent = "";
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setLong(1, jobId);
                pstmt.setLong(2, dlId);
                pstmt.setLong(3, jobId);
                pstmt.setLong(4, dlId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder finalQueryBuilder = new StringBuilder();
                    while (rs.next()) {
                        String tableName = rs.getString("Table_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                       //System.out.println("                " + tableNameAlias);
                        //String sourceComponent = (String) globalMap.get("Source_Component"); // Input
                        tableNameAlias = tableNameAlias.replace(" ", "_");
                        String dynamicName = sourceComponent.replace("Dynamic_Table_Name", tableNameAlias.replace("$", "\\$"));
                        String dynamicSourceSqlQuery = dynamicName.replace("Dynamic_SourceSql_Query", "\\${" + tableNameAlias.replace("$", "\\$") + ".sourcesql.query}");
                        String dynamicTableName = dynamicSourceSqlQuery.replace("TableName", tableNameAlias.replace("$", "\\$"));
                        String dynamicPartitionSqlQuery = dynamicTableName.replace("Dynamic_PartitionSql_Query", "\\${" + tableNameAlias.replace("$", "\\$") + ".partitionsql.query}");
                        
                        //String finalQuery = (finalQuery == null) ? dynamicPartitionSqlQuery : finalQuery + "\n" + dynamicPartitionSqlQuery + "\n";
                        if (finalQueryBuilder.length() > 0) {
                            finalQueryBuilder.append("\n").append(dynamicPartitionSqlQuery).append("\n");
                        } else {
                            finalQueryBuilder.append(dynamicPartitionSqlQuery);
                        }
                        previousComponent = tableNameAlias;
                    }
                    sourceComponent = finalQueryBuilder.toString();
                    // previousComponent = last tableNameAlias no aggregation
                }
            }
            //System.out.println(previousComponent);
            //System.out.println(sourceComponent);
            Map<String, Object> returnValue = new HashMap<String, Object>();
            returnValue.put("SourceComponent", sourceComponent);
            returnValue.put("PreviousComponent", previousComponent);

            return returnValue;
        }

        public String fetchAndFormatProperties(Connection conn, String component) {
            //String query = SQLQueries.SELECT_JOB_PROPERTIES_INFO_WITH_COMPONENT; // op '=' (Single component)
            String query = SQLQueries.SELECT_ELT_JOB_PROPERTIES_INFO; // IN clause: Though Handles Multiple Component,
                                                                    // due to stmt.setString(),  works good with one Comp only
            StringBuilder script = new StringBuilder();
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, component);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    String keyName = rs.getString("Key_Name");
                    String valueName = rs.getString("Value_Name");
                    script.append(keyName)
                                 .append("=")
                                 .append(valueName)
                                 .append("\n");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return "";
            }
            return script.toString();
        }
    
        public boolean createNewTable(Connection conn, String tableName) throws SQLException {
            // Drop the table if it exists and create a new one
            String deleteTableSql = "DROP TABLE IF EXISTS " + tableName + "; ";
            String createTableSql = "CREATE TABLE " + tableName + " (" +
                         "id INT NOT NULL AUTO_INCREMENT, " +
                         "table_name VARCHAR(255) NOT NULL, " +
                         "Final_Table_Name VARCHAR(255) NOT NULL, " +
                         "property VARCHAR(100), " +
                         "PRIMARY KEY (id));";
            
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(deleteTableSql);
                stmt.executeUpdate(createTableSql);

                System.out.println("The Table " + tableName + " has been successfully created.");
                return true;
            } catch (SQLException e) {
                throw new SQLException("Error in creating the table: " + e.getMessage(), e);
            }
        }

        public void insertDataIntoTable(Connection conn, String tableName, List<Map<String, Object>> list) throws SQLException {
            String insertSQL = "INSERT INTO " + tableName + " (table_name, Final_Table_Name, property) VALUES (?, ?, ?)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                for (Map<String, Object> row : list) {
                    String table_name = (String) row.get("table_name");
                    String final_table_name = (String) row.get("Final_Table_Name");
                    String property = (String) row.get("property");
    
                    pstmt.setString(1, table_name);
                    pstmt.setString(2, final_table_name);
                    pstmt.setString(3, property);
                    System.out.println(table_name + " " + final_table_name + " " + property);
                    pstmt.executeUpdate();
                }
                System.out.println("Data has been inserted successfully into table: " + tableName);
            } catch (SQLException e) {
                throw new SQLException("Error inserting data into table: " + e.getMessage(), e);
            }
        }

        Map<String, String> getSpecialCharReplaceConfig(String tmpTable, String origTableName) throws SQLException {
            String query = "SELECT " +
               dlId + " as `DL_Id`, " +
               jobId + " as `Job_Id`, " +
               tmpTable + ".`table_name`, " +
               tmpTable + ".`property` " +
               "FROM " + tmpTable + " " +
               "WHERE property != 'db' " +
               "AND table_name = '" + origTableName + "'";

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {

                Map<String, Map<String, String>> appProps = getELTAppProperties(conn);
                String finalSpclReplaceName = null;
                Map<String, String> returnedMap = new HashMap<>();
                while (rs.next()) {
                    String dlIdResult = rs.getString("DL_Id");
                    String jobIdResult = rs.getString("Job_Id");
                    String tableName = rs.getString("table_name");
                    String property = rs.getString("property");

                    String key = dlIdResult + "-" + jobIdResult;

  
                    Map<String, String> props = appProps.getOrDefault(key, new HashMap<>());
                    String characterValue = props.get("CharacterValue");
                    String uiReplaceable = props.get("UIReplaceable");

                    String finalTableName = (finalSpclReplaceName == null)
                            ? tableName.replaceAll(characterValue, uiReplaceable)
                            : finalSpclReplaceName.replaceAll(characterValue, uiReplaceable);

                    finalSpclReplaceName = finalTableName;

                    System.out.println("characterValue: " + characterValue);
                    System.out.println("uiReplaceable: " + uiReplaceable);

                    System.out.println("table_name: " + tableName);
                    System.out.println("finalTableName: " + finalTableName);
                    returnedMap.put(tableName, finalTableName);
                }
                return returnedMap;
            }
        }

        public void insertTableDataIntoDB(Connection conn, String table, Map<String, String> config) throws SQLException {
            // TODO what should be the default Property value???
            String insertQuery = "INSERT INTO " + table + " (`table_name`, `Final_Table_Name`, `property`) VALUES (?, ?, 'db')";
    
            try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
                for (Map.Entry<String, String> entry : config.entrySet()) { // TODO likely only one entry
                    String tableName = entry.getKey();
                    String finalTableName = entry.getValue();
    
                    pstmt.setString(1, tableName);
                    pstmt.setString(2, finalTableName);
    
                    pstmt.executeUpdate(); // TODO batchUpdate
                }
            } catch (SQLException e) {
                throw new SQLException("Error executing INSERT query: " + e.getMessage(), e);
            }
        }

        public String fetchLoadConfigsSettings(Connection conn, long dlId, long jobId) throws SQLException {
            String query = "SELECT `ELT_DL_Load_Configs`.`Settings` FROM `ELT_DL_Load_Configs` WHERE DL_Id = ? AND Job_Id = ?";
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setLong(1, dlId);
                ps.setLong(2, jobId);
                // TBD: One result only?
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("Settings");
                    } else {
                        return null;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SQLException("Error while fetching load config settings", e);
            }
        }


        public Map<String, Map<String, String>> getELTAppProperties(Connection conn) throws SQLException {
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            String query = SQLQueries.buildAppPropertiesQuery(String.valueOf(dlId), String.valueOf(jobId));
    
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    String dlId = rs.getString("DL_Id");
                    String jobId = rs.getString("Job_Id");
                    String characterValue = rs.getString("CharacterValue");
                    String uiReplaceable = rs.getString("Id");
    
                    characterValue = processCharacterValue(characterValue);
    
                    String key = dlId + "-" + jobId;
                    Map<String, String> value = new HashMap<>();
                    value.put("CharacterValue", characterValue);
                    value.put("UiReplaceable", uiReplaceable);
                    
                    resultMap.put(key, value);
                }
            }
            return resultMap;
        }
    
        // Helper function to process CharacterValue
        private String processCharacterValue(String characterValue) {
            if (characterValue.equals("*") || characterValue.equals("(") || characterValue.equals(")") ||
                characterValue.equals("+") || characterValue.equals(";") || characterValue.equals(".") ||
                characterValue.equals("[") || characterValue.equals("]") || characterValue.equals("{") ||
                characterValue.equals("}") || characterValue.equals("?") || characterValue.equals(" ")) {
                return "\\" + characterValue;
            } else {
                return characterValue;
            }
        }

        // Function to execute both queries and perform an inner join using a hash map for efficiency
        public List<Map<String, Object>> executeAndJoinTablesSource(Connection conn, String dlId, String jobId)
                throws SQLException {
            //Fetch Filter Group By Info and store in a hash map
            Map<String, Map<String, Object>> filterGroupByInfoMap = fetchFilterGroupByInfoMap(conn, dlId, jobId);
            //Fetch Driving and Lookup Table Info and perform the INNER join
            return joinWithDrivingAndLookupTableInfo(conn, dlId, jobId, filterGroupByInfoMap);
        }

        // Function to fetch SELECT_ELT_DL_FILTER_GROUP_BY_INFO and store in a hash map for fast lookups
        private Map<String, Map<String, Object>> fetchFilterGroupByInfoMap(Connection conn, String dlId, String jobId)
                throws SQLException {
            String query = SQLQueries.SELECT_ELT_DL_FILTER_GROUP_BY_INFO;
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, dlId);
                ps.setString(2, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    Map<String, Map<String, Object>> resultMap = new HashMap<>();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("Job_Id", rs.getString("Job_Id"));
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));
                        row.put("Settings_Position", rs.getString("Settings_Position"));

                        String key = createJoinKey(
                                rs.getString("DL_Id"),
                                rs.getString("Job_Id"),
                                rs.getString("Table_Name"),
                                rs.getString("Table_Name_Alias"),
                                rs.getString("Settings_Position"));

                        resultMap.put(key, row);
                    }
                    return resultMap;
                }
            }
        }

        
        // Function to join with SELECT_ELT_DL_DRIVING_AND_LOOKUP_TABLE_INFO using the
        // hash map
        private List<Map<String, Object>> joinWithDrivingAndLookupTableInfo(Connection conn, String dlId, String jobId,
                Map<String, Map<String, Object>> filterGroupByInfoMap) throws SQLException {
            String query = SQLQueries.SELECT_ELT_DL_DRIVING_AND_LOOKUP_TABLE_INFO;

            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, dlId);
                ps.setString(2, jobId);
                ps.setString(3, dlId);
                ps.setString(4, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String, Object>> joinedResults = new ArrayList<>();
                    while (rs.next()) {
                        String key = createJoinKey(
                                rs.getString("DL_Id"),
                                rs.getString("Job_Id"),
                                rs.getString("Table_Name"),
                                rs.getString("Table_Name_Alias"),
                                rs.getString("Settings_Position"));

                        if (filterGroupByInfoMap.containsKey(key)) {
                            Map<String, Object> filterRow = filterGroupByInfoMap.get(key);
                            Map<String, Object> drivingRow = new HashMap<>();
                            drivingRow.put("DL_Id", rs.getString("DL_Id"));
                            drivingRow.put("Job_Id", rs.getString("Job_Id"));
                            drivingRow.put("Table_Name", rs.getString("Table_Name"));
                            drivingRow.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));
                            //drivingRow.put("Settings_Position", rs.getString("Settings_Position"));

                            // Map<String, Object> joinedRow = new HashMap<>(filterRow);
                            // joinedRow.putAll(drivingRow);
                            // Common key data. Hence, taken from either map
                            joinedResults.add(drivingRow);
                        }
                    }
                    return joinedResults;
                }
            }
        }

        private String createJoinKey(String dlId, String jobId, String tableName, String tableAlias, String settingPositions) {
            return dlId + "_" + jobId + "_" + tableName + "_" + tableAlias + "_" + settingPositions;
        }

        // Job source - Alternate approach
        public void executeJoinedQuery(Connection conn, String dlId, String jobId) throws SQLException {
            String query = SQLQueries.SELECT_ELT_JOINED_FILTER_GROUP_BY_DRIVING_AND_LOOKUP;
            
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, dlId); // For the Filter Group By Info and Driving Table
                ps.setString(2, jobId); // For the Filter Group By Info and Driving Table
                ps.setString(3, dlId); // For the Lookup Table
                ps.setString(4, jobId); // For the Lookup Table
                ps.setString(5, dlId); // For the Filter Group By Info again
                ps.setString(6, jobId); // For the Filter Group By Info again
                
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // Process the result set
                        String dlIdResult = rs.getString("DL_Id");
                        String jobIdResult = rs.getString("Job_Id");
                        String tableName = rs.getString("Table_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String settingsPosition = rs.getString("Settings_Position");
    
                        // Driving/Lookup table details
                        String drivingLookupTableName = rs.getString("Driving_Lookup_Table_Name");
                        String drivingLookupTableAlias = rs.getString("Driving_Lookup_Table_Alias");
                        String drivingLookupSettingsPosition = rs.getString("Driving_Lookup_Settings_Position");
    
                        // Further processing...
                    }
                }
            }
        }

        public List<Map<String, String>> getDerivedColumnInfoByJobAndDLId(Connection conn, long jobId, long dlId) {
            List<Map<String, String>> results = new ArrayList<>();

            String query = SQLQueries.SELECT_ELT_DL_DERIVED_COLUMN_INFO_BY_JOB_AND_DL_ID;
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setLong(1, jobId);
                stmt.setLong(2, dlId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> row = new HashMap<>();
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("Job_Id", rs.getString("Job_Id"));
                        row.put("Level", rs.getString("Level"));
                        results.add(row);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return results;
        }

        public Map<String, String> processDerivedColumnInfoInRename(Connection conn, long jobId, long dlId, final String defaultJoinName, String globalEmptyComponent) throws SQLException {
            String query = "SELECT DISTINCT DL_Id FROM ELT_DL_Derived_Column_Info WHERE Job_Id = ? AND DL_Id = ?";
            String previousJoinName = null;
            String emptyRetainRename = null;
            Map<String, String> retValue = new HashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setLong(1, jobId);
                ps.setLong(2, dlId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String dlIdResult = rs.getString("DL_Id");
                        if (dlIdResult == null) {
                            previousJoinName = null;
                            emptyRetainRename = null;
                        } else {
                            previousJoinName = defaultJoinName;
                            emptyRetainRename = globalEmptyComponent;
                        }
                        System.out.println("DL_Id: " + dlIdResult);
                        System.out.println("        Previous_Join_Name: " + previousJoinName); // TODO
                        System.out.println("        Empty Retain Rename: " + emptyRetainRename);
                        retValue.put("PreviousJoinName", previousJoinName);
                        retValue.put(EMPTY_RETAIN_RENAME, emptyRetainRename);
                    }
                    System.out.println("No Record Found: processDerivedColumnInfoInRename");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SQLException("Error while processing derived column info", e);
            }
            return retValue;
        }
        
        public Map<String, String> processDerivedColumnInfoInRemit(Connection conn, long jobId, long dlId, final String defaultJoinName, String emptyComponent) throws SQLException {
            String query = "SELECT DISTINCT DL_Id FROM ELT_DL_Derived_Column_Info WHERE Job_Id = ? AND DL_Id = ? and Column_Alias_Name = ''";
            String previousJoinName = null;
            String emptyRetainRename = null;
            Map<String, String> retValue = new HashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setLong(1, jobId);
                ps.setLong(2, dlId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String dlIdResult = rs.getString("DL_Id");
                        if (dlIdResult == null) {
                            previousJoinName = null;
                            emptyRetainRename = null;
                        } else {
                            previousJoinName = defaultJoinName;
                            emptyRetainRename = emptyComponent;
                        }
                        System.out.println("DL_Id: " + dlIdResult);
                        System.out.println("    inside Previous_Join_Name: " + previousJoinName);
                        System.out.println("    inside Empty Retain Rename: " + emptyRetainRename);
                        retValue.put("PreviousJoinName", previousJoinName);
                        retValue.put(EMPTY_RETAIN_RENAME, emptyRetainRename);
                    }
                    System.out.println("No Record Found: processDerivedColumnInfoInRemit");
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SQLException("Error while processing derived column info", e);
            }
            return retValue;
        }

        public Map<String, String> executeJoinQueryAndBuildJoinComponent(Connection conn, String table, long dlId, long jobId, String joinComponent)
                throws SQLException {
            String query = SQLQueries.buildConfigJoinQuery(table, table);
            Map<String, String> retValue = new HashMap<>();

            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setLong(1, jobId);
                ps.setLong(2, dlId);

                try (ResultSet rs = ps.executeQuery()) {

                   // String joinComponent = ""; // Value used in all iterations
                    String previousJoinName = null; // Value used in all iterations
                    StringBuilder finalDynamicJoinSources = new StringBuilder();

                    while (rs.next()) {
                        //String tableName = rs.getString("Table_Name");
                        //String joinTableName = rs.getString("Join_Table"); // TODO: Recheck the name
                        //String joinTableName = rs.getString("Join_Table_Name"); // TODO: Recheck the name
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String joinTableAlias = rs.getString("Join_Table_Alias");
                        //String joinName = rs.getString("Join_Name");

                        String tableFinalTableName = rs.getString("Table_Final_Table_Name");
                        String tableProperty = rs.getString("Table_Property");
                        String JoinFinalTableName = rs.getString("Join_Final_Table_Name");
                        String joinProperty = rs.getString("Join_Property");

                        // Process aliases
                        // tableNameAlias = tableNameAlias.replace(" ", "_");
                        // joinTableAlias = joinTableAlias.replace(" ", "_");
                        String tableName = (tableProperty == null) ? tableNameAlias : tableFinalTableName + "_ExecuteSql";
                        String joinTableName = (joinProperty == null) ? joinTableAlias : JoinFinalTableName + "_ExecuteSql";
                        String JoinName = tableName + "_" + joinTableAlias;

                        // As per Sequence after above
                        tableNameAlias = tableNameAlias.replace(" ", "_");
                        joinTableAlias = joinTableAlias.replace(" ", "_");

                        // String currentJoinName = tableName + joinTableAlias;
                        tableName = tableName.replace("\\$", "\\\\$");
                        joinTableName = joinTableName.replace("\\$", "\\\\$");

                        String joinSourceTables = previousJoinName == null ? (tableName + "," + joinTableName)
                                : (previousJoinName + "," + joinTableName);
                        joinSourceTables = joinSourceTables.replace("\\$", "\\\\$"); // TBD: redundant? to be removed
                                                                                     // done above in parts
                        //joinComponent = finalDynamicJoinSources.toString(); // from previous iteration
                        String dynamicJoinName = joinComponent.replace("Dynamic_Join_Name",
                                tableName + "_" + joinTableName);
                        String dynamicJoinSources = dynamicJoinName.replace("Dynamic_Join_Sources", joinSourceTables);

                        // Append to final dynamic join sources
                        if (finalDynamicJoinSources.length() > 0) {
                            finalDynamicJoinSources.append("\n");
                        }
                        finalDynamicJoinSources.append(dynamicJoinSources);
                        // Below to be used in next iteration
                        previousJoinName = JoinName; // from Processing
                    }
                    // TODO If there is no data, do not make any change
                    // Print or use finalDynamicJoinSources and previousJoinName. They are end
                    // Results.
                    joinComponent = finalDynamicJoinSources.toString();
                    System.out.println("Join Component: " + joinComponent);
                    System.out.println("Join Name: " + previousJoinName);
                    retValue.put("PreviousJoinName", previousJoinName);
                    retValue.put("JoinComponent", joinComponent);

                    return retValue;
                }
            }
        }
    
        // Helper method to sanitize and replace spaces with underscores (like StringHandling.EREPLACE in Talend)
        private String sanitizeAlias(String alias) {
            return alias != null ? alias.replace(" ", "_") : null;
        }

        private String sanitizeReplace(String input, String target, String replacement) {
            return input.replace(target, replacement);
        }

        // Child of Derived - component 2
        // public List<Map<String, Object>> fetchDerivedColumnInfoByLevel(Connection conn, String expression, String expressionType, String level, String jobId, String dlId) throws SQLException {
        //     String query = SQLQueries.SELECT_ELT_DL_DERIVED_COLUMN_INFO;
            
        //     try (PreparedStatement ps = conn.prepareStatement(query)) {
        //         ps.setString(1, expressionType);
        //         ps.setString(2, level);
        //         ps.setString(3, jobId);
        //         ps.setString(4, dlId);
                
        //         try (ResultSet rs = ps.executeQuery()) {
        //             List<Map<String, Object>> results = new ArrayList<>();
        //             StringBuilder finalExpressionBuilder = new StringBuilder();
        //             String Last_Component_Source = "";
        //             while (rs.next()) {
        //                 Map<String, Object> row = new HashMap<>();
        //                 row.put("DL_Id", rs.getString("DL_Id"));
        //                 row.put("Job_Id", rs.getString("Job_Id"));
        //                 row.put("Level", rs.getString("Level"));
        //                 String previousComponent = ""; // TBD from inputs or otherwise
        //                 final String expressionLevel = "Expression_" + rs.getString("Level");
        //                 Last_Component_Source = "Expression_" + rs.getString("Level"); // TBD: Use one of them this, previous
        //                 String expressionName = expression.replace("Dynamic_Expression_Name", expressionLevel);
        //                 String expressionSource = expressionName.replace("Dynamic_Expression_Source", previousComponent);
                        
        //             // Block to make a final expression
        //                 String lookupColumnName = rs.getString("lookup_column_names");

        //                 // One of them to be prferred
        //                 //Concise code
        //                 if (finalExpressionBuilder.length() > 0) {
        //                     finalExpressionBuilder.append("\n");
        //                 }
        //                 finalExpressionBuilder.append(expressionSource);

        //                 // // Explicit code
        //                 // if (finalExpressionBuilder.length() > 0) {
        //                 //     finalExpressionBuilder.append("\n").append(expressionSource);
        //                 // } else {
        //                 //     finalExpressionBuilder.append(expressionSource);
        //                 // }

        //                 results.add(row);
        //             }

        //             System.out.println("finalExpression      : " + finalExpressionBuilder.toString());
        //             System.out.println("Last_Component_Source: " + Last_Component_Source);

        //             return results;
        //         }
        //     }
        // }

        public List<Map<String, Object>> fetchDerivedColumnInfoByLevel(Connection conn, String expressionType, String level, long jobId, long dlId) throws SQLException {
            String query = SQLQueries.SELECT_ELT_DL_DERIVED_COLUMN_INFO;
            
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, expressionType);
                ps.setString(2, level);
                ps.setLong(3, jobId);
                ps.setLong(4, dlId);
                
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String, Object>> results = new ArrayList<>();
                    // StringBuilder finalExpressionBuilder = new StringBuilder();
                    // String Last_Component_Source = "";
        
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("Job_Id", rs.getString("Job_Id"));
                        row.put("Level", rs.getString("Level"));

                        //String expressionSource = buildExpressionSource(rs, expression, finalExpressionBuilder);
                        // Last_Component_Source = getLastComponentSource(rs);
                        //finalExpressionBuilder = buildFinalExpression(finalExpressionBuilder, expressionSource);
        
                        results.add(row);
                    }
        
                    //logFinalExpression(finalExpressionBuilder, Last_Component_Source);
        
                    return results;
                }
            }
        }

        public void processDerivedColumnInfoForTypeJava(List<Map<String, Object>> results, String expression) {
            // Loop over the results list
            StringBuilder finalExpressionBuilder = new StringBuilder();
            String lastComponentSource = "";
            for (Map<String, Object> row : results) {
                String dlId = (String) row.get("DL_Id");
                String jobId = (String) row.get("Job_Id");
                String level = (String) row.get("Level");
                
                String previousComponent = ""; // TBD from inputs or otherwise
                final String expressionLevel = "Expression_" + level;
                lastComponentSource = "Expression_" + level; // TBD: Use one of them this, previous
                String expressionName = expression.replace("Dynamic_Expression_Name", expressionLevel);
                String expressionSource = expressionName.replace("Dynamic_Expression_Source", previousComponent);
        
                // One of them to be prferred
                // Concise code
                if (finalExpressionBuilder.length() > 0) {
                    finalExpressionBuilder.append("\n");
                }
                finalExpressionBuilder.append(expressionSource);

                // // Explicit code
                // if (finalExpressionBuilder.length() > 0) {
                //     finalExpressionBuilder.append("\n").append(expressionSource);
                // } else {
                //     finalExpressionBuilder.append(expressionSource);
                // }
            }

            System.out.println("Final Expression     : " + finalExpressionBuilder.toString());
            System.out.println("Last_Component_Source: " + lastComponentSource);

        }

        public String processDerivedColumnInfoForTypeSQL(List<Map<String, Object>> results, String expression) {
            // Loop over the results list
            StringBuilder finalExpressionBuilder = new StringBuilder();
            String lastComponentSource = "";
            for (Map<String, Object> row : results) {
                String dlId = (String) row.get("DL_Id");
                String jobId = (String) row.get("Job_Id");
                String level = (String) row.get("Level");
                
                String previousComponent = ""; // TBD from inputs or otherwise
                final String expressionLevel = "ExecuteSql_" + level;
                lastComponentSource = "ExecuteSql_" + level; // TBD: Use one of them this, previous
                String expressionName = expression.replace("Dynamic_Name", expressionLevel);
                String expressionSource = expressionName.replace("Dynamic_Source", previousComponent);

                if (finalExpressionBuilder.length() > 0) {
                    finalExpressionBuilder.append("\n");
                }
                finalExpressionBuilder.append(expressionSource);
            }

            System.out.println("Final Expression     : " + finalExpressionBuilder.toString());
            System.out.println("Last_Component_Source: " + lastComponentSource);

            return finalExpressionBuilder.toString();
        }

        private boolean insertIntoEltDlConfigProperties(Connection conn, Map<String, String> rowDetails) {
            String insertSql = "INSERT INTO ELT_DL_CONFIG_PROPERTIES (DL_Id, Job_Id, DL_Name, DL_Table_Name, config_file_name, Active_Flag) " +
                               "VALUES (?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                insertPs.setString(1, rowDetails.get("DL_Id"));
                insertPs.setString(2, rowDetails.get("Job_Id"));
                insertPs.setString(3, rowDetails.get("DL_Name"));
                insertPs.setString(4, rowDetails.get("DL_Table_Name"));
                insertPs.setString(5, rowDetails.get("config_file_name"));
                insertPs.setBoolean(6, rowDetails.get("DL_Active_Flag").equals("1"));
                System.out.println("DL_Active_Flag value : " + rowDetails.get("DL_Active_Flag"));

                int rowsAffected = insertPs.executeUpdate();
                return rowsAffected > 0;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public class DataMartValueScriptGenerator {

        public DataMartValueScriptGenerator() {
         }

        public Status generateValueScript() {
            System.out.println("\nGenerating value script for DL_ID: " + dlId);
            boolean status = false;

            // Job 1 Source
            String componentSource1 = COMPONENT_PARTITIONSOURCESQL_DL;
            String componentSource2 = COMPONENT_SOURCESQL;
            String finalSource = componentSourceValue(componentSource1, componentSource2); // Output

            // Job 2 SourceExecutesql
            String componentExecuteSqlSource = COMPONENT_EXECUTESQL_SOURCE;
            String finalSourceExecuteSql = componentSourceExecuteSqlValue(componentExecuteSqlSource); // Output

            // Job 3 lkp/join
            String componentJoin = "join";
            String joinValue = componentJoinValue(componentJoin); // Output

            // Job 4 Recoercing
            String componentEmptyRecoercing = "empty_recoercing";
            String RecoercingValue = componentRecoercing(componentEmptyRecoercing); // Output
            //String propsEmptyRecoercing = fetchAndFormatProperties(conn, componentEmptyRecoercing);

            // Job 5 NullReplacement
            String tgtPwd = ""; // TBD: TODO use input, replace literal $
            tgtPwd = tgtPwd.replace("$", "\\$");
            String componentNullReplacement = "empty";
            String emptyValue = componentNullReplacementValue(componentNullReplacement); 

            // Job 6 FilterValue
            String componentFilterGroupBy = "executesqlfiltergroupby";
            Map<String, String> values = componentFilterGroupBy(componentEmptyRecoercing);
            String joinFilterGroupbyValue = values.get("JoinFilterGroupby");
            String derivedFilterGroupbyValue = values.get("DerivedFilterGroupby");


            // Job 7 Expression
            String componentExpression = "expression";
            String expressionType = "JAVA";
            List<String> levels = getDistinctLevels(conn, jobId, dlId); // TODO name of output
            String expressionValue = executeExpressionChildComponentValue(conn, levels, jobId, dlId);

            // Job 8 Sql_Expression
            String componentSQLExpression = "executesql";
            String executeSqlValue = componentSQLExpressionValue(componentSQLExpression);

            // Job 9 remit
            String componentRemit = "empty_remit";
            String mappingRemitValue = componentRemitValue(componentRemit);

            // Job 10 Rename
            String mappingRenameValue = componentRenameValue(COMPONENT_EMPTY_RENAME);

            // Job 11 Sink
            String componentSink = "sqlsink";
            String sinkValue = componentSinkValue(componentSink);
            System.out.println(sinkValue);

            //Output: Values.properties
            StringBuffer output = new StringBuffer();
            if (finalSource != "") { // 1
                output.append(finalSource);
            }
            if (finalSourceExecuteSql != "") { // 2
                output.append("\n").append(finalSourceExecuteSql);
            }
            if (joinValue != "") { // 3
                output.append("\n").append(joinValue);
            }
            if (RecoercingValue != "") { // 4
                output.append("\n").append("\n").append(RecoercingValue);
            }
            if (emptyValue != "") { // 5
                output.append("\n").append(emptyValue);
            }
            if (joinFilterGroupbyValue != "") { // 5
                output.append("\n").append(joinFilterGroupbyValue);
            }
            if (expressionValue != "") { // 6
                output.append("\n").append(expressionValue);
            }
            if (executeSqlValue != "") { // 7
                output.append("\n").append(executeSqlValue);
            }
            if (derivedFilterGroupbyValue != "") { // 8
                output.append("\n").append(derivedFilterGroupbyValue);
            }
            // TODO: Seems Not in Use
            // if (DerivedValueGenerated != "") { // 7
            //     output.append("\n").append(DerivedValueGenerated);
            // }
            if (mappingRemitValue != "") { // 9
                output.append("\n").append(mappingRemitValue);
            }
            if (mappingRenameValue != "") { // 10
                output.append("\n").append(mappingRenameValue);
            }
            if (sinkValue != "") { // 11
                output.append("\n").append(sinkValue);
            }
            System.out.println("Output: Values.properties ######################## \n\n" + output.toString());

            String valueFileName = getValueFileName();
            String script = output.toString();
            Map<String, String> rowDetails = selectActiveEltDlTableInfo(conn, dlId);
            if (rowDetails == null) {
                System.out.println("No record found with DL_Id: " + dlId);
                return Status.FAILURE;
            }
            rowDetails.put("Job_Id", String.valueOf(jobId));
            rowDetails.put("value_file_name", valueFileName);
            rowDetails.remove("DL_Version");

            status = deleteFromEltDlValuesProperties(conn, dlId, dlId);
            status = insertIntoEltDlValuesProperties(conn, rowDetails);

            writeToFile(script, valueFileName);

            return status ? Status.SUCCESS : Status.FAILURE;
        }

        // Value Expression
        public List<String> getDistinctLevels(Connection connection, long jobId, long dlId){
            List<String> levels = new ArrayList<>();
            String query = "SELECT DISTINCT `ELT_DL_Derived_Column_Info`.`Level` " +
                        "FROM `ELT_DL_Derived_Column_Info` " +
                        "WHERE Expression_Type = 'JAVA' AND Job_Id = ? AND DL_Id = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setLong(1, jobId);
                preparedStatement.setLong(2, dlId);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String level = resultSet.getString("Level");
                        levels.add(level);
                    }
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            return levels;
        }

        private String executeExpressionChildComponentValue(Connection connection, List<String> levels, long jobId, long dlId) {
            System.out.println("Total levels: " + levels.size());
            StringBuilder expressionValueBuilder = new StringBuilder();
            try {
                for (String level : levels) {
                    // part 1:
                    System.out.println("Processing level: " + level);
                    List<Map<String, Object>> data = executeQueries(connection, level, jobId, dlId);
                    
                    // TODO - seems level is not caputures in any of the data and hence in insert table. Verify!!
                    // TODO - Ditto for below few parts. Should be moved out of the loop?
                    insertIntoELTExpressionTemp(connection, data);

                    // part 2:
                    String columnExpression = fetchColumnExpressions(connection, jobId, dlId, level);

                    // part 3:
                    String expressions = updateColumnExpression(connection, columnExpression);

                    // part 4:
                    StringBuilder javaDataTypeBuilder = new StringBuilder();
                    String columnArguments = performInMemoryJoin(connection, null);
                    String javaDataType = javaDataTypeBuilder.toString();

                    // part 5:
                    String componentExpression = "expression";
                    String derivedValue= getValueNamesFromJobPropertiesInfo(conn, componentExpression);

                    // part 6:
                    String finalDerivedValue = processDerivedExpressions(connection, derivedValue, columnArguments, javaDataType, level, expressions, String.valueOf(jobId), String.valueOf(dlId));
                    if (expressionValueBuilder.length() > 0) {
                        expressionValueBuilder.append("\n");
                    }
                    expressionValueBuilder.append(finalDerivedValue);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return expressionValueBuilder.toString();
        }

        // Value Expression
        // Left outer Join to get the data to be stored in "ELT_Expression_Temp"
        public List<Map<String, Object>> executeQueries(Connection connection, String level, long jobId, long dlId) throws SQLException {
            // Query 1: Main Set
            String query1 = "SELECT DISTINCT Column_Name, Column_Arguments, Column_Expression " +
                            "FROM ELT_DL_Derived_Column_Info " +
                            "WHERE Expression_Type='JAVA' AND Level=? AND Job_Id=? AND DL_Id=?";
    
            // Query 2: Lookup Set
            String query2 = "SELECT DISTINCT Column_Name_Alias, LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)) AS Data_Type " +
                            "FROM (" +
                            "  SELECT DISTINCT Column_Name_Alias, Data_Type " +
                            "  FROM ELT_DL_Driving_Table_Info " +
                            "  WHERE Job_Id=? AND DL_Id=? " +
                            "  UNION ALL " +
                            "  SELECT DISTINCT Column_Name_Alias, Data_Type " +
                            "  FROM ELT_DL_Lookup_Table_Info " +
                            "  WHERE Job_Id=? AND DL_Id=? " +
                            "  UNION ALL " +
                            "  SELECT DISTINCT Column_Name AS Column_Name_Alias, Data_Type " +
                            "  FROM ELT_DL_Derived_Column_Info " +
                            "  WHERE Job_Id=? AND DL_Id=?" +
                            ") AS lookup";
    
            // Execute Query 1
            List<Map<String, Object>> mainResults = new ArrayList<>();
            try (PreparedStatement stmt1 = connection.prepareStatement(query1)) {
                stmt1.setString(1, level);
                stmt1.setLong(2, jobId);
                stmt1.setLong(3, dlId);
                ResultSet rs1 = stmt1.executeQuery();
    
                while (rs1.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Column_Name", rs1.getString("Column_Name"));
                    row.put("Column_Arguments", rs1.getString("Column_Arguments"));
                    row.put("Column_Expression", rs1.getString("Column_Expression"));
                    mainResults.add(row);
                }
            }
    
            // Execute Query 2 and build lookup map
            Map<String, String> lookupMap = new HashMap<>();
            try (PreparedStatement stmt2 = connection.prepareStatement(query2)) {
                stmt2.setLong(1, jobId);
                stmt2.setLong(2, dlId);
                stmt2.setLong(3, jobId);
                stmt2.setLong(4, dlId);
                stmt2.setLong(5, jobId);
                stmt2.setLong(6, dlId);
                ResultSet rs2 = stmt2.executeQuery();
    
                while (rs2.next()) {
                    // "Column_Name_Alias" is the key and "Data_Type" is the value.
                    lookupMap.put(rs2.getString("Column_Name_Alias"), rs2.getString("Data_Type"));
                }
            }
    
            List<Map<String, Object>> finalResults = new ArrayList<>();
    
            // Perform left outer join
            for (Map<String, Object> mainRow : mainResults) {
                String columnArguments = (String) mainRow.get("Column_Arguments");
                String dataType = lookupMap.getOrDefault(columnArguments, null); // Lookup Data_Type using Column_Arguments
    
                int length = (columnArguments != null) ? columnArguments.length() : 0;
    
                Map<String, Object> resultRow = new HashMap<>();
                resultRow.put("Column_Name", mainRow.get("Column_Name"));
                resultRow.put("Column_Arguments", columnArguments);
                resultRow.put("Column_Expression", mainRow.get("Column_Expression"));
                resultRow.put("Data_Type", dataType);
                resultRow.put("Lengths", length);
    
                finalResults.add(resultRow);
            }
            return finalResults;
        }
        // Value Expression part 1
        public void insertIntoELTExpressionTemp(Connection connection, List<Map<String, Object>> data) throws SQLException {
            String insertSQL = "INSERT INTO ELT_Expression_Temp (Column_Name, Column_Arguments, Column_Expression, Data_Type, Lengths) " +
                               "VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {
                for (Map<String, Object> row : data) {
                    String columnName = (String) row.get("Column_Name");
                    String columnArguments = (String) row.get("Column_Arguments");
                    String columnExpression = (String) row.get("Column_Expression");
                    String dataType = (String) row.get("Data_Type");
                    Integer lengths = (Integer) row.get("Lengths");
    
                    stmt.setString(1, columnName);
                    stmt.setString(2, columnArguments);
                    stmt.setString(3, columnExpression);
                    stmt.setString(4, dataType);
                    stmt.setInt(5, lengths);
    
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }

        // Value Expression part 2
        public String fetchColumnExpressions(Connection connection, long jobId, long dlId, String level) throws SQLException {
            
            final String SELECT_COLUMN_EXPRESSION_QUERY = 
            "SELECT DISTINCT CONCAT('\"', `Column_Expression`, '\"') " +
            "FROM `ELT_DL_Derived_Column_Info` " +
            "WHERE Expression_Type='JAVA' " +
            "AND Job_Id=? " +
            "AND DL_Id=? " +
            "AND Level=?";

            StringBuilder columnExpressionList = new StringBuilder();
            StringJoiner joiner = new StringJoiner(",");
            try (PreparedStatement statement = connection.prepareStatement(SELECT_COLUMN_EXPRESSION_QUERY)) {
                statement.setLong(1, jobId);
                statement.setLong(2, dlId);
                statement.setString(3, level);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String columnExpression = resultSet.getString(1);
                        joiner.add(columnExpression);
                    }
                }
            }

            columnExpressionList.append(joiner.toString());
            return columnExpressionList.toString();
        }

        // Value Expression part 3
        public String updateColumnExpression(Connection connection, String columnExpression) throws SQLException {
            final String SELECT_EXPRESSION_QUERY = 
                "SELECT `ELT_Expression_Temp`.`Id`, " +
                "`ELT_Expression_Temp`.`Column_Name`, " +
                "`ELT_Expression_Temp`.`Column_Arguments`, " +
                "`ELT_Expression_Temp`.`Column_Expression`, " +
                "`Lengths`, " +
                "Data_Type " +
                "FROM `ELT_Expression_Temp` ORDER BY Lengths DESC";
            String finalExpression = columnExpression;
            try (PreparedStatement statement = connection.prepareStatement(SELECT_EXPRESSION_QUERY);
                 ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("Id");
                    String columnArguments = resultSet.getString("Column_Arguments");
                    finalExpression = finalExpression.replace(columnArguments, "\\$" + (id - 1));
                }
            }
            return finalExpression;
        }

        // Value Expression part 4
        public String performInMemoryJoin(Connection connection, StringBuilder javaDataTypeBuilder) throws SQLException {
            String mainQuery = "SELECT `ELT_Expression_Temp`.`Id`, " +
                               "`ELT_Expression_Temp`.`Column_Name`, " +
                               "`ELT_Expression_Temp`.`Column_Arguments`, " +
                               "`ELT_Expression_Temp`.`Column_Expression`, " +
                               "`Lengths`, Data_Type " +
                               "FROM `ELT_Expression_Temp` ORDER BY Id";
    
            String lookupQuery = "SELECT DISTINCT LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS IL_Data_Type, " +
                                 "`ELT_Datatype_Conversions`.`Java_Data_Type` " +
                                 "FROM `ELT_Datatype_Conversions`";
    
            StringBuilder columnArgumentsBuilder = new StringBuilder();
    
            // Map to store lookup data IL_Data_Type -> Java_Data_Type
            PreparedStatement lookupStmt = connection.prepareStatement(lookupQuery);
            ResultSet lookupRs = lookupStmt.executeQuery();
    
            // Use a HashMap to store lookup results (IL_Data_Type -> Java_Data_Type)
            HashMap<String, String> lookupMap = new HashMap<>();
            while (lookupRs.next()) {
                String ilDataType = lookupRs.getString("IL_Data_Type");
                String javaDataType = lookupRs.getString("Java_Data_Type");
                lookupMap.put(ilDataType, javaDataType);
            }
            PreparedStatement mainStmt = connection.prepareStatement(mainQuery);
            ResultSet mainRs = mainStmt.executeQuery();
            while (mainRs.next()) {
                String dataType = mainRs.getString("Data_Type").toLowerCase();
                String columnArguments = mainRs.getString("Column_Arguments");
    
                String javaDataType = lookupMap.getOrDefault(dataType, "Unknown"); // TODO: what should be default value
    
                if (javaDataTypeBuilder.length() > 0) {
                    javaDataTypeBuilder.append(",");
                }
                javaDataTypeBuilder.append(javaDataType);
    
                if (columnArgumentsBuilder.length() > 0) {
                    columnArgumentsBuilder.append(",");
                }
                columnArgumentsBuilder.append(columnArguments);
            }
            return columnArgumentsBuilder.toString();
        }
        // Value Expression part 6
        public String processDerivedExpressions(Connection connection, String derivedValue, String columnArguments, String javaDataTypeInput, String level, String expression, String jobId, String dlId) throws SQLException {
            // Main query: Job_Properties_info
            String mainQuery = "SELECT DISTINCT Column_Name, LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)) AS Data_Type " +
                    "FROM ELT_DL_Derived_Column_Info WHERE Expression_Type='JAVA' " +
                    "AND Level='" + level + "' AND Job_Id='" + jobId + "' AND DL_Id='" + dlId + "'";
    
            // Lookup query: Datatypes
            String lookupQuery = "SELECT DISTINCT LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS IL_Data_Type, Java_Data_Type " +
                    "FROM ELT_Datatype_Conversions";
    
            // HashMap for lookup data: IL_Data_Type -> Java_Data_Type
            Map<String, String> lookupMap = new HashMap<>();
            PreparedStatement lookupStmt = connection.prepareStatement(lookupQuery);
            ResultSet lookupRs = lookupStmt.executeQuery();
            while (lookupRs.next()) {
                String ilDataType = lookupRs.getString("IL_Data_Type");
                String javaDataType = lookupRs.getString("Java_Data_Type");
                lookupMap.put(ilDataType, javaDataType); // Add to lookup map
            }
    
            // Execute the main query
            PreparedStatement mainStmt = connection.prepareStatement(mainQuery);
            ResultSet mainRs = mainStmt.executeQuery();
            String finalDerivedValue = null;
    
            while (mainRs.next()) {
                String columnName = mainRs.getString("Column_Name");
                String dataType = mainRs.getString("Data_Type").toLowerCase();
                String sourceDataType = lookupMap.getOrDefault(dataType, "Unknown");
    
                String expressions = derivedValue.replace("${Dynamic_Expression_Name.expressions}", 
                            "Expression_" + level + ".expressions=" + expression.replace("$", "\\\\\\$"));
    
                String fieldNames = expressions.replace("${Dynamic_Expression_Name.field.names}",
                            "Expression_" + level + ".field.names=" + columnName.replace("$", "\\\\\\$"));
    
                String argumentFields = fieldNames.replace("${Dynamic_Expression_Name.argument.fields}", 
                            "Expression_" + level + ".argument.fields=" + columnArguments.replace("$", "\\\\\\$"));
    
                String argumentTypes = argumentFields.replace("${Dynamic_Expression_Name.argument.types}", 
                            "Expression_" + level + ".argument.types=" + javaDataTypeInput);
    
                String returnTypes = argumentTypes.replace("${Dynamic_Expression_Name.return.types}", 
                            "Expression_" + level + ".return.types=" + sourceDataType);
    
                // Concatenate returnTypes into final expression
                finalDerivedValue = finalDerivedValue == null ? returnTypes : finalDerivedValue + "\n" + returnTypes;
            }
            return finalDerivedValue;
        }

        // Value SQL Expression
        private String componentSQLExpressionValue(String component) {
            String executeSQLExpressionValue = "";
            try {
                String executeSqlValue = getValueNamesFromJobPropertiesInfo(conn, component);
                String queryColumns = getQueryColumnNames(conn, String.valueOf(jobId), String.valueOf(dlId));
                executeSQLExpressionValue = fetchAndProcessColumnInfoForSQLExpression(conn, executeSqlValue, queryColumns, String.valueOf(dlId), String.valueOf(jobId)); 
                // TODO queryColumns is updated inside aabove fn. and is an output        
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return executeSQLExpressionValue;
        }

        //  Function to execute the query and group results by Level
        public String fetchAndProcessColumnInfoForSQLExpression(Connection connection, String executeSqlValue, String queryColumns, String dlId, String jobId) throws SQLException {
            // SQL Query to fetch levels and columns
            String sqlQuery = "SELECT `ELT_DL_Derived_Column_Info`.Level, " +
                            "Column_Name AS Level_Columns, " +
                            "CASE WHEN Expression_Type = 'SQL' THEN CONCAT('(', Column_Expression, ') AS `', Column_Name, '`') ELSE Column_Name END AS Columns " +
                            "FROM ELT_DL_Derived_Column_Info " +
                            "WHERE DL_Id = ? AND Job_Id = ?";

            StringBuilder finalValue = new StringBuilder();

            try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                statement.setString(1, dlId);
                statement.setString(2, jobId);

                try (ResultSet resultSet = statement.executeQuery()) {
                    String lastComponent = "";
                    while (resultSet.next()) {
                        String level = resultSet.getString("Level");
                        String columns = resultSet.getString("Columns");
                        String levelColumns = resultSet.getString("Level_Columns"); // Output

                        String sourceAlias = executeSqlValue.replace("${Dynamic_Name.source.alias.names}", "ExecuteSql_" + level + ".source.alias.names=Joined_Output_" + level);
                        String dynamicQuery = "SELECT " + String.join(",", queryColumns) + ", " + columns + " FROM Joined_Output_" + level;
                        String query = sourceAlias.replace("${Dynamic_Name.query}", "ExecuteSql_" + level + ".query=" + dynamicQuery.replace("$", "\\\\\\$"));

                        // Append the query to Final_Value
                        if (finalValue.length() == 0) {
                            finalValue.append(query);
                        } else {
                            finalValue.append("\n").append(query);
                        }
                        lastComponent = "ExecuteSql_" + level; // Output
                        queryColumns = queryColumns + "," + levelColumns; // Output
                    }

                    return finalValue.toString();
                }
            }
        }
        // Value SQL Expression Method to get column names
        public String getQueryColumnNames(Connection connection, String dlId, String jobId) throws SQLException {
            String query = SQLQueries.SELECT_COLUMN_NAME_ALIASES;
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, dlId);
                statement.setString(2, jobId);
                statement.setString(3, dlId);
                statement.setString(4, jobId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    List<String> queryColumns = new ArrayList<>();
                    while (resultSet.next()) {
                        String columnName = resultSet.getString(1);
                        queryColumns.add(columnName);
                    }
                    return String.join(", ", queryColumns);
                }
            }
        }
        // remit
        private String componentRemitValue(String component) {
            String mappingRemitValue = "";
            try {
                String mappingRetainValueRemit = getValueNamesFromJobPropertiesInfo(conn, component);
                mappingRemitValue = fetchAndProcessColumnInfoForRemit(conn, String.valueOf(jobId), String.valueOf(dlId), mappingRetainValueRemit);          
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return mappingRemitValue; //TODO
        }
        // remit
        public String fetchAndProcessColumnInfoForRemit(Connection conn, String jobId, String dlId, String mappingRetainValue) throws SQLException {
            String query = "SELECT DL_Id, Job_Id, Column_Name, Column_Alias_Name " +
                        "FROM ELT_DL_Derived_Column_Info " +
                        "WHERE Job_Id = ? AND DL_Id = ? AND Column_Alias_Name != ''";

            String mappingRemitValue = null;
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, jobId);
                ps.setString(2, dlId);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String columnName = rs.getString("Column_Name");
                        // String columnAliasName = rs.getString("Column_Alias_Name");
                        String emit_unwanted_columns = mappingRetainValue.replaceAll("\\$\\{emit.unwanted.columns}", "emit.unwanted.columns=" + columnName);
                        mappingRemitValue = emit_unwanted_columns;
                    }
                }
            }
            return mappingRemitValue;
        }
        // Rename
        private String componentRenameValue(String component) {
            String mappingRenameValue = "";
            try {
                String mappingRetainValue= getValueNamesFromJobPropertiesInfo(conn, component);
                mappingRenameValue = fetchAndProcessColumnInfo(conn, String.valueOf(jobId), String.valueOf(dlId), mappingRetainValue);          
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return mappingRenameValue; //TODO
        }
        
        // Rename
        //public static List<Map<String, String>> fetchAndProcessColumnInfo(Connection conn, String jobId, String dlId, String mappingRetainValue) throws SQLException {
        public String fetchAndProcessColumnInfo(Connection conn, String jobId, String dlId, String mappingRetainValue) throws SQLException {
            String query = "SELECT DL_Id, Job_Id, Column_Name, Column_Alias_Name " +
                        "FROM ELT_DL_Derived_Column_Info " +
                        "WHERE Job_Id = ? AND DL_Id = ? AND Column_Alias_Name != ''";

            List<Map<String, String>> resultList = new ArrayList<>();
            String mappingRenameValue = null;

            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, jobId);
                ps.setString(2, dlId);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String columnName = rs.getString("Column_Name");
                        String columnAliasName = rs.getString("Column_Alias_Name");
                        String from = mappingRetainValue.replaceAll("\\$\\{derived.rename.from}", "derived.rename.from=" + columnName);
                        String to = from.replaceAll("\\$\\{derived.rename.to}", "derived.rename.to=" + columnAliasName);

                        mappingRenameValue = to;

                        // Map<String, String> resultMap = new HashMap<>();
                        // resultMap.put("Column_Name", columnName);
                        // resultMap.put("Column_Alias_Name", columnAliasName);
                        // resultList.add(resultMap);
                    }
                }
            }
            return mappingRenameValue;
        }

        // Value Sink
        private String componentSinkValue(String component) {
            //String mappingSinkValue = "";
            try {
                String sinkValue = getValueNamesFromJobPropertiesInfo(conn, component);
                Map<String, JoinAggregationData> mappingJoinValue = executeJoinedQuery(conn, String.valueOf(dlId), String.valueOf(jobId));
                Map<String, SinkAggregationData> mappingCleansingValue = executeInnerJoinQuery(conn, String.valueOf(dlId), String.valueOf(jobId)); 
                sinkValue = executeAndJoinSinkingAggregation(conn, String.valueOf(dlId), String.valueOf(jobId), sinkValue, mappingJoinValue, mappingCleansingValue); // Output        
                return sinkValue;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return "";
        }
 
        private String executeAndJoinSinkingAggregation(Connection conn, String dlId, String jobId, String sinkValue,
                Map<String, JoinAggregationData> mappingJoinValue, Map<String, SinkAggregationData> mappingCleansingValue) {

            // Define the main query
            String mainQuery = "SELECT Table_Name, Column_Name, Column_Name_Alias, Constraints, DL_Id, Job_Id " +
                    "FROM ELT_DL_Driving_Table_Info " +
                    "WHERE Job_Id = '" + jobId + "' AND DL_Id = '" + dlId + "'";

            String finalSinkValue = "";
            // String keyFieldsValues = new String();
            StringBuilder keyFieldsValuesBuilder = new StringBuilder();

            try (PreparedStatement pstmt = conn.prepareStatement(mainQuery)) {
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    // Get the main query values
                    String tableName = rs.getString("Table_Name");
                    String columnName = rs.getString("Column_Name");
                    String columnNameAlias = rs.getString("Column_Name_Alias");
                    String constraints = rs.getString("Constraints");
                    // String dlId = rs.getString("DL_Id");
                    // String jobId = rs.getString("Job_Id");

                    // Key for joining the in-memory maps
                    String key = dlId + "-" + jobId;
                    // Retrieve values from mappingJoinValue
                    JoinAggregationData mapJoinData = mappingJoinValue.get(key);
                    // Retrieve values from mappingCleansingValue
                    SinkAggregationData mapCleansingData = mappingCleansingValue.get(key);

                    // Check if Column_Name_Alias is empty
                    //String emptyValue = columnNameAlias == null ? null : "EMPTY";

                    //String dateFormats = mapCleansingData != null && mapCleansingData.getDataType().contains("date") ? "yyyy-MM-dd" : "";

                    // Perform string replacements (similar to StringHandling.EREPLACE)
                    Context context = Context.getContext(); // TODO FIX it get the value
                    sinkValue = replaceSinkValues(sinkValue, mapJoinData, mapCleansingData, context, columnNameAlias, keyFieldsValuesBuilder, constraints);

                    finalSinkValue = sinkValue;
                    // Append date format replacement at the end B. Already done inside replaceSinkValues
                    //finalSinkValue = sinkValue.replace("${date.formats}", "date.formats=" + dateFormats);
                }
                rs.close();
                pstmt.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return finalSinkValue;
        }

        // Helper function to replace sink values based on the mappings
        // TODO: Context - Target DB how to get the value
        private String replaceSinkValues(String sinkValue, JoinAggregationData mapJoinData, SinkAggregationData mapCleansingData, Context context, String columnNameAlias,
                StringBuilder keyFieldsValues, String constraints) {

            String url = sinkValue.replace("${tgt.jdbc.url}", "tgt.jdbc.url=jdbc:mysql://" + context.getTgtHost() + ":"
                    + context.getTgtPort() + "/" + context.getTgtDbName());
            String driver = url.replace("${tgt.jdbc.driver}", "tgt.jdbc.driver=com.mysql.jdbc.Driver");
            String username = driver.replace("${tgt.db.user}", "tgt.db.user=" + context.getTgtUser());
            String password = username.replace("${tgt.db.password}", "tgt.db.password=" + context.getTgtPassword());
            String targetTable = password.replace("${target.table}", "target.table=" + context.getDlName());

            // Handle key fields values based on constraints
            // TODO: Check the keyFieldsValues is sort of StringBuilder. Is that intended?
            if ("PK".equals(constraints) || "SK".equals(constraints)) {
                if (keyFieldsValues.length() > 0) {
                    keyFieldsValues.append(",");
                }
                keyFieldsValues.append(columnNameAlias);
                //keyFieldsValues = keyFieldsValues == null ? columnNameAlias : keyFieldsValues + "," + columnNameAlias;
            }

            String keyFields = targetTable.replace("${key.fields}", "key.fields=" + keyFieldsValues.toString());
            String keyColumns = keyFields.replace("${key.columns}", "key.columns=" + keyFieldsValues.toString());
            String returnFields = keyColumns.replace("${return.fields}", "return.fields=Source_Hash_Value");

            // If mapJoinData and mapCleansingData are not null, use their values to replace the respective fields
            String batchSize = null;
            if (mapJoinData != null) {
                String joinColumn = mapJoinData.getJoinColumn();
                String retainFields = returnFields.replace("${retain.fields}", "retain.fields=" + joinColumn);
                String insertConstantColumns = retainFields.replace("${insert.constant.columns}", "insert.constant.columns=");
                String insertConstantValues = insertConstantColumns.replace("${insert.constant.store.values}", "insert.constant.store.values=");
                String insertConstantTypes = insertConstantValues.replace("${insert.constant.store.values}", "insert.constant.store.values=");

                String updateConstantColumns = insertConstantTypes.replace("${update.constant.columns}", "update.constant.columns=");
                String updateConstantValues = updateConstantColumns.replace("${update.constant.columns}", "update.constant.columns=");
                String updateConstantTypes = updateConstantValues.replace("${update.constant.store.types}", "update.constant.store.types=");

                String batchType = updateConstantTypes.replace("${batch.type}", "batch.type=BATCH_BY_SIZE");
                batchSize = batchType.replace("${batch.size}", "batch.size=25000");
            }

            if (mapCleansingData != null && batchSize != null) {
                String cleansingFields = batchSize.replace("${cleansing.fields}", "cleansing.fields=" + mapCleansingData.getColumnNameAlias());
                String cleansingValidation = cleansingFields.replace("${cleansing.validation}", "cleansing.validation=" + mapCleansingData.getCleansingValidations());
                String cleansingValues = cleansingValidation.replace("${cleansing.values}", "cleansing.values=" + mapCleansingData.getCleansingValue());
                String dateFormats = cleansingValues.replace("${date.formats}", "date.formats=" + mapCleansingData.getDateFormats());
                sinkValue = dateFormats;
            }
            return sinkValue;
        }

        
        // Value Sink
        public Map<String, JoinAggregationData> executeJoinedQuery(Connection conn, String dlId, String jobId) throws SQLException {
            // TODO made changes wrt to JobID. review
            // String query = "SELECT DISTINCT m.DL_Name AS Join_Table, " +
            //                "m.DL_Column_Names AS Join_Column, " +
            //                "m.DL_Id, " +
            //                jobId + " AS Job_Id " +
            //                "FROM ELT_DL_Mapping_Info_Saved m " +
            //                "INNER JOIN ELT_DL_Join_Mapping_Info j " +
            //                "ON m.DL_Id = j.DL_Id " +
            //                "AND m.Job_Id = j.Job_Id " +
            //                "AND m.DL_Name = j.Join_Table " +
            //                "WHERE m.DL_Id = ? AND m.Job_Id = ?";
        // New version
            String query = "SELECT DISTINCT " +
                    "main.DL_Name AS Join_Table, " +
                    "main.DL_Column_Names AS Join_Column, " +
                    "main.DL_Id, " +
                    "'" + jobId + "' AS Job_Id " +
                    "FROM ELT_DL_Mapping_Info_Saved main " +
                    "INNER JOIN ELT_DL_Join_Mapping_Info lookup " +
                    "ON main.DL_Id = lookup.DL_Id " +
                    "AND main.DL_Column_Names = lookup.Join_Column_Alias " +
                    "AND lookup.Job_Id = '" + jobId + "' " +
                    "WHERE main.DL_Id = '" + dlId + "'";

            PreparedStatement pstmt = conn.prepareStatement(query);
            // pstmt.setString(1, dlId);
            // pstmt.setString(2, jobId);
            ResultSet rs = pstmt.executeQuery();
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, JoinAggregationData> joinAggregationMap = new HashMap<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("Join_Table", rs.getString("Join_Table"));
                row.put("Join_Column", rs.getString("Join_Column"));
                row.put("DL_Id", rs.getString("DL_Id"));
                row.put("Job_Id", rs.getString("Job_Id"));
        
                // Assuming DL_Id and Job_Id combination is the unique key
                String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id");
                JoinAggregationData data = joinAggregationMap.getOrDefault(key, new JoinAggregationData(dlId, jobId));

                data.joinColumn.append(data.joinColumn.length() > 0 ? ", " : "").append(rs.getString("Join_Column"));
                data.joinTable.append(data.joinTable.length() > 0 ? ", " : "").append(rs.getString("Join_Table"));
                joinAggregationMap.put(key, data);

                resultMap.put(key, row);
            }
            rs.close();
            pstmt.close();
            return joinAggregationMap;
        }
        // Value Sink
        public Map<String, SinkAggregationData> executeInnerJoinQuery(Connection conn, String dlId, String jobId) throws SQLException {
            // TODO some changes have been made in the query `Constraints`, Source_Table_Name as Source_Name,
            String query = "SELECT DISTINCT main.Table_Name, " +
                           "main.Column_Name_Alias AS Column_Name_Alias, " +
                           "main.Constraints, " +
                           "main.Source_Name AS Source_Name, " +
                           "LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)) AS Data_Type, " +
                           "main.DL_Id, " +
                           "main.Job_Id " +
                           "FROM ( " +
                           "  SELECT DISTINCT Table_Name, Column_Name_Alias, `Constraints`, Source_Table_Name as Source_Name, LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)), DL_Id, Job_Id " +
                           "  FROM ELT_DL_Driving_Table_Info WHERE DL_Id = ? AND Job_Id = ? " +
                           "  UNION ALL " +
                           "  SELECT DISTINCT Table_Name, Column_Name_Alias, '' as `Constraints`, '' as Source_Name, LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)), DL_Id, Job_Id " +
                           "  FROM ELT_DL_Lookup_Table_Info WHERE DL_Id = ? AND Job_Id = ? " +
                           "  UNION ALL " +
                           "  SELECT DISTINCT 'Derived_Column' AS Table_Name, Column_Alias_Name, '' as `Constraints`, '' as Source_Name, LOWER(SUBSTRING_INDEX(Data_Type, '(', 1)), DL_Id, Job_Id " +
                           "  FROM ELT_DL_Derived_Column_Info WHERE DL_Id = ? AND Job_Id = ? " +
                           ") AS main " +
                           "INNER JOIN ELT_DL_Join_Mapping_Info emit " +
                           "ON main.Table_Name = emit.Join_Table " +
                           "AND main.Column_Name_Alias = emit.Join_Column_Alias " +
                           "AND main.DL_Id = emit.DL_Id " +
                           "AND main.Job_Id = emit.Job_Id " +
                           "WHERE emit.Job_Id = ? AND emit.DL_Id = ?";
        
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.setString(1, dlId);
            pstmt.setString(2, jobId);
            pstmt.setString(3, dlId);
            pstmt.setString(4, jobId);
            pstmt.setString(5, dlId);
            pstmt.setString(6, jobId);
            pstmt.setString(7, jobId); // Set Job_Id parameter for emit
            pstmt.setString(8, dlId);  // Set DL_Id parameter for emit
        
            ResultSet rs = pstmt.executeQuery();
            Map<String, String> conversionsMap = getDataTypeConversionsMapToCleansingValue(conn); // dataType -> Cleansing
            Map<String, Object> resultMap = new HashMap<>();
            // Map to aggregate based on dlId, jobId
            Map<String, SinkAggregationData> sinkAggregationMap = new HashMap<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("Table_Name", rs.getString("Table_Name"));
                row.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));
                row.put("Data_Type", rs.getString("Data_Type"));
                row.put("DL_Id", rs.getString("DL_Id"));
                row.put("Job_Id", rs.getString("Job_Id"));
        
                String dataType = rs.getString("Data_Type");
                String cleansingValue = conversionsMap.getOrDefault(dataType, "DefaultCleansingValue");

                // Aggregate on key = Dl_Id + Job_Id
                // String key = dlId + "-" + jobId;
                String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id");
                SinkAggregationData data = sinkAggregationMap.getOrDefault(key, new SinkAggregationData(dlId, jobId));
                data.Table_Name.append(data.Table_Name.length() > 0 ? ", " : "").append(rs.getString("Table_Name"));
                data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(rs.getString("Column_Name_Alias"));
                data.Constraints.append(data.Constraints.length() > 0 ? ", " : "").append(rs.getString("Constraints"));
                data.Source_Name.append(data.Source_Name.length() > 0 ? ", " : "").append(rs.getString("Source_Name"));
                data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(rs.getString("Data_Type"));
                // Specific fields
                data.Cleansing_Value.append(data.Cleansing_Value.length() > 0 ? ", " : "").append(cleansingValue);  
                data.cleansing_Validations.append(data.cleansing_Validations.length() > 0 ? ", " : "").append(rs.getString("Column_Name_Alias")==null?null:"EMPTY");
                data.date_formats.append(data.date_formats.length() > 0 ? ", " : "").append(rs.getString("Data_Type").toLowerCase().contains("date")?"yyyy-MM-dd":"");

                sinkAggregationMap.put(key, data);
                resultMap.put(key, row);
            }
            rs.close();
            pstmt.close();
            return sinkAggregationMap;
        }
        // Value Sink
        public Map<String, String> getDataTypeConversionsMapToCleansingValue(Connection conn) throws SQLException {
            String query = "SELECT " +
                    "ELT_Datatype_Conversions.Id, " +
                    "ELT_Datatype_Conversions.Source_Data_Type, " +
                    "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS IL_Data_Type, " +
                    "ELT_Datatype_Conversions.Java_Data_Type, " +
                    "Cleansing_Value " +
                    "FROM ELT_Datatype_Conversions";

            PreparedStatement pstmt = conn.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery();

            // key: IL_Data_Type, value: Cleansing_Value)
            Map<String, String> dbResultMap = new HashMap<>();
            while (rs.next()) {
                String ilDataType = rs.getString("IL_Data_Type");
                String cleansingValue = rs.getString("Cleansing_Value");
                dbResultMap.put(ilDataType, cleansingValue);
            }
            rs.close();
            pstmt.close();
            return dbResultMap;
        }


        // Value 1. Source
        private String componentSourceValue(String component1, String component2) {
            try {
                String sourceValue1 = getValueNamesFromJobPropertiesInfo(conn, component1);
                String sourceValue2 = getValueNamesFromJobPropertiesInfo(conn, component2);
                String sourceValue = getSourceValue(sourceValue1, sourceValue2);

                Map<String, SourceAggregationData> mainData = getDrivingAndLookupTableData(String.valueOf(jobId), String.valueOf(dlId), conn);
                Map<String, SourceGroupByAggregationData> groupByInfoData = getGroupByInfoData(conn, String.valueOf(jobId), String.valueOf(dlId));
                Map<String, SourceFilterByAggregationData> filterGroupByInfoData = getFilterGroupByInfoData(conn, String.valueOf(jobId), String.valueOf(dlId));

                String tmpTable = getTmpTableName();
                Map<String, Map<String, String>> tmpTableDataMap = getTmpTableData(conn, tmpTable, true); //`property` = 'db'

                sourceValue = performJoinWithLookupData(sourceValue, mainData, groupByInfoData, filterGroupByInfoData, tmpTableDataMap, String.valueOf(jobId), String.valueOf(dlId));

                return sourceValue;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return "";
        }

        private String getSourceValue(String sourceValue1, String sourceValue2) {
            String sourceValue;
            if (!sourceValue1.isEmpty() && !sourceValue2.isEmpty()) {
                sourceValue = sourceValue1 + "\n" + sourceValue2;
            } else if (!sourceValue1.isEmpty()) {
                sourceValue = sourceValue1;
            } else {
                sourceValue = sourceValue2;
            }
            return sourceValue;
        }

        // Value 2. SourceExecutesql entry point
        private String componentSourceExecuteSqlValue(String component) {
            try {
                String sourceValue = getValueNamesFromJobPropertiesInfo(conn, component);
                
                Map<String, SourceGroupByAggregationData> groupByExecuteSqlInfoData = getGroupByInfoDataSourceExecuteSql(conn, String.valueOf(jobId), String.valueOf(dlId));
                Map<String, SourceFilterByAggregationData> filterGroupByInfoData = getFilterGroupByInfoData(conn, String.valueOf(jobId), String.valueOf(dlId));

                String tmpTable = getTmpTableName();
                Map<String, Map<String, String>> tmpTableDataMap = getTmpTableData(conn, tmpTable, false);
                
                Map<String, Map<String, Object>> filtergroupByMap = getFilterGroupByInfo(conn, Long.valueOf(jobId), Long.valueOf(dlId));


                Map<String, SourceAggregationData> mainData = executeQueryAndProcessData(conn, groupByExecuteSqlInfoData, filterGroupByInfoData, tmpTableDataMap, filtergroupByMap, Long.valueOf(jobId), Long.valueOf(dlId));

                
                String finalSourceExecuteSql = getFinalSourceExecuteSqlScript(sourceValue, mainData, filterGroupByInfoData, groupByExecuteSqlInfoData, Long.valueOf(jobId), Long.valueOf(dlId));

                return finalSourceExecuteSql;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return "";
        }

        public String getFinalSourceExecuteSqlScript(
            String sourceValue, 
            Map<String, SourceAggregationData> mainData, 
            Map<String, SourceFilterByAggregationData> filterGroupByInfoData, 
            Map<String, SourceGroupByAggregationData> groupByExecuteSqlInfoData, 
            Long jobId, Long dlId
        ) {
            StringBuilder finalSource = new StringBuilder();
            
            // Iterate over the mainData entries
            for (Map.Entry<String, SourceAggregationData> entry : mainData.entrySet()) {
                String key = entry.getKey();
                SourceAggregationData row4 = entry.getValue();
                String dlIdValue = row4.getDlId();
                String jobIdValue = row4.getJobId();
                String tableName = row4.getTableName();
                String tableNameAlias = row4.getTableNameAlias();

                // Retrieve corresponding filter and groupBy data using getOrDefault
                SourceFilterByAggregationData filter = filterGroupByInfoData.getOrDefault(key, new SourceFilterByAggregationData(dlIdValue, jobIdValue, tableName, tableNameAlias));
                SourceGroupByAggregationData groupBy = groupByExecuteSqlInfoData.getOrDefault(key, new SourceGroupByAggregationData(Long.valueOf(dlIdValue), Long.valueOf(jobIdValue), tableName, tableNameAlias));

                // Add Java statements based on the mappings
                // String Var_source_values = sourceValue; // Comes from the parameters
                String flow = row4.getFlow() == null ? "" : row4.getFlow();
                        tableNameAlias = row4.getFinalTableName() + "_ExecuteSql";
                String fromTable = row4.getFinalTableName() + "_excsql";
                String whereCondition = " ";
                String havingCondition = (filter.getFilterCondition() != null) ? " having " + filter.getFilterCondition() : "";
                String groupByCondition = (groupBy.getGroupByColumns() != null && flow.equals("G")) ?
                    " group by " + groupBy.getHavingGroupByColumns() :
                    (groupBy.getGroupByColumns() != null ? " group by " + groupBy.getGroupByColumns() : "");

                String sqlQuery = row4.getFlow() == null ?
                    ("Select " + row4.getColumnNameWithAlias() + " from `" + row4.getTableName() + "`") :
                    row4.getFlow().equals("G") ? 
                        ("Select " + groupBy.getHavingGroupByColumnsWithAlias() + groupBy.getHavingAggColAlias() + 
                        " from `" + row4.getTableName() + "` " + groupByCondition + " " + havingCondition) :
                    (row4.getGroupById() == 0 ? 
                        "Select " + row4.getColumnNameWithAlias() + " from `" + row4.getTableName() + "` " + whereCondition :
                        "Select " + groupBy.getGroupByColumnsAlias() + groupBy.getAggregationColumnsWithAlias() + 
                        " from `" + row4.getTableName() + "` " + whereCondition + " " + groupByCondition);

                String partitionQuery = row4.getFlow() == null ?
                    ("Select " + row4.getColumnNameWithAlias() + " from `" + fromTable + "` ") :
                    row4.getFlow().equals("G") ?
                        (groupBy.getAggregationColumnsWithAlias() == null || groupBy.getAggregationColumnsWithAlias().isEmpty() ?
                            ("Select " + groupBy.getHavingGroupByColumnsWithAlias() + " from `" + fromTable + "` " + groupByCondition + " " + havingCondition) :
                            ("Select " + groupBy.getHavingGroupByColumnsWithAlias() + "," + groupBy.getHavingAggColAlias() + 
                            " from `" + fromTable + "` " + groupByCondition + " " + havingCondition)) :
                    (row4.getGroupById() == 0 ?
                        "Select " + row4.getColumnNameWithAlias() + " from `" + fromTable + "` " + whereCondition :
                        (groupBy.getAggregationColumnsWithAlias() == null || groupBy.getAggregationColumnsWithAlias().isEmpty() ?
                            "Select " + groupBy.getGroupByColumnsWithAlias() + " from `" + fromTable + "` " + whereCondition + groupByCondition :
                            "Select " + groupBy.getGroupByColumnsWithAlias() + "," + groupBy.getAggregationColumnsWithAlias() + 
                            " from `" + fromTable + "` " + whereCondition + groupByCondition));

                String partitionSqlQuery = row4.getFinalTableName() + ".query=" + partitionQuery;
                String sourceSqlQuery = tableNameAlias + ".sourcesql.query=" + sqlQuery;

                // TODO target DB Name etc. 
                // replace them appropriately
                String tgtHost = "", tgtport = "", tgtDbName = "", tgtUser = "", tgtPassword = "";
            
                String url = sourceValue.replace("${TableName.src.jdbc.url}", 
tableNameAlias.replace("$", "\\$") + ".src.jdbc.url=jdbc:mysql://" + tgtHost + ":" + tgtport + "/" + tgtDbName);

                String driver = url.replace("${TableName.src.jdbc.driver}", 
                    tableNameAlias.replace("$", "\\$") + ".src.jdbc.driver=com.mysql.jdbc.Driver");

                String user = driver.replace("${TableName.src.db.user}", 
                    tableNameAlias.replace("$", "\\$") + ".src.db.user=" + tgtUser);

                String password = user.replace("${TableName.src.db.password}", 
                    tableNameAlias.replace("$", "\\$") + ".src.db.password=" + 
                    tgtPassword.replace("$", "\\$"));
                    // ((String)globalMap.get("tgt_pwd")).replace("$", "\\$"));


                String partitionSize = password.replace("${TableName.partition.size}", 
                    tableNameAlias.replace("$", "\\$") + ".partition.size=");

                String dynamicPartitionQuery = partitionSize.replace("${DynamicName.query}", 
                    partitionSqlQuery.replace("$", "\\$"));

                String sourceIsQuery = dynamicPartitionQuery.replace("${TableName.source.is.query}", 
                    tableNameAlias.replace("$", "\\$") + ".source.is.query=TRUE");

                String upperBound = sourceIsQuery.replace("${TableName.partition.upper.bound}", 
                    tableNameAlias.replace("$", "\\$") + ".partition.upper.bound=");

                String noOfPartitions = upperBound.replace("${TableName.number.of.partitions}", 
                    tableNameAlias.replace("$", "\\$") + ".number.of.partitions=");

                String dynamicSourceSqlQuery = noOfPartitions.replace("Dynamic_SourceSql_Query", 
                    sourceSqlQuery.replace("$", "\\$"));

                // Append to final result
                if (finalSource.length() == 0) {
                    finalSource.append(dynamicSourceSqlQuery);
                } else {
                    finalSource.append("\n").append(dynamicSourceSqlQuery).append("\n");
                }
            }

            return finalSource.toString();
        }

        public Map<String, SourceAggregationData> executeQueryAndProcessData(Connection conn, 
                                       Map<String, SourceGroupByAggregationData> groupByExecuteSqlInfoData, 
                                       Map<String, SourceFilterByAggregationData> filterGroupByInfoData, 
                                       Map<String, Map<String, String>> tmpTableDataMap, 
                                       Map<String, Map<String, Object>> filtergroupByMap,
                                       Long jobId, Long dlId) throws SQLException {
            String query = SQLQueries.SELECT_FROM_DRIVING_LOOKUP_TABLE_INFO;

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setLong(1, jobId);
                pstmt.setLong(2, dlId);
                pstmt.setLong(3, jobId);
                pstmt.setLong(4, dlId);

                Map<String, SourceAggregationData> sourceAggregationMap = new HashMap<>();

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        
                        // Example: Accessing values from ResultSet
                        String jobIdResult = rs.getString("Job_Id");
                        String dlIdResult = rs.getString("DL_Id");
                        String tableName = rs.getString("Table_Name");
                        String columnName = rs.getString("Column_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String columnNameAlias = rs.getString("Column_Name_Alias");
                        String dataType = rs.getString("Data_Type");

                        

                        String keyForTmptableDataMap = tableNameAlias;
                        // Lookup in tmpTableDataMap
                        Map<String, String> tmpTableData = tmpTableDataMap.getOrDefault(keyForTmptableDataMap, new HashMap<>());//TODO instead of null blank value object must be return
                        String finalTableName  = tmpTableData.get("Final_Table_Name"); // TODO check for nullness
                        
                        // Key also include finalTableName. But is not benefecial or doesn't matter.
                        //String keyMain = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + tableNameAlias + "-" + finalTableName;
                        String keyMain = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + tableNameAlias;

                        Map<String, Object> filterGroupData = filtergroupByMap.getOrDefault(keyMain, new HashMap<>());//TODO instead of null blank value object must be return
                        String columnNameWithAlias = "`"+ columnNameAlias +"` as `"+  columnNameAlias +"`";
                        tableNameAlias = tableNameAlias.replace(" ","_"); 

                        SourceAggregationData data = sourceAggregationMap.getOrDefault(keyMain, new SourceAggregationData(String.valueOf(dlId), String.valueOf(jobId), tableName, tableNameAlias));
                        
                        data.Column_Name.append(data.Column_Name.length() > 0 ? ", " : "").append(columnName);
                        data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(columnNameAlias);
                        data.Column_Name_with_Alias.append(data.Column_Name_with_Alias.length() > 0 ? ", " : "").append(columnNameWithAlias);
                        data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(dataType);
                        data.Final_Table_Name = finalTableName; // TODO check it is just part of the key not the data. so list of it not required
                        // Values from lookup table
                        if (filtergroupByMap != null) {
                            // TODO Using last values only
                            data.Flow = (String) filterGroupData.get("Flow");
                            data.Filter_Id = (Long) filterGroupData.get("Filter_Id");
                            data.Group_By_Id = (Long) filterGroupData.get("Group_By_Id");
                            // data.Flow.append(data.Flow.length() > 0 ? ", " : "").append(lookupValues.get(rs.getString("Flow"))); 
                            // data.Filter_Id.append(data.Filter_Id.length() > 0 ? ", " : "").append(lookupValues.get(rs.getString("Filter_Id")));  
                            // data.Group_By_Id.append(data.Group_By_Id.length() > 0 ? ", " : "").append(lookupValues.get(rs.getString("Group_By_Id")));  
                        }
                        sourceAggregationMap.put(keyMain, data); 

                        // Use the provided maps (e.g., groupByExecuteSqlInfoData, filterGroupByInfoData, etc.)
                        // for further processing based on your logic.
                        
                        // Example usage:
                        // String key = jobIdResult + "-" + dlIdResult + "-" + tableName + "-" + columnName;
                        // Check or update tmpTableDataMap, groupByExecuteSqlInfoData, filterGroupByInfoData, etc.
                    }
                }
                return sourceAggregationMap;
            }
        }

        public String performJoinWithLookupData(
                String sourceValues,
                Map<String, SourceAggregationData> mainData,
                Map<String, SourceGroupByAggregationData> groupByInfoData,
                Map<String, SourceFilterByAggregationData> filterGroupByInfoData,
                Map<String, Map<String, String>> tmpTableDataMap,
                String jobId, String dlId) {

            StringBuilder finalSource = new StringBuilder();
            Context context = Context.getContext();
            for (Map.Entry<String, SourceAggregationData> mainEntry : mainData.entrySet()) {
                String key = mainEntry.getKey();
                SourceAggregationData mainRecord = mainEntry.getValue();
                String tableName = mainRecord.getTableName();
                String tablenameAlias = mainRecord.getTableNameAlias();

                // Lookup in groupByInfoData
                SourceGroupByAggregationData groupBy = groupByInfoData.getOrDefault(key, new SourceGroupByAggregationData(Long.valueOf(dlId), Long.valueOf(jobId), tableName, tablenameAlias));
                // Lookup in filterGroupByInfoData
                SourceFilterByAggregationData filter = filterGroupByInfoData.getOrDefault(key, new SourceFilterByAggregationData(dlId, jobId, tableName, tablenameAlias));
                // Lookup in tmpTableDataMap
                Map<String, String> tmpTableData = tmpTableDataMap.getOrDefault(key, new HashMap<String, String>());

                String whereCondition = (filter.getFilterCondition() != null) ?
                    " WHERE " + filter.getFilterCondition() : "";
                String havingCondition = (tmpTableData.get("property") == null) ? " " :
                    (filter.getFilterCondition() != null ?
                        " HAVING " + filter.getFilterCondition() : "");
                String groupByCondition = (tmpTableData.get("property") == null) ? " " :
                    (groupBy.getGroupByColumns() != null ?
                        " GROUP BY " + groupBy.getGroupByColumns() : "");

                String sqlQuery = (mainRecord.getFlow() == null) ?
                    ("SELECT " + mainRecord.getColumnNameWithAlias() + " FROM `" + mainRecord.getTableName() + "`") :
                    (mainRecord.getFlow().equals("G") ?
                        ("SELECT " + groupBy.getHavingGroupByColumnsWithAlias() + groupBy.getHavingAggColAlias() + " FROM `" + mainRecord.getTableName() + "` " + groupByCondition + " " + havingCondition) :
                        (mainRecord.getGroupById() == 0 ?
                            "SELECT " + mainRecord.getColumnNameWithAlias() + " FROM `" + mainRecord.getTableName() + "` " + whereCondition :
                            "SELECT " + groupBy.getGroupByColumnsAlias() + groupBy.getAggregationColumnsWithAlias() + " FROM `" + mainRecord.getTableName() + "` " + whereCondition + " " + groupByCondition));

                String partitionQuery = (mainRecord.getFlow() == null) ?
                    ("SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + mainRecord.getColumnNameWithAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t") :
                    (mainRecord.getFlow().equals("G") ?
                        (groupBy.getHavingAggColAlias() == null || groupBy.getHavingAggColAlias().isEmpty() ?
                            ("SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + groupBy.getHavingGroupByColumnsWithAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t " + groupByCondition + " " + havingCondition) :
                            ("SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + groupBy.getHavingGroupByColumnsWithAlias() + ", " + groupBy.getHavingAggColAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t " + groupByCondition + " " + havingCondition)) :
                        (mainRecord.getGroupById() == 0 ?
                            "SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + mainRecord.getColumnNameWithAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t " + whereCondition :
                            (groupBy.getAggregationColumnsWithAlias() == null || groupBy.getAggregationColumnsWithAlias().isEmpty() ?
                                "SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + groupBy.getGroupByColumnsWithAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t " + whereCondition + groupByCondition :
                                "SELECT ROUND((@row_number:=@row_number+1) / 40000) + 1 AS Partition_Column, " + groupBy.getGroupByColumnsWithAlias() + ", " + groupBy.getAggregationColumnsWithAlias() + " FROM `" + mainRecord.getTableName() + "`, (SELECT @row_number:=-1) AS t " + whereCondition + groupByCondition)));

                String partitionSqlQuery = mainRecord.getTableNameAlias() + ".partitionsql.query=" + partitionQuery;
                String sourceSqlQuery = mainRecord.getTableNameAlias() + ".sourcesql.query=" + sqlQuery;

                // Note: replaceAll() is used instead of replace() as regular expressions are understood by it only.
                String tgtHost = context.getTgtHost(), tgtport = context.getTgtPort(), tgtDbName = context.getTgtDbName(), tgtUser = context.getTgtUser(), tgtPassword = context.getTgtPassword();
                String url = sourceValues.replaceAll("\\$\\{TableName.src.jdbc.url}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".src.jdbc.url=jdbc:mysql://" + tgtHost + ":" + tgtport + "/" + tgtDbName);

                String driver = url.replaceAll("\\$\\{TableName.src.jdbc.driver}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".src.jdbc.driver=com.mysql.jdbc.Driver");

                String user = driver.replaceAll("\\$\\{TableName.src.db.user}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".src.db.user=" + tgtUser);

                String password = user.replaceAll("\\$\\{TableName.src.db.password}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".src.db.password=" + tgtPassword.replaceAll("\\$", "\\$"));

                String partitionSize = password.replaceAll("\\$\\{TableName.partition.size}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".partition.size=");

                String dynamicPartitionQuery = partitionSize.replaceAll("Dynamic_PartitionSql_Query",
                    partitionSqlQuery.replaceAll("\\$", "\\\\\\$"));

                String sourceIsQuery = dynamicPartitionQuery.replaceAll("\\$\\{TableName.source.is.query}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".source.is.query=TRUE");

                String upperBound = sourceIsQuery.replaceAll("\\$\\{TableName.partition.upper.bound}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".partition.upper.bound=");

                String noOfPartitions = upperBound.replaceAll("\\$\\{TableName.number.of.partitions}",
                    mainRecord.getTableNameAlias().replaceAll("\\$", "\\\\\\$") + ".number.of.partitions=");

                String dynamicSourceSqlQuery = noOfPartitions.replaceAll("Dynamic_SourceSql_Query", sourceSqlQuery.replaceAll("\\$", "\\\\\\$"));

                // Append to final dynamic join sources
                if (finalSource.length() > 0) {
                    finalSource.append("\n");
                }
                finalSource.append(dynamicSourceSqlQuery).append("\n");  // Double newlines
            }
            return finalSource.toString();
        }

        public Map<String, SourceAggregationData> getDrivingAndLookupTableData(String jobId, String dlId, Connection connection) throws SQLException {        
            Map<String, Map<String, Object>> lookupTableMap = getLookupData(jobId, dlId, connection);
            String mainQuery = buildDrivingAndLookupTableQuery();
            Map<String, SourceAggregationData> sourceAggregationMap = new HashMap<>();
            try (PreparedStatement pstmt = connection.prepareStatement(mainQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                pstmt.setString(3, jobId);
                pstmt.setString(4, dlId);
        
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    String columnNameWithAlias = "`" + rs.getString("Column_Name") + "` as `" + rs.getString("Column_Name_Alias") + "`";
                    String tableNameAliasOriginal = rs.getString("Table_Name_Alias");
                    String tableNameAliasReplaced = tableNameAliasOriginal.replace(" ", "_");

                    String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id") + "-" + rs.getString("Table_Name") + "-" + tableNameAliasReplaced;
                    String tableName = rs.getString("Table_Name");

                    Map<String, Object> lookupValues = lookupTableMap.getOrDefault(key, null); // TODO: what should be default value

                    SourceAggregationData data = sourceAggregationMap.getOrDefault(key, new SourceAggregationData(dlId, jobId, tableName, tableNameAliasReplaced));
        
                    // Values from main table
                    data.Column_Name.append(data.Column_Name.length() > 0 ? ", " : "").append(rs.getString("Column_Name"));
                    data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(rs.getString("Column_Name_Alias"));
                    data.Column_Name_with_Alias.append(data.Column_Name_with_Alias.length() > 0 ? ", " : "").append(columnNameWithAlias);
                    data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(rs.getString("Data_Type"));
                    // Values from lookup table
                    if (lookupValues != null) {
                        // Using last values only
                        data.Flow = (String) lookupValues.get(rs.getString("Flow"));
                        data.Filter_Id = (Long) lookupValues.get(rs.getString("Filter_Id"));
                        data.Group_By_Id = (Long) lookupValues.get(rs.getString("Group_By_Id")); 
                    }
                    sourceAggregationMap.put(key, data);
                }
            }
            return sourceAggregationMap;
        }
        
        public String executeMainQuery(String jobId, String dlId, Connection connection) throws SQLException {
            StringBuilder result = new StringBuilder();
            String mainQuery = buildDrivingAndLookupTableQuery();
            try (PreparedStatement pstmt = connection.prepareStatement(mainQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                pstmt.setString(3, jobId);
                pstmt.setString(4, dlId);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    String dlIdValue = rs.getString("DL_Id");
                    String jobIdValue = rs.getString("Job_Id");
                    String tableName = rs.getString("Table_Name");
                    String tableAlias = rs.getString("Table_Name_Alias");
                    String columnName = rs.getString("Column_Name");
                    String columnAlias = rs.getString("Column_Name_Alias");
                    
                    String columnNameWithAlias = "`" + columnName + "` as `" + columnAlias + "`";
        
                    String tableAliasReplaced = tableAlias.replace(" ", "_");
        
                    // Append the result to StringBuilder for demonstration (could be processed further)
                    result.append("Column: ").append(columnNameWithAlias)
                          .append(", Table Alias: ").append(tableAliasReplaced)
                          .append("\n");
                }
            }
            return result.toString();
        }
        
        public Map<String, Map<String, Object>> getLookupData(String jobId, String dlId, Connection connection) throws SQLException {
            Map<String, Map<String, Object>> lookupTableMap = new HashMap<>();
            String lookupQuery = "SELECT " +
                    "`DL_Id`, `Job_Id`, `Table_Name`, `Table_Name_Alias`, " +
                    "`Flow`, `Filter_Id`, `Group_By_Id` " +
                    "FROM `ELT_DL_FilterGroupBy_Info` " +
                    "WHERE Job_Id = ? AND DL_Id = ? AND Settings_Position IN ('Lookup_Table', 'Driving_Table')";
        
            try (PreparedStatement pstmt = connection.prepareStatement(lookupQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
        
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id") + "-" + rs.getString("Table_Name") + "-" + rs.getString("Table_Name_Alias");
                    Map<String, Object> lookupValues = new HashMap<>();
                    lookupValues.put("Flow", rs.getString("Flow"));
                    lookupValues.put("Filter_Id", rs.getString("Filter_Id"));
                    lookupValues.put("Group_By_Id", rs.getString("Group_By_Id"));
        
                    lookupTableMap.put(key, lookupValues);
                }
            }
            return lookupTableMap;
        }        
        
        // Used in components Source and SourceExecutesql
        // TODO simlar to query 2,4  3 is also in same group
        public String buildDrivingAndLookupTableQuery() {
            String query = "SELECT DISTINCT " +
                           "Job_Id, " +
                           "DL_Id, " +
                           "Table_Name, " +
                           "Table_Name AS Table_Name_Alias, " +
                           "Column_Name, " +
                           "Column_Name_Alias, " +
                           "Data_Type " +
                           "FROM ELT_DL_Driving_Table_Info " +
                           "WHERE Job_Id = ? AND DL_Id = ? " +
                           "UNION ALL " +
                           "SELECT DISTINCT " +
                           "Job_Id, " +
                           "DL_Id, " +
                           "Table_Name, " +
                           "Table_Name_Alias, " +
                           "Column_Name, " +
                           "Column_Name_Alias, " +
                           "Data_Type " +
                           "FROM ELT_DL_Lookup_Table_Info " +
                           "WHERE Job_Id = ? AND DL_Id = ?";
        
            return query;
        }

        public Map<String, Map<String, String>> getLookupTableInfoData(String jobId, String dlId, Connection connection) throws SQLException {
            Map<String, Map<String, String>> lookupTableMap = new HashMap<>();
            String lookupQuery = "SELECT DISTINCT " +
                    "DL_Id, Job_Id, Table_Name, Table_Name_Alias, Column_Name, Column_Name_Alias " +
                    "FROM ELT_DL_Lookup_Table_Info " +
                    "WHERE Job_Id = ? AND DL_Id = ?";
        
            try (PreparedStatement pstmt = connection.prepareStatement(lookupQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id");
                        Map<String, String> rowMap = new HashMap<>();
                        rowMap.put("Table_Name", rs.getString("Table_Name"));
                        rowMap.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));
                        rowMap.put("Column_Name", rs.getString("Column_Name"));
                        rowMap.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));
        
                        lookupTableMap.put(key, rowMap);
                    }
                }
            }
            return lookupTableMap;
        }
        public Map<String, Map<String, String>> getDrivingTableInfoData(String jobId, String dlId, Connection connection) throws SQLException {
            Map<String, Map<String, String>> drivingTableMap = new HashMap<>();
            String drivingQuery = "SELECT DISTINCT " +
                    "DL_Id, Job_Id, Table_Name, Table_Name AS Table_Name_Alias, Column_Name, Column_Name_Alias " +
                    "FROM ELT_DL_Driving_Table_Info " +
                    "WHERE Job_Id = ? AND DL_Id = ?";
        
            try (PreparedStatement pstmt = connection.prepareStatement(drivingQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id");
        
                        Map<String, String> rowMap = new HashMap<>();
                        rowMap.put("Table_Name", rs.getString("Table_Name"));
                        rowMap.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));  // Same as Table_Name
                        rowMap.put("Column_Name", rs.getString("Column_Name"));
                        rowMap.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));
        
                        drivingTableMap.put(key, rowMap);
                    }
                }
            }
            return drivingTableMap;
        }


        // Value SourceExecuteSql GroupBy filter
        // TODO GOOD datatype
        public Map<String, Map<String, Object>> getFilterGroupByInfo(Connection conn, Long jobId, Long dlId) throws SQLException {
            Map<String, Map<String, Object>> filterGroupByInfoMap = new HashMap<>();
            String query = SQLQueries.SELECT_FROM_FILTER_GROUP_BY_INFO;
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setLong(1, jobId);
                pstmt.setLong(2, dlId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getLong("DL_Id") + "-" + rs.getLong("Job_Id") + "-" +
                                     rs.getString("Table_Name") + "-" + rs.getString("Table_Name_Alias");
        
                        Map<String, Object> valueMap = new HashMap<>();
                        valueMap.put("Filter_Id", rs.getLong("Filter_Id"));
                        valueMap.put("Group_By_Id", rs.getLong("Group_By_Id"));
                        valueMap.put("Flow", rs.getString("Flow"));
                        filterGroupByInfoMap.put(key, valueMap);
                    }
                }
            }
            return filterGroupByInfoMap;
        }
        
        // Value SourceExecuteSql GroupBy 
        public Map<String, SourceGroupByAggregationData> getGroupByInfoDataSourceExecuteSql(Connection connection, String jobId,
                String dlId) throws SQLException {
            Map<String, Map<String, String>> drivingTableDataMap = getDrivingTableInfoData(jobId, dlId, connection);
            Map<String, Map<String, String>> lookupTableDataMap = getLookupTableInfoData(jobId, dlId, connection);
            // Data from lookup and Driving tables union'ed
            Map<String, Map<String, String>> lookupAndDrivingTableDataMap = getDistinctFromDrivingLookupTableInfo(connection, Long.valueOf(jobId), Long.valueOf(dlId));

            // Prepare the main query and execute
            try (PreparedStatement pstmt = connection.prepareStatement(SQLQueries.SELECT_DISTINCT_FROM_FILTER_GROUP_BY_INFO)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                Map<String, SourceGroupByAggregationData> sourceGroupByExecuteSqlAggregationMap = new HashMap<>();
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Long dlIdResult = rs.getLong("DL_Id");
                        Long jobIdResult = rs.getLong("Job_Id");
                        String tableName = rs.getString("Table_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String columnName = rs.getString("Column_Name");
                        String flow = rs.getString("Flow");

                        // Create keys for DrivingTable, LookupTable and comnined data
                        String drivingKey = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + columnName;
                        String lookupKey = dlIdResult + "-" + jobIdResult + "-" + tableNameAlias + "-" + columnName;
                        String queryTableKey = dlIdResult + "-" + jobIdResult + "-" + tableNameAlias + "-" + columnName;

                        // Perform left outer join with the driving table data
                        Map<String, String> drivingData = drivingTableDataMap.getOrDefault(drivingKey, new HashMap<>());
                        // Perform left outer join with the lookup table data
                        Map<String, String> lookupData = lookupTableDataMap.getOrDefault(lookupKey, new HashMap<>());
                        // Perform left outer join with the Union'ed tables data
                        Map<String, String> lookupAndDrivingData = lookupAndDrivingTableDataMap.getOrDefault(queryTableKey, new HashMap<>());

                        System.out.println("Main Query Result:");
                        System.out.println("DL_Id: " + dlIdResult);
                        System.out.println("Job_Id: " + jobIdResult);
                        System.out.println("Table_Name: " + tableName);
                        System.out.println("Table_Name_Alias: " + tableNameAlias);
                        System.out.println("Column_Name: " + columnName);

                        // Join the data from the other sources (e.g., drivingData, lookupData,
                        // queryTableData)
                        // For example, log the joins or use the data in further processing
                        System.out.println("Driving Table Data: " + drivingData);
                        System.out.println("Lookup Table Data: " + lookupData);
                        System.out.println("Query Table Data: " + lookupAndDrivingData);
                        // Key to put data into output
                        String key = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + tableNameAlias;

                        // Aggregation_Columns (corresponding to Var.Aggregation_Columns in Talend)
                        String aggregationColumns = (rs.getString("Flag").equals("true")) ? 
                            rs.getString("Aggregation") + "(`" + lookupAndDrivingData.get(rs.getString("Column_Name_Alias")) + "`)" 
                            : null;

                        // nxt_column (corresponding to Var.nxt_column)
                        String nxtColumn = (lookupData.get(rs.getString("Column_Name")) == null) ? 
                            drivingData.get(rs.getString("Column_Name_Alias")) 
                            : lookupData.get(rs.getString("Column_Name_Alias"));

                        // Aggregation_Columns_with_Alias (corresponding to Var.Aggregation_Columns_with_Alias)
                        String aggregationColumnsWithAlias = rs.getString("Flag").equals("true") ? 
                            rs.getString("Aggregation").equals("Distinct_Count") ? 
                                "count(distinct `" + nxtColumn + "`) as `" + nxtColumn + "`" :
                            rs.getString("Aggregation").equals("Stddev_Samp") ? 
                                "case when Stddev_Samp(`" + nxtColumn + "`)='NaN' then null else Stddev_Samp(`" + nxtColumn + "`) end as `" + nxtColumn + "`" :
                            rs.getString("Aggregation").equals("Var_Samp") ? 
                                "case when Var_Samp(`" + nxtColumn + "`)='NaN' then null else Var_Samp(`" + nxtColumn + "`) end as `" + nxtColumn + "`" :
                            rs.getString("Aggregation") + "(`" + nxtColumn + "`) as `" + nxtColumn + "`" : null;

                        // Group_By_Columns (corresponding to Var.Group_By_Columns)
                        String groupByColumns = rs.getBoolean("Flag") == false ? 
                            "`" + lookupAndDrivingData.get(rs.getString("Column_Name_Alias")) + "`" 
                            : null;

                        // Group_By_Columns_Alias (corresponding to Var.Group_By_Columns_Alias)
                        String groupByColumnsAlias = rs.getBoolean("Flag") == false ? 
                            (lookupData.get(rs.getString("Column_Name_Alias")) == null ? 
                                "`" + drivingData.get(rs.getString("Column_Name_Alias")) + "`" : 
                                "`" + lookupData.get(rs.getString("Column_Name_Alias")) + "`") 
                            : null;

                        // Group_By_Columns_With_Alias (corresponding to Var.Group_By_Columns_With_Alias)
                        String groupByColumnsWithAlias = rs.getBoolean("Flag") == false ? 
                            (lookupData.get(rs.getString("Column_Name_Alias")) == null ? 
                                "`" + drivingData.get(rs.getString("Column_Name_Alias")) + "` as `" + drivingData.get(rs.getString("Column_Name_Alias")) + "`" : 
                                "`" + lookupData.get(rs.getString("Column_Name_Alias")) + "` as `" + lookupData.get(rs.getString("Column_Name_Alias")) + "`") 
                            : null;

                        // Having_nxt_column (corresponding to Var.Having_nxt_column)
                        String havingNxtColumn = (lookupData.get(rs.getString("Column_Name")) == null) ? 
                            drivingData.get(rs.getString("Column_Name")) 
                            : lookupData.get(rs.getString("Column_Name"));

                        // Having_Aggregation_Columns_with_Alias (corresponding to Var.Having_Aggregation_Columns_with_Alias)
                        String havingAggregationColumnsWithAlias = rs.getBoolean("Flag") == true ? 
                            rs.getString("Aggregation").equals("Distinct_Count") ? 
                                "count(distinct `" + havingNxtColumn + "`) as `" + nxtColumn + "`" :
                            rs.getString("Aggregation").equals("Stddev_Samp") ? 
                                "case when Stddev_Samp(`" + havingNxtColumn + "`)='NaN' then null else Stddev_Samp(`" + havingNxtColumn + "`) end as `" + nxtColumn + "`" :
                            rs.getString("Aggregation").equals("Var_Samp") ? 
                                "case when Var_Samp(`" + havingNxtColumn + "`)='NaN' then null else Var_Samp(`" + havingNxtColumn + "`) end as `" + nxtColumn + "`" :
                            rs.getString("Aggregation") + "(`" + havingNxtColumn + "`) as `" + nxtColumn + "`" 
                            : null;

                        // Having_Group_By_Columns (corresponding to Var.Having_Group_By_Columns)
                        String havingGroupByColumns = rs.getBoolean("Flag") == false ? 
                            "`" + lookupAndDrivingData.get(rs.getString("Column_Name")) + "`" 
                            : null;

                        // Having_Group_By_Columns_Alias (corresponding to Var.Having_Group_By_Columns_Alias)
                        String havingGroupByColumnsAlias = rs.getBoolean("Flag") == false ? 
                            (lookupData.get(rs.getString("Column_Name_Alias")) == null ? 
                                "`" + drivingData.get(rs.getString("Column_Name")) + "` as `" + drivingData.get(rs.getString("Column_Name_Alias")) + "`" : 
                                "`" + lookupData.get(rs.getString("Column_Name")) + "` as `" + lookupData.get(rs.getString("Column_Name_Alias")) + "`") 
                            : null;

                        // Having_Group_By_Columns_With_Alias (corresponding to Var.Having_Group_By_Columns_With_Alias)
                        String havingGroupByColumnsWithAlias = rs.getBoolean("Flag") == false ? 
                            (lookupData.get(rs.getString("Column_Name_Alias")) == null ? 
                                "`" + drivingData.get(rs.getString("Column_Name")) + "` as `" + drivingData.get(rs.getString("Column_Name_Alias")) + "`" : 
                                "`" + lookupData.get(rs.getString("Column_Name")) + "` as `" + lookupData.get(rs.getString("Column_Name_Alias")) + "`") 
                            : null;
                        
                        SourceGroupByAggregationData data = sourceGroupByExecuteSqlAggregationMap.getOrDefault(key,
                            new SourceGroupByAggregationData(dlIdResult, jobIdResult, tableName, tableNameAlias));
                        // Similar to another component
                        data.Aggregation_Columns.append(data.Aggregation_Columns.length() > 0 ? ", " : "")
                                .append(aggregationColumns);
                        data.Group_By_Columns.append(data.Group_By_Columns.length() > 0 ? ", " : "")
                                .append(groupByColumns);
                        // Flow 
                        data.Flow = flow;
                        data.Aggregation_Columns_with_Alias
                                .append(data.Aggregation_Columns_with_Alias.length() > 0 ? ", " : "")
                                .append(aggregationColumnsWithAlias);
                        data.Group_By_Columns_Alias.append(data.Group_By_Columns_Alias.length() > 0 ? ", " : "")
                                .append(groupByColumnsAlias);
                        data.Group_By_Columns_With_Alias
                                .append(data.Group_By_Columns_With_Alias.length() > 0 ? ", " : "")
                                .append(groupByColumnsWithAlias);
                        data.Having_Agg_Col_Alias.append(data.Having_Agg_Col_Alias.length() > 0 ? ", " : "")
                                .append(havingAggregationColumnsWithAlias);
                        data.Having_Grpby_Columns.append(data.Having_Grpby_Columns.length() > 0 ? ", " : "")
                                .append(havingGroupByColumns);
                        data.Having_Grpby_Columns_Alias.append(data.Having_Grpby_Columns_Alias.length() > 0 ? ", " : "")
                                .append(havingGroupByColumnsAlias);
                        data.Having_Grpby_Columns_with_Alias
                                .append(data.Having_Grpby_Columns_with_Alias.length() > 0 ? ", " : "")
                                .append(havingGroupByColumnsWithAlias);
                        
                        sourceGroupByExecuteSqlAggregationMap.put(key, data);
                    }
                }
                return sourceGroupByExecuteSqlAggregationMap;
            }
        }
        // Value SourceExecuteSql GroupBy 
        public Map<String, Map<String, String>> getDistinctFromDrivingLookupTableInfo(Connection conn, long jobId, long dlId) throws SQLException {
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            String query = SQLQueries.SELECT_DISTINCT_FROM_DRIVING_LOOKUP_TABLE_INFO;
        
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setLong(1, jobId);
                pstmt.setLong(2, dlId);
                pstmt.setLong(3, jobId);
                pstmt.setLong(4, dlId);
        
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("Job_Id") + "-" + rs.getString("DL_Id") + "-" + rs.getString("Table_Name") + "-" + rs.getString("Column_Name");
                        Map<String, String> rowData = new HashMap<>();
                        rowData.put("Job_Id", rs.getString("Job_Id"));
                        rowData.put("DL_Id", rs.getString("DL_Id"));
                        rowData.put("Table_Name", rs.getString("Table_Name"));
                        rowData.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));
                        rowData.put("Column_Name", rs.getString("Column_Name"));
                        rowData.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));
                        rowData.put("Data_Type", rs.getString("Data_Type"));
                        
                        resultMap.put(key, rowData);
                    }
                }
            }
            return resultMap;
        }        

        // Value Source 'Group by Conditions'
        public Map<String, SourceGroupByAggregationData> getGroupByInfoData(Connection connection, String jobId,
                String dlId) throws SQLException {
            Map<String, Map<String, String>> drivingTableDataMap = getDrivingTableInfoData(jobId, dlId, connection);
            Map<String, Map<String, String>> lookupTableDataMap = getLookupTableInfoData(jobId, dlId, connection);
            String tmpTable = getTmpTableName();
            Map<String, Map<String, String>> tmpTableDataMap = getTmpTableData(connection, tmpTable, true); //`property` = 'db'

            try (PreparedStatement pstmt = connection.prepareStatement(SQLQueries.SOURCE_MAIN_QUERY)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                Map<String, SourceGroupByAggregationData> sourceGroupByAggregationMap = new HashMap<>();
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Long dlIdResult = rs.getLong("DL_Id");
                        Long jobIdResult = rs.getLong("Job_Id");
                        String tableName = rs.getString("Table_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String columnName = rs.getString("Column_Name");
                        String flow = rs.getString("Flow");

                        // keys for DrivingTable, LookupTable and Temp Table
                        String drivingKey = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + columnName;
                        String lookupKey = dlIdResult + "-" + jobIdResult + "-" + tableNameAlias + "-" + columnName;
                        String queryTableKey = tableNameAlias;

                        // left outer join with the driving table data
                        Map<String, String> drivingData = drivingTableDataMap.getOrDefault(drivingKey, new HashMap<>());
                        // left outer join with the lookup table data
                        Map<String, String> lookupData = lookupTableDataMap.getOrDefault(lookupKey, new HashMap<>());
                        // left outer join with the temp table data
                        Map<String, String> tmpData = tmpTableDataMap.getOrDefault(queryTableKey, new HashMap<>());

                        // Example process for final result; this can be expanded or modified
                        // System.out.println("Main Query Result:");
                        // System.out.println("DL_Id: " + dlIdResult);
                        // System.out.println("Job_Id: " + jobIdResult);
                        // System.out.println("Table_Name: " + tableName);
                        // System.out.println("Table_Name_Alias: " + tableNameAlias);
                        // System.out.println("Column_Name: " + columnName);

                        // Join the data from the other sources (e.g., drivingData, lookupData, queryTableData)
                        // For example, log the joins or use the data in further processing

                        String key = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + tableNameAlias;

                        // Map 5
                        String aggregation = tmpData.get("property") == null ? " " : rs.getString("Aggregation");
                        String nxtColumn = (drivingData.get("Column_Name") == null ? lookupData.get("Column_Name_Alias")
                                : drivingData.get("Column_Name_Alias"));
                        String aggregationColumns = (rs.getBoolean("Flag")
                                ? rs.getString("Aggregation").equals("Distinct_Count")
                                        ? "count(distinct `" + rs.getString("Column_Name") + "`) as `" + nxtColumn + "`"
                                        : rs.getString("Aggregation").equals("Stddev_Samp")
                                                ? "case when Stddev_Samp(`" + rs.getString("Column_Name")
                                                        + "`)='NaN' then null else Stddev_Samp(`"
                                                        + rs.getString("Column_Name") + "`) end as `" + nxtColumn + "`"
                                                : rs.getString("Aggregation").equals("Var_Samp")
                                                        ? "case when Var_Samp(`" + rs.getString("Column_Name")
                                                                + "`)='NaN' then null else Var_Samp(`"
                                                                + rs.getString("Column_Name") + "`) end as `"
                                                                + nxtColumn + "`"
                                                        : rs.getString("Aggregation") + "(`"
                                                                + rs.getString("Column_Name") + "`) as `" + nxtColumn
                                                                + "`"
                                : null);
                        String aggregationColumnsWithAlias = (rs.getBoolean("Flag")
                                ? aggregation + "(`" + rs.getString("Column_Name") + "`) as `"
                                        + (drivingData.get("Column_Name") == null ? lookupData.get("Column_Name_Alias")
                                                : drivingData.get("Column_Name_Alias"))
                                        + "`"
                                : null);
                        String groupByColumns = (rs.getBoolean("Flag") == false
                                ? "`" + rs.getString("Column_Name") + "`"
                                : null);
                        String groupByColumnsAlias = (rs.getBoolean("Flag") == false
                                ? (drivingData.get("Column_Name") == null
                                        ? "`" + lookupData.get("Column_Name_Alias") + "`"
                                        : "`" + drivingData.get("Column_Name_Alias") + "`")
                                : null);
                        String groupByColumnsWithAlias = (rs.getBoolean("Flag") == false
                                ? (drivingData.get("Column_Name") == null
                                        ? "`" + lookupData.get("Column_Name") + "` as `"
                                                + lookupData.get("Column_Name_Alias") + "`"
                                        : "`" + drivingData.get("Column_Name") + "` as `"
                                                + drivingData.get("Column_Name_Alias") + "`")
                                : null);
                        String havingAggColAlias = (rs.getBoolean("Flag")
                                ? aggregation + "(`" + rs.getString("Column_Name") + "`) as `"
                                        + (drivingData.get("Column_Name") == null ? lookupData.get("Column_Name")
                                                : drivingData.get("Column_Name"))
                                        + "`"
                                : null);
                        String havingGrpByColumns = (rs.getBoolean("Flag") == false
                                ? "`" + rs.getString("Column_Name") + "`"
                                : null);
                        String havingGrpByColumnsAlias = (rs.getBoolean("Flag") == false
                                ? (drivingData.get("Column_Name") == null ? "`" + lookupData.get("Column_Name") + "`"
                                        : "`" + drivingData.get("Column_Name") + "`")
                                : null);
                        String havingGrpByColumnsWithAlias = (rs.getBoolean("Flag") == false
                                ? (drivingData.get("Column_Name") == null
                                        ? "`" + lookupData.get("Column_Name") + "` as `" + lookupData.get("Column_Name")
                                                + "`"
                                        : "`" + drivingData.get("Column_Name") + "` as `"
                                                + drivingData.get("Column_Name") + "`")
                                : null);

                        SourceGroupByAggregationData data = sourceGroupByAggregationMap.getOrDefault(key,
                                new SourceGroupByAggregationData(dlIdResult, jobIdResult, tableName, tableNameAlias));

                        data.Aggregation_Columns.append(data.Aggregation_Columns.length() > 0 ? ", " : "")
                                .append(aggregationColumns);
                        data.Group_By_Columns.append(data.Group_By_Columns.length() > 0 ? ", " : "")
                                .append(groupByColumns);
                        data.Flow = flow; // Flow is Last value, rest are lists
                        data.Aggregation_Columns_with_Alias
                                .append(data.Aggregation_Columns_with_Alias.length() > 0 ? ", " : "")
                                .append(aggregationColumnsWithAlias);
                        data.Group_By_Columns_Alias.append(data.Group_By_Columns_Alias.length() > 0 ? ", " : "")
                                .append(groupByColumnsAlias);
                        data.Group_By_Columns_With_Alias
                                .append(data.Group_By_Columns_With_Alias.length() > 0 ? ", " : "")
                                .append(groupByColumnsWithAlias);
                        data.Having_Agg_Col_Alias.append(data.Having_Agg_Col_Alias.length() > 0 ? ", " : "")
                                .append(havingAggColAlias);
                        data.Having_Grpby_Columns.append(data.Having_Grpby_Columns.length() > 0 ? ", " : "")
                                .append(havingGrpByColumns);
                        data.Having_Grpby_Columns_Alias.append(data.Having_Grpby_Columns_Alias.length() > 0 ? ", " : "")
                                .append(havingGrpByColumnsAlias);
                        data.Having_Grpby_Columns_with_Alias
                                .append(data.Having_Grpby_Columns_with_Alias.length() > 0 ? ", " : "")
                                .append(havingGrpByColumnsWithAlias);

                        sourceGroupByAggregationMap.put(key, data);
                    }
                }
                return sourceGroupByAggregationMap;
            }
        }
                
        public Map<String, SourceFilterByAggregationData> getFilterGroupByInfoData(Connection conn, String jobId, String dlId) throws SQLException {           
            String query = SQLQueries.GET_FILTER_GROUP_BY_INFO_QUERY;
            Map<String, SourceFilterByAggregationData> sourceFilterByAggregationMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {    
                        String dlIdResult = rs.getString("DL_Id");
                        String jobIdResult = rs.getString("Job_Id");
                        String tableName = rs.getString("Table_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");

                        Long filterId = rs.getLong("Filter_Id");
                        String flow = rs.getString("Flow");
                        String filterCondition = rs.getString("Filter_Condition");
                        String filterColumns = rs.getString("Filter_Columns");
                        
                        String key = dlIdResult + "-" + jobIdResult + "-" + tableName + "-" + tableNameAlias;

                        SourceFilterByAggregationData data = sourceFilterByAggregationMap.getOrDefault(key, new SourceFilterByAggregationData(dlId, jobId, tableName, tableNameAlias));

                        data.Filter_Id = filterId; // Long
                        data.Flow = flow; // String
                        data.Filter_Condition = filterCondition;
                        data.Filter_Columns = filterColumns;

                        sourceFilterByAggregationMap.put(key, data);
                    }
                }
            }
            return sourceFilterByAggregationMap;
        }

        // Value 3. lkp/Join
        private String componentJoinValue(String component) {
            try {
                String joinValue = getValueNamesFromJobPropertiesInfo(conn, component);
                String tmpTable = dlName + dlId + jobId;
                joinValue = joinTablesAndFetchDerivedValues(conn, joinValue, tmpTable, String.valueOf(jobId), String.valueOf(dlId));  
                return joinValue;         
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return "";
        }

        public String joinTablesAndFetchDerivedValues(Connection connection, String joinValue, String tmpTable, String jobId, String dlId) throws SQLException {
            String sqlQuery = "SELECT " +
                    "    main.DL_Id, " +
                    "    main.Job_Id, " +
                    "    Join_Name, " +
                    "    Left_Hand_Fields, " +
                    "    Right_Hand_Fields, " +
                    "    main.Table_Name_Alias, " +
                    "    main.Join_Table_Alias, " +
                    "    lookup2.Final_Table_Name AS Final_Table_Name_2, " +
                    "    lookup2.property AS property_2, " +
                    "    lookup3.Final_Table_Name AS Final_Table_Name_3, " +
                    "    lookup3.property AS property_3 " +
                    "FROM ( " +
                    "    SELECT " +
                    "        DL_Id, " +
                    "        Job_Id, " +
                    "        Table_Name_Alias, " +
                    "        Join_Table_Alias, " +
                    "    CONCAT(Table_Name_Alias, '_', Join_Table_Alias) AS Join_Name, " +
                    "    GROUP_CONCAT(Column_Name_Alias) AS Left_Hand_Fields, " +
                    "    GROUP_CONCAT(Join_Column_Alias) AS Right_Hand_Fields " +
                    "    FROM ELT_DL_Join_Mapping_Info " +
                    "    WHERE Job_Id = ? " +  // placeholder for Job_Id
                    "      AND DL_Id = ? " +   // placeholder for DL_Id
                    "    GROUP BY Table_Name_Alias, Join_Table_Alias " +
                    "    ORDER BY Join_Level " +
                    ") AS main " +
                    "LEFT OUTER JOIN ( " +
                    "    SELECT " +
                    "        LOWER(SUBSTRING_INDEX(table_name, '(', 1)) AS table_name, " +
                    "        Final_Table_Name, " +
                    "        property " +
                    "    FROM " + tmpTable +
                    "    WHERE property != 'db' " +
                    ") AS lookup2 " +
                    "ON main.Table_Name_Alias = lookup2.table_name " +
                    "LEFT OUTER JOIN ( " +
                    "    SELECT " +
                    "        LOWER(SUBSTRING_INDEX(table_name, '(', 1)) AS table_name, " +
                    "        Final_Table_Name, " +
                    "        property " +
                    "    FROM " + tmpTable +  // Dynamic table name
                    "    WHERE property != 'db' " +
                    ") AS lookup3 " +
                    "ON main.Join_Table_Alias = lookup3.table_name " +
                    "GROUP BY main.Table_Name_Alias, main.Join_Table_Alias;";
    
            try (PreparedStatement pstmt = connection.prepareStatement(sqlQuery)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);
    
                try (ResultSet rs = pstmt.executeQuery()) {
                    StringBuilder finalJoinValue = new StringBuilder();
                    while (rs.next()) {
                        // String joinName = rs.getString("Join_Name");
                        String tableNameAlias = rs.getString("Table_Name_Alias");
                        String joinTableAlias = rs.getString("Join_Table_Alias");
                        String leftHandFields = rs.getString("Left_Hand_Fields");
                        String rightHandFields = rs.getString("Right_Hand_Fields");
                        String finalTableName2 = rs.getString("Final_Table_Name_2");
                        String property2 = rs.getString("property_2");
                        String finalTableName3 = rs.getString("Final_Table_Name_3");
                        String property3 = rs.getString("property_3");

                        // Map - set1
                        String tablename = (property2 == null) ? tableNameAlias : finalTableName2 + "_ExecuteSql";
                        String jointablename = (property3 == null) ? joinTableAlias : finalTableName3 + "_ExecuteSql";
                        String joinName = tablename + "_" + jointablename;
                        joinName = joinName.replace(" ", "_");

                        // Map - set2
                        // TODO These below expressions have to be reviewed 
                        String joinType = joinValue.replace("${Dynamic_Join_Name.join.type}", 
                        joinName.replace("$", "\\$") + ".join.type=LEFT_OUTER_JOIN");

                        String leftFields = joinType.replace("${Dynamic_Join_Name.left.hand.side.fields}", 
                        joinName.replace("$", "\\$") + ".left.hand.side.fields=" + leftHandFields.replace("$", "\\$"));

                        String rightFields = leftFields.replace("${Dynamic_Join_Name.right.hand.side.fields}", 
                        joinName.replace("$", "\\$") + ".right.hand.side.fields=" + rightHandFields.replace("$", "\\$"));

                        if (finalJoinValue.length() > 0) {
                            finalJoinValue.append("\n");
                        }
                        finalJoinValue.append(rightFields).append("\n");
                    }
                    return finalJoinValue.toString();
                }
            }
        }

        // Value 4. Recoercing
        private String componentRecoercing(String component) {
           try {
                String emptyRecoercingValue = getValueNamesFromJobPropertiesInfo(conn, component);
                Map<String, RecoercingAggregationData> recoercingMap = executeJoinQuery(conn, String.valueOf(dlId), String.valueOf(jobId));
                emptyRecoercingValue = executeAndJoinRecoercingAggregation(conn, String.valueOf(dlId), String.valueOf(jobId), emptyRecoercingValue, recoercingMap); // Output
                return emptyRecoercingValue;
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return "";
        }
        // Value 4. Recoercing
        public String executeAndJoinRecoercingAggregation(Connection conn, String dlId, String jobId, String recoercingValue, Map<String, RecoercingAggregationData> recoercingMap) throws SQLException {
            String query = "SELECT DL_Id, Job_Id FROM ELT_DL_Driving_Table_Info WHERE Job_Id = ? AND DL_Id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, jobId);
                pstmt.setString(2, dlId);

                try (ResultSet rs = pstmt.executeQuery()) {
                    // Map<String, RecoercingAggregationData> resultMap = new HashMap<>(); // TODO Not needed anymore
                    while (rs.next()) {
                        String dlIdValue = rs.getString("DL_Id");
                        String jobIdValue = rs.getString("Job_Id");

                        String key = dlIdValue + "-" + jobIdValue;

                        RecoercingAggregationData recoercingData = recoercingMap.getOrDefault(key, new RecoercingAggregationData(dlIdValue, jobIdValue));
                            
                        // Perform the replacements sequentially
                        recoercingValue = recoercingValue
                                .replace("${recoerce.to.format}", "recoerce.to.format=" + recoercingData.getRecoerceToFormat())
                                .replace("${recoerce.to.type}", "recoerce.to.type=" + recoercingData.getRecoerceToType())
                                .replace("${recoerce.decimal.precisions}",
                                        "recoerce.decimal.precisions=" + recoercingData.getRecoerceDecimalPrecisions())
                                .replace("${recoerce.decimal.scales}",
                                        "recoerce.decimal.scales=" + recoercingData.getRecoerceDecimalScales())
                                .replace("${recoerce.fields}", "recoerce.fields=" + recoercingData.getColumnNameAlias());
                            
                        // resultMap.put(key, recoercingData); Not Needed anymore
                    }
                    return recoercingValue;
                }
            }
        }
        // Value 4. Recoercing
        public Map<String, RecoercingAggregationData> executeJoinQuery(Connection conn, String dlId, String jobId) throws SQLException {
            // TODO: Table_Name, Source_Name, `Column_Name_Alias`, Precision_Val and Scale_Val should be reviews in case of error
            String query = "SELECT " +
                           "m.DL_Id, " +
                           jobId + " AS Job_Id, " +
                           "'' AS Table_Name, " +
                           "'' AS Source_Name, " +
                           "m.DL_Column_Names AS Column_Name_Alias, " +
                           "m.Constraints, " +
                           "LOWER(SUBSTRING_INDEX(m.DL_Data_Types, '(', 1)) AS Data_Type, " +
                        //    "j.Join_Table, " +
                        //    "j.Join_Column_Alias, " +
                        //    "j.DL_Id, " +
                        //    "j.Job_Id, " +
                           "SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(m.DL_Data_Types, '(', -1), ')', 1), ',', 1) AS Precision_Val, " +
                           "SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(m.DL_Data_Types, '(', -1), ')', 1), ',', -1) AS Scale_Val " +
                           "FROM ELT_DL_Mapping_Info_Saved m " +
                        //    "INNER JOIN ELT_DL_Join_Mapping_Info j " +
                        //    "ON m.DL_Id = j.DL_Id " +
                        //    "AND " + jobId + " = j.Job_Id " +
                        //    "AND m.DL_Column_Names = j.Join_Column_Alias " +
                           "WHERE m.DL_Id = ? " +
                           /* "AND m.Job_Id = ? " +*/
                           "AND m.DL_Column_Names NOT IN ( " +
                           "SELECT DISTINCT Column_Alias_Name " +
                           "FROM ELT_DL_Derived_Column_Info " +
                           "WHERE DL_ID = ? AND Job_Id = ?)";
    
            Map<String, String> lookupMap = getDataTypeConversionsMapToJavaDataType(conn);
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            // Map to aggregate based on dlId, jobId
            Map<String, RecoercingAggregationData> aggregationMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                // pstmt.setString(1, jobId);
                pstmt.setString(1, dlId);
                //pstmt.setString(2, jobId);
                pstmt.setString(2, dlId);
                pstmt.setString(3, jobId);
    
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        //String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id") + "-" + rs.getString("Join_Column_Alias");
                        Map<String, Object> row = new HashMap<>();
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));
                        row.put("Constraints", rs.getString("Constraints"));
                        row.put("Data_Type", rs.getString("Data_Type"));
                        // row.put("Join_Table", rs.getString("Join_Table"));
                        // row.put("Join_Column_Alias", rs.getString("Join_Column_Alias"));
                        // Below 3 have to be reviewed if they exists and what values
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("Source_Name", rs.getString("Source_Name"));
                        //row.put("Column_Name_Alias", rs.getString("Column_Name_Alias"));

                        // TODO check that all the keys are there in the sql table or resultset
                        String dataType = rs.getString("Data_Type").toLowerCase();  //
                        boolean isDecimalType = dataType.contains("decimal") ||
                                                dataType.contains("double") ||
                                                dataType.contains("float") ||
                                                dataType.contains("numeric") ||
                                                dataType.contains("real");

                        String precisionVal = isDecimalType ? rs.getString("Precision_Val") : "";
                        String scaleVal = isDecimalType ? rs.getString("Scale_Val") : "";
                        row.put("Precision_Val", precisionVal);
                        row.put("Scale_Val", scaleVal);
                        // from ELT_Datatype_Convesions
                        String javaDataType = lookupMap.getOrDefault(dataType, ""); // TODO: what should be default value
                        // TODO: Inner join means javaDataType is "Unknown", continue
                        row.put("Java_Data_Type", javaDataType);

                        // from Map 6
                        row.put("recoerce_to_format", dataType.contains("date")?"yyyy-MM-dd":"");
                        row.put("recoerce_to_type", javaDataType);
                        row.put("recoerce_decimal_precisions", precisionVal);
                        row.put("recoerce_decimal_scales", scaleVal);

                        // Aggregate on key = Dl_Id + Job_Id
                        String key = dlId + "-" + jobId;
                        // Retrieve or create the AggregationData object. make a list of data.
                        // TO DO include the check if first element itself is BLANK
                        RecoercingAggregationData data = aggregationMap.getOrDefault(key, new RecoercingAggregationData(dlId, jobId));
                        data.Table_Name.append(data.Table_Name.length() > 0 ? ", " : "").append(rs.getString("Table_Name"));
                        data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(rs.getString("Column_Name_Alias"));
                        data.Constraints.append(data.Constraints.length() > 0 ? ", " : "").append(rs.getString("Constraints"));
                        data.Source_Name.append(data.Source_Name.length() > 0 ? ", " : "").append(rs.getString("Source_Name"));
                        data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(rs.getString("Data_Type"));
                        data.recoerce_to_format.append(data.recoerce_to_format.length() > 0 ? ", " : "").append(rs.getString("Data_Type").toLowerCase().contains("date")?"yyyy-MM-dd":"");
                        data.recoerce_to_type.append(data.recoerce_to_type.length() > 0 ? ", " : "").append(javaDataType);
                        data.recoerce_decimal_precisions.append(data.recoerce_decimal_precisions.length() > 0 ? ", " : "").append(precisionVal);
                        data.recoerce_decimal_scales.append(data.recoerce_decimal_scales.length() > 0 ? ", " : "").append(scaleVal);

                        aggregationMap.put(key, data);

                        resultMap.put(key, row);
                        // TODO resultMap and row can be removed.
                    }
                }
            }
            return aggregationMap;
        }
        // value 4;   Recoercing
        // TODO there is similar copy of it. check and see if one of them can be can be removed. (executeDataTypeConversionsQuery)
        // (getDataTypeConversionsMapToCleansingValue) - they have different value for the key in map
        public Map<String, String> getDataTypeConversionsMapToJavaDataType(Connection conn) throws SQLException {
            String query = "SELECT "
                    + "`ELT_Datatype_Conversions`.`Source_Data_Type`, "
                    + "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS IL_Data_Type, "
                    + "`ELT_Datatype_Conversions`.`Java_Data_Type` "
                    + "FROM `ELT_Datatype_Conversions`";
            
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String ilDataType = rs.getString("IL_Data_Type");
                    String javaDataType = rs.getString("Java_Data_Type");
                    resultMap.put(ilDataType, javaDataType);
                }
            }
            return resultMap;
        }

        // Value 6. filterGroupByValue
        private Map<String, String> componentFilterGroupBy(String component) {
            Map<String, String> out = new HashMap<String, String>();
            try {
                // Part 1
                String filterGroupByValue = getValueNamesFromJobPropertiesInfo(conn, component);
                // Part 2
                String settingsPosition = "Join_Columns";
                String scriptJoinFilterGroupby = getScriptFilterGroupByForSettingPosition(filterGroupByValue, settingsPosition); // output
                out.put("JoinFilterGroupby", scriptJoinFilterGroupby);
                // Part 3
                settingsPosition = "Derived_Columns";
                String scriptDerivedFilterGroupby = getScriptFilterGroupByForSettingPosition(filterGroupByValue, settingsPosition);  // Output  
                out.put("DerivedFilterGroupby", scriptDerivedFilterGroupby);
        
                // TODO: Part2 and part 3 are similar. A. settingsPosition is different. B. Internal final processing is different
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return out;
        }

        private String getScriptFilterGroupByForSettingPosition(String filterGroupByValue, String settingsPosition)
                throws SQLException {
            ResultSet rs = executeSelectFromEltDlFilterGroupByInfo(conn, settingsPosition, String.valueOf(dlId), String.valueOf(jobId));
            //  step 2a, 2b
            Map<String, AggregationData> mapGroupByData = executeSelectGroupByInfo(conn, settingsPosition, String.valueOf(dlId), String.valueOf(jobId));
            Map<String, Map<String, Object>> mapFilterData = executeFilterGroupByInfoQuery(conn, String.valueOf(dlId), String.valueOf(jobId), settingsPosition);
            // Step 2
            StringBuilder scriptJoinFilterGroupbyBuilder =  new StringBuilder();
            while (rs.next()) {
                String dlIdValue = rs.getString("DL_Id");
                String jobIdValue = rs.getString("Job_Id");
                String groupByIdValue = rs.getString("Group_By_Id");
                String filterIdValue = rs.getString("Filter_Id");
                String flowValue = rs.getString("Flow");

                String key = dlIdValue + "-" + jobIdValue;

                AggregationData groupByData = mapGroupByData.get(key);
                Map<String, Object> filterData = mapFilterData.get(key);

                Map<String, Object> row = new HashMap<>();
                row.put("DL_Id", dlIdValue);
                row.put("Job_Id", jobIdValue);
                row.put("Group_By_Id", groupByIdValue);
                row.put("Filter_Id", filterIdValue);
                row.put("Flow", flowValue);

                if (filterData != null) {
                    row.put("Filter_Condition", filterData.get("Filter_Condition"));
                }
                if (groupByData != null) {
                    row.put("Group_By_Data", groupByData);
                }

                String scriptFilterGroupby = processFilterGroupBy(filterGroupByValue, row, settingsPosition);

                if (scriptJoinFilterGroupbyBuilder.length() > 0) {
                    scriptJoinFilterGroupbyBuilder.append(", ");
                }
                scriptJoinFilterGroupbyBuilder.append(scriptFilterGroupby);
            }
            return scriptJoinFilterGroupbyBuilder.toString();
        }

        public String processFilterGroupBy(String filterGroupby, Map<String, Object> row, String settingsPosition) {

            long dlIdValue = (long) row.get("DL_Id");
            long jobIdValue = (long) row.get("Job_Id");
            long groupByIdValue = (long) row.get("Group_By_Id");
            long filterIdValue = (long) row.get("Filter_Id");
            String flowValue = (String) row.get("Flow");
            String filterConditionValue = (String) row.get("Filter_Condition");
            AggregationData groupByData = (AggregationData) row.get("Group_By_Data");
            String groupbycolumns = groupByData.getGroupbycolumns().toString();
            String allcolumn = groupByData.getAllcolumn().toString();

            String whereCondition = "";
            if (filterIdValue != 0 && groupByIdValue == 0) {
                whereCondition = " where " + filterConditionValue;
            } else if (filterIdValue != 0 && groupByIdValue > 0 && "F".equals(flowValue)) {
                whereCondition = " where " + filterConditionValue + " Group by " + groupbycolumns;
            } else if (filterIdValue != 0 && groupByIdValue > 0 && "G".equals(flowValue)) {
                whereCondition = " Group by " + groupbycolumns + " having " + filterConditionValue;
            } else if (filterIdValue == 0 && groupByIdValue > 0) {
                whereCondition = " Group by " + groupbycolumns;
            }

            String columns = groupByIdValue != 0 ? allcolumn : " * ";
            String query = buildQueryForSettingPosition(filterGroupby, whereCondition, columns, settingsPosition);

            return query;
        }

        private String buildQueryForSettingPosition(String filterGroupby, String whereCondition, String columns, String settingsPosition) {
            String statement;
            String aliasName;
            String query;
            if ("'Join_Columns'".equals(settingsPosition)) {
                statement = "select " + columns + " from Join_FilterGroupby " + whereCondition;
                aliasName = filterGroupby.replace("${Dynamic_FilterGroupby_Name.source.alias.names}",
                        "Join_Aggregation.source.alias.names=Join_FilterGroupby");
                query = aliasName.replace("${Dynamic_FilterGroupby_Name.query}",
                        "Join_Aggregation.query=" + statement);
            } else if ("'Derived_Columns'".equals(settingsPosition)) {
                statement = "select " + columns + " from Derived_FilterGroupby " + whereCondition;
                aliasName = filterGroupby.replace("${Dynamic_FilterGroupby_Name.source.alias.names}",
                        "Derived_Aggregation.source.alias.names=Derived_FilterGroupby");
                query = aliasName.replace("${Dynamic_FilterGroupby_Name.query}",
                        "Derived_Aggregation.query=" + statement);
            } else {
                // Throw an exception for undefined Setting_Position
                throw new IllegalArgumentException("Setting_Position is not defined: " + settingsPosition);
            }

            return query;
        }

        public ResultSet executeSelectFromEltDlFilterGroupByInfo(Connection connection, String settingsPosition, String dlId, String jobId) throws SQLException {
            String query = "SELECT " +
                           "`ELT_DL_FilterGroupBy_Info`.`Job_Id`, " +
                           "`ELT_DL_FilterGroupBy_Info`.`DL_Id`, " +
                           "`ELT_DL_FilterGroupBy_Info`.`Group_By_Id`, " +
                           "`ELT_DL_FilterGroupBy_Info`.`Filter_Id`, " +
                           "`ELT_DL_FilterGroupBy_Info`.`Flow` " +
                           "FROM `ELT_DL_FilterGroupBy_Info` " +
                           "WHERE Settings_Position = ? AND DL_Id = ? AND Job_Id = ?";
        
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, settingsPosition);
            preparedStatement.setString(2, dlId);
            preparedStatement.setString(3, jobId);
        
            return preparedStatement.executeQuery();
        }

        public Map<String, AggregationData> executeSelectGroupByInfo(Connection connection, String settingPosition, String dlId, String jobId) throws SQLException {
            String query = "SELECT a.DL_Id, a.Job_Id, a.Group_By_Id, " +
                    "b.Table_Name_Alias, b.Column_Name, b.Column_Name_Alias, " +
                    "b.Aggregation, b.Flag " +
                    "FROM ELT_DL_FilterGroupBy_Info a " +
                    "INNER JOIN ELT_DL_Group_By_Info b " +
                    "ON a.Group_By_Id = b.Group_By_Id " +
                    "WHERE a.Settings_Position = ? AND a.DL_Id = ? AND a.Job_Id = ?";

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, settingPosition);
            preparedStatement.setString(2, dlId);
            preparedStatement.setString(3, jobId);

            ResultSet resultSet = preparedStatement.executeQuery();

            // Map to aggregate based on dlId, jobId, groupId
            Map<String, AggregationData> aggregationMap = new HashMap<>();

            // StringBuilder allcolumnBuilder = new StringBuilder();
            // StringBuilder groupbycolumns = new StringBuilder();
            while (resultSet.next()) {
                // String dlId = resultSet.getString("DL_Id");
                // String jobId = resultSet.getString("Job_Id");
                String groupId = resultSet.getString("Group_Id");

                String aggregation = resultSet.getString("Aggregation");
                String columnNameAlias = resultSet.getString("Column_Name_Alias");
                int flag = resultSet.getInt("Flag");

                String finalAggregation = aggregation.equals("Random") ? "First"
                        : aggregation.equals("GroupBy") ? "" : aggregation;

                String allcolumn;
                if (finalAggregation.equals("Distinct_Count")) {
                    allcolumn = "count(distinct `" + columnNameAlias + "`) as `" + columnNameAlias + "`";
                } else if (finalAggregation.equals("Stddev_Samp")) {
                    allcolumn = "case when Stddev_Samp(`" + columnNameAlias + "`)='NaN' then null else Stddev_Samp(`"
                            + columnNameAlias + "`) end as `" + columnNameAlias + "`";
                } else if (finalAggregation.equals("Var_Samp")) {
                    allcolumn = "case when Var_Samp(`" + columnNameAlias + "`)='NaN' then null else Var_Samp(`"
                            + columnNameAlias + "`) end as `" + columnNameAlias + "`";
                } else {
                    allcolumn = finalAggregation + "(`" + columnNameAlias + "`) as `" + columnNameAlias + "`";
                }

                String key = dlId + "-" + jobId + "-" + groupId;
                // Retrieve or create the AggregationData object
                // TODO: ctr should have all fields default
                AggregationData data = aggregationMap.getOrDefault(key, new AggregationData(dlId, jobId, groupId));
                
                if (data.allcolumn.length() > 0) {
                    data.allcolumn.append(", ");
                }
                data.allcolumn.append(allcolumn);  // 'allcolumn' is appended as part of aggregation
                
                // if (allcolumnBuilder.length() > 0) {
                //     allcolumnBuilder.append(", ");
                // }
                // allcolumnBuilder.append(allcolumn);

                // Processing groupbycolumns based on flag value
                // TODO ensure that the last groupbycolumns is appended.
                if (flag == 0) {
                    if (data.groupbycolumns.length() == 0) {
                        data.groupbycolumns.append(columnNameAlias);
                    } else {
                        // Add/insert in the beginning followed by a comma
                        data.groupbycolumns.insert(0, columnNameAlias + ",");
                    }
                }

                // if (flag == 0) {
                //     if (groupbycolumns.length() == 0) {
                //         groupbycolumns.append(columnNameAlias);
                //     } else {
                //         // Add/insert in the beginning followed by a comma
                //         groupbycolumns.insert(0, columnNameAlias + ",");
                //     }
                // }

                aggregationMap.put(key, data);
            }

            // TODO: Sample code to extract the data. Not needed at this place.
            // AggregationData result = aggregationMap.get(dlId + "-" + jobId + "-" + groupId);
            // String finalAllColumns = result.allcolumn.toString();
            // String finalGroupByColumns = result.groupbycolumns.toString();
            // Old Code. Irrevant here
            // String finalAllColumns = allcolumnBuilder.toString();
            // String finalGroupByColumns = groupbycolumns.length() > 0 ? groupbycolumns.toString() : null;

            // Chagne the Key as that expected at destination. TODO: Check why do we need groupId in the key in abvoe code
            aggregationMap = updateAggregationMapKeys(aggregationMap);

            return aggregationMap;
        }
        // Values are the same. Just update the keys with a part of keys. There could be conflict.  TODO: check and avoid this function
        public Map<String, AggregationData> updateAggregationMapKeys(Map<String, AggregationData> aggregationMap) {
            Map<String, AggregationData> newMap = new HashMap<>();
            for (Map.Entry<String, AggregationData> entry : aggregationMap.entrySet()) {
                AggregationData data = entry.getValue();
                String newKey = data.getDlId() + "-" + data.getJobId();
                newMap.put(newKey, data);
            }
            return newMap;
        }

        // Value 6; filterGroupBy - filter Step 2b
        public Map<String, Map<String, Object>> executeFilterGroupByInfoQuery(Connection conn, String dlId, String jobId, String settingPosition) throws SQLException {
            String query = "SELECT a.DL_Id, a.Job_Id, a.Filter_Id, b.Filter_Condition " +
                           "FROM ELT_DL_FilterGroupBy_Info a " +
                           "INNER JOIN ELT_DL_Filter_Info b " +
                           "ON a.Filter_Id = b.Filter_Id " +
                           "AND a.Settings_Position = ? " +
                           "AND a.DL_Id = ? " +
                           "AND a.Job_Id = ?";
        
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.setString(1, settingPosition);
            pstmt.setString(2, dlId);
            pstmt.setString(3, jobId);
        
            ResultSet rs = pstmt.executeQuery();
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            while (rs.next()) {
                String dlIdValue = rs.getString("DL_Id");
                String jobIdValue = rs.getString("Job_Id");
                String filterId = rs.getString("Filter_Id");
                String filterCondition = rs.getString("Filter_Condition");
        
                String key = dlIdValue + "-" + jobIdValue;
        
                Map<String, Object> valueMap = new HashMap<>();
                valueMap.put("Filter_Id", filterId);
                valueMap.put("Filter_Condition", filterCondition);
        
                resultMap.put(key, valueMap);
            }
            return resultMap;
        }
     
        
        private String getValueFileName() {
            String suffix = getTimeStamp();
            String valueFileName = clientId + dlName + "_Value_File_" + suffix + ".values.properties";
            System.out.println(valueFileName);
            return valueFileName;
        }

        class JoinAggregationData {
            String dlId;
            String jobId;
            StringBuilder joinTable;
            StringBuilder joinColumn;
   
            public String getJoinTable() {
                return joinTable.toString();
            }

            public String getJoinColumn() {
                return joinColumn.toString();
            }

            public JoinAggregationData(String dlId, String jobId) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.joinTable = new StringBuilder();
                this.joinColumn = new StringBuilder();
            }
        }

        
        class SourceAggregationData {
            String dlId;
            String jobId;
            String Table_Name;
            String Table_Name_Alias;

            String Final_Table_Name;

            public String getDlId() {
                return dlId;
            }
            public String getJobId() {
                return jobId;
            }
            public String getTableName() {
                return Table_Name;
            }
            public String getTableNameAlias() {
                return Table_Name_Alias;
            }
            public String getFinalTableName() {
                return Final_Table_Name;
            }
            StringBuilder Column_Name;
            StringBuilder Column_Name_Alias;
            StringBuilder Column_Name_with_Alias;
            public String getColumnNameWithAlias() {
                return Column_Name_with_Alias.toString();
            }
            StringBuilder Data_Type;
            String Flow;            
            public String getFlow() {
                return Flow;
            }
            Long Filter_Id;
            Long Group_By_Id;
            
            public Long getGroupById() {
                return Group_By_Id;
            }
            public SourceAggregationData(String dlId, String jobId, String Table_Name, String Table_Name_Alias) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.Table_Name = Table_Name;
                this.Table_Name_Alias = Table_Name_Alias;

                this.Column_Name = new StringBuilder();
                this.Column_Name_Alias = new StringBuilder();
                this.Column_Name_with_Alias = new StringBuilder();
                this.Data_Type = new StringBuilder();
            }
        }

        class SourceGroupByAggregationData {
            Long dlId;
            Long jobId;
            String Table_Name;
            String Table_Name_Alias;

            StringBuilder Aggregation_Columns;
            StringBuilder Group_By_Columns;
            String Flow;
            StringBuilder Aggregation_Columns_with_Alias;
            StringBuilder Group_By_Columns_Alias;            
            StringBuilder Group_By_Columns_With_Alias;
            StringBuilder Having_Agg_Col_Alias;
            StringBuilder Having_Grpby_Columns;
            StringBuilder Having_Grpby_Columns_Alias;
            StringBuilder Having_Grpby_Columns_with_Alias;

            public StringBuilder getAggregation_Columns() {
                return Aggregation_Columns;
            }

            public String getGroupByColumns() {
                return Group_By_Columns.toString();
            }

            public String getFlow() {
                return Flow;
            }

            public String getAggregationColumnsWithAlias() {
                return Aggregation_Columns_with_Alias.toString();
            }

            public String getGroupByColumnsAlias() {
                return Group_By_Columns_Alias.toString();
            }

            public String getGroupByColumnsWithAlias() {
                return Group_By_Columns_With_Alias.toString();
            }

            public String getHavingAggColAlias() {
                return Having_Agg_Col_Alias.toString();
            }

            public String getHavingGroupByColumns() {
                return Having_Grpby_Columns.toString();
            }

            public String getHavingGroupbyColumnsAlias() {
                return Having_Grpby_Columns_Alias.toString();
            }

            public String getHavingGroupByColumnsWithAlias() {
                return Having_Grpby_Columns_with_Alias.toString();
            }

            public SourceGroupByAggregationData(Long dlId, Long jobId, String Table_Name, String Table_Name_Alias) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.Table_Name = Table_Name;
                this.Table_Name_Alias = Table_Name_Alias;

                this.Aggregation_Columns = new StringBuilder();
                this.Group_By_Columns = new StringBuilder();
                // this.Flow = new StringBuilder();
                this.Aggregation_Columns_with_Alias = new StringBuilder();
                this.Group_By_Columns_Alias = new StringBuilder();
                this.Group_By_Columns_With_Alias = new StringBuilder();
                this.Having_Agg_Col_Alias = new StringBuilder();
                this.Having_Grpby_Columns = new StringBuilder();
                this.Having_Grpby_Columns_Alias = new StringBuilder();
                this.Having_Grpby_Columns_with_Alias = new StringBuilder();
            }
        }

        class SourceFilterByAggregationData {
            String dlId; // TODO they are LONG
            String jobId;
            String Table_Name;
            String Table_Name_Alias;

            Long Filter_Id;
            String Flow;
            String Filter_Condition;
            String Filter_Columns;

            public String getFilterId() {
                return Filter_Id.toString();
            }

            public String getFlow() {
                return Flow;
            }

            public String getFilterCondition() {
                return Filter_Condition;
            }

            public String getFilterColumns() {
                return Filter_Columns;
            }

            public SourceFilterByAggregationData(String dlId, String jobId, String Table_Name, String Table_Name_Alias) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.Table_Name = Table_Name;
                this.Table_Name_Alias = Table_Name_Alias;

                // this.Filter_Id = new StringBuilder();
                // this.Flow = new StringBuilder();
                // this.Filter_Condition = new StringBuilder();
                // this.Filter_Columns = new StringBuilder();
            }
        }

        class SinkAggregationData {
            String dlId;
            String jobId;
            StringBuilder Table_Name;
            StringBuilder Column_Name_Alias;
            StringBuilder Constraints;
            StringBuilder Source_Name;
            StringBuilder Data_Type;
            

            StringBuilder Cleansing_Value;
            StringBuilder cleansing_Validations;
            StringBuilder date_formats;

            public String getDataType() {
                return Data_Type.toString();
            }
            public String getColumnNameAlias() {
                return Column_Name_Alias.toString();
            }
            public String getCleansingValue() {
                return Cleansing_Value.toString();
            }
            public String getCleansingValidations() {
                return cleansing_Validations.toString();
            }
            public String getDateFormats() {
                return date_formats.toString();
            }
            public SinkAggregationData(String dlId, String jobId) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.Table_Name = new StringBuilder();
                this.Column_Name_Alias = new StringBuilder();
                this.Constraints = new StringBuilder();
                this.Source_Name = new StringBuilder();
                this.Data_Type = new StringBuilder();
                this.Cleansing_Value = new StringBuilder();
                this.cleansing_Validations = new StringBuilder();
                this.date_formats = new StringBuilder();
            }
        }

        class RecoercingAggregationData {
            String dlId;
            String jobId;
            StringBuilder Table_Name;
            StringBuilder Constraints;
            StringBuilder Source_Name;
            StringBuilder Data_Type;
            StringBuilder Column_Name_Alias;

            public String getColumnNameAlias() {
                return Column_Name_Alias.toString();
            }
            StringBuilder recoerce_to_format;
            public String getRecoerceToFormat() {
                return recoerce_to_format.toString();
            }

            StringBuilder recoerce_to_type;           
            public String getRecoerceToType() {
                return recoerce_to_type.toString();
            }

            StringBuilder recoerce_decimal_precisions;
            public String getRecoerceDecimalPrecisions() {
                return recoerce_decimal_precisions.toString();
            }

            StringBuilder recoerce_decimal_scales;
            public String getRecoerceDecimalScales() {
                return recoerce_decimal_scales.toString();
            }

            public RecoercingAggregationData(String dlId, String jobId) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.Table_Name = new StringBuilder();
                this.Column_Name_Alias = new StringBuilder();
                this.Constraints = new StringBuilder();
                this.Source_Name = new StringBuilder();
                this.Data_Type = new StringBuilder();
                this.recoerce_to_format = new StringBuilder();
                this.recoerce_to_type = new StringBuilder();
                this.recoerce_decimal_precisions = new StringBuilder();
                this.recoerce_decimal_scales = new StringBuilder();
            }
        }

        class AggregationData {
            String dlId;
            String jobId;
            String groupId; // = resultSet.getString("Group_Id");
            StringBuilder allcolumn;
            StringBuilder groupbycolumns;
            
            public AggregationData(String dlId, String jobId, String groupId) {
                this.dlId = dlId;
                this.jobId = jobId;
                this.groupId = groupId;
                this.allcolumn = new StringBuilder();
                this.groupbycolumns = new StringBuilder();
            }

            //TODO: Perhaps not in use
            public AggregationData() {
                allcolumn = new StringBuilder();
                groupbycolumns = new StringBuilder();
            }

            public String getDlId() {
                return dlId;
            }

            public String getJobId() {
                return jobId;
            }

            public StringBuilder getAllcolumn() {
                return allcolumn;
            }

            public StringBuilder getGroupbycolumns() {
                return groupbycolumns;
            }

            public String getGroupId() {
                return groupId;
            }
        }

        private String componentNullReplacementValue(String component) {
            String finalEmptyvalue = "";
            try {
                String emptyValue = getValueNamesFromJobPropertiesInfo(conn, component); // EmptyValue
                // Constraints is PK, SK
                List<Map<String, Object>> replacementInfoPKList = fetchReplacementIncludeConstraintsInfo(conn, String.valueOf(jobId), String.valueOf(dlId));
                Map<String, Map<String, Object>> pkCleansingConversionMap = getDataTypeConversionsForPKCleansinghValue(conn); // 28
                Map<String, SinkAggregationData> pkCleansingDataMap = getPKCleansingInfo(replacementInfoPKList, pkCleansingConversionMap);

                // Constraints is not PK, SK
                List<Map<String, Object>> replacementInfoList = fetchReplacementExcludeConstraintsInfo(conn, String.valueOf(jobId), String.valueOf(dlId));
                Map<String, Map<String, Object>> cleansingConversionMap = getDataTypeConversionsForCleansinghValue(conn); // 0/7
                Map<String, SinkAggregationData> cleansingDataMap = getCleansingInfo(replacementInfoList, cleansingConversionMap);

                Map<String, String> cleansingData = consolidateCleansingInfo(pkCleansingDataMap, cleansingDataMap);
                String columnNameAlias = cleansingData.get("Column_Name_Alias");
                String cleansingValue = cleansingData.get("Cleansing_Value");
                String cleansingValidations = cleansingData.get("cleansing_Validations");
                String dateFormat = cleansingData.get("date_formats");

                String cleansingValidation = emptyValue.replace("${cleansing.validation}", "cleansing.validation=" + cleansingValidations);
                String cleansingValues = cleansingValidation.replace("${cleansing.values}", "cleansing.values=" + cleansingValue);
                String dateFormats = cleansingValues.replace("${date.formats}", "date.formats=" + dateFormat);
                String cleansingFields = dateFormats.replace("${cleansing.fields}", "cleansing.fields=" + columnNameAlias);
                finalEmptyvalue = cleansingFields;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return finalEmptyvalue;
        }

        public Map<String, String> consolidateCleansingInfo(Map<String, SinkAggregationData> pkCleansingDataMap, Map<String, SinkAggregationData> cleansingDataMap) {
            Map.Entry<String, SinkAggregationData> entry = pkCleansingDataMap.entrySet().iterator().next();
            SinkAggregationData value1 = entry.getValue();
            String key1 = entry.getKey();

            Map.Entry<String, SinkAggregationData> entry2 = cleansingDataMap.entrySet().iterator().next();
            SinkAggregationData value2 = entry2.getValue();
            String key2 = entry2.getKey();

            if (!key1.equals(key2)) {
                System.out.println("Some error in data.");
                return null;
            }

            Map<String, String> out = new HashMap<>();
            // Merge Column_Name_Alias
            String columnNameAlias = (value2.getColumnNameAlias() == null) 
            ? value1.getColumnNameAlias() 
            : value1.getColumnNameAlias() + ", " + value2.getColumnNameAlias();

            // Merge Cleansing_Value
            String cleansingValue = (value2.getCleansingValue() == null) 
            ? value1.getCleansingValue() 
            : value1.getCleansingValue() + ", " + value2.getCleansingValue();

            // Merge cleansing_Validations
            String cleansingValidations = (value2.getCleansingValidations() == null) 
            ? value1.getCleansingValidations() 
            : value1.getCleansingValidations() + ", " + value2.getCleansingValidations();

            // Merge date_formats
            String dateFormats = (value2.getDateFormats() == null) 
            ? value1.getDateFormats() 
            : value1.getDateFormats() + ", " + value2.getDateFormats();

            // Setting the values in Var (or any appropriate data structure)
            out.put("Column_Name_Alias", columnNameAlias);
            out.put("Cleansing_Value", cleansingValue);
            out.put("cleansing_Validations", cleansingValidations);
            out.put("date_formats" , dateFormats);

            return out;
        }

        public Map<String, SinkAggregationData> getCleansingInfo(List<Map<String, Object>> replacementInfoList,
                Map<String, Map<String, Object>> cleansingConversionMap) {
            Map<String, SinkAggregationData> aggregationMap = new HashMap<>(); // Sink contain the same data

            for (Map<String, Object> replacementInfo : replacementInfoList) {
                String dataType = (String) replacementInfo.get("Data_Type");

                // get the corresponding entry in cleansingConversionMap
                Map<String, Object> cleansingInfo = cleansingConversionMap.get(dataType);
                
                // Inner join
                if (cleansingInfo != null) {
                    String dlIdResult = (String) replacementInfo.get("DL_Id");
                    String jobIdResult = (String) replacementInfo.get("Job_Id");
                    String keyMap = dlIdResult + "-" + jobIdResult;
                    
                    String columnNameAlias = (String) replacementInfo.get("DL_Column_Names");
                    String dataTypeInfo = (String) replacementInfo.get("Data_Type");

                    String cleansingValidations = (columnNameAlias == null) ? null : "EMPTY";
                    String cleansingValue = (String) cleansingInfo.get("Cleansing_Value");

                    String dateFormats = (dataTypeInfo.toLowerCase().contains("date")) ? "yyyy-MM-dd" : ""; // toLowerCase()
                    SinkAggregationData data = aggregationMap.getOrDefault(keyMap, new SinkAggregationData(dlIdResult, jobIdResult));

                    data.Table_Name.append(data.Table_Name.length() > 0 ? ", " : "").append(replacementInfo.get("Table_Name"));
                    data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(replacementInfo.get("DL_Column_Names")); // DL_Column_Names in place of Column_Name_Alias
                    data.Constraints.append(data.Constraints.length() > 0 ? ", " : "").append(replacementInfo.get("Constraints"));
                    data.Source_Name.append(data.Source_Name.length() > 0 ? ", " : "").append(replacementInfo.get("Source_Name"));
                    data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(replacementInfo.get("Data_Type"));
                    // Specific fields
                    data.Cleansing_Value.append(data.Cleansing_Value.length() > 0 ? ", " : "").append(cleansingValue);  
                    data.cleansing_Validations.append(data.cleansing_Validations.length() > 0 ? ", " : "").append(cleansingValidations);
                    if (data.date_formats.length() == 0 && dateFormats.equals("")) {
                        data.date_formats.append(", ");
                    } else {
                        data.date_formats.append(data.date_formats.length() > 0 ? ", " : "").append(dateFormats);
                    }

                    aggregationMap.put(keyMap, data);
                }
            }
            return aggregationMap;
        }

        public Map<String, SinkAggregationData> getPKCleansingInfo(List<Map<String, Object>> replacementInfoList,
                Map<String, Map<String, Object>> pkCleansingConversionMap) {
            Map<String, SinkAggregationData> aggregationMap = new HashMap<>(); // Sink contain the same data

            for (Map<String, Object> replacementInfo : replacementInfoList) {
                String dataType = (String) replacementInfo.get("Data_Type");

                // get the corresponding entry in cleansingConversionMap
                Map<String, Object> pkCleansingInfo = pkCleansingConversionMap.get(dataType);
                
                // Inner join
                if (pkCleansingInfo != null) {
                    String dlIdResult = (String) replacementInfo.get("DL_Id");
                    String jobIdResult = (String) replacementInfo.get("Job_Id");
                    String keyMap = dlIdResult + "-" + jobIdResult;
                    
                    String columnNameAlias = (String) replacementInfo.get("DL_Column_Names");
                    String dataTypeInfo = (String) replacementInfo.get("Data_Type");

                    String cleansingValidations = (columnNameAlias == null) ? null : "EMPTY";
                    String pkCleansingValue = (String) pkCleansingInfo.get("PK_Cleansing_Value");

                    String dateFormats = (dataTypeInfo.toLowerCase().contains("date")) ? "yyyy-MM-dd" : ""; // toLowerCase()
                    SinkAggregationData data = aggregationMap.getOrDefault(keyMap, new SinkAggregationData(dlIdResult, jobIdResult));

                    data.Table_Name.append(data.Table_Name.length() > 0 ? ", " : "").append(replacementInfo.get("Table_Name"));
                    data.Column_Name_Alias.append(data.Column_Name_Alias.length() > 0 ? ", " : "").append(replacementInfo.get("DL_Column_Names")); // DL_Column_Names in place of Column_Name_Alias
                    data.Constraints.append(data.Constraints.length() > 0 ? ", " : "").append(replacementInfo.get("Constraints"));
                    data.Source_Name.append(data.Source_Name.length() > 0 ? ", " : "").append(replacementInfo.get("Source_Name"));
                    data.Data_Type.append(data.Data_Type.length() > 0 ? ", " : "").append(replacementInfo.get("Data_Type"));
                    // Specific fields
                    data.Cleansing_Value.append(data.Cleansing_Value.length() > 0 ? ", " : "").append(pkCleansingValue);  
                    data.cleansing_Validations.append(data.cleansing_Validations.length() > 0 ? ", " : "").append(cleansingValidations);
                    if (data.date_formats.length() == 0 && dateFormats.equals("")) {
                        data.date_formats.append(", ");
                    } else {
                        data.date_formats.append(data.date_formats.length() > 0 ? ", " : "").append(dateFormats);
                    }

                    aggregationMap.put(keyMap, data);
                }
            }
            return aggregationMap;
        }

        public List<Map<String, Object>> fetchReplacementIncludeConstraintsInfo(Connection conn, String jobId, String dlId) throws SQLException {
           //String query = SQLQueries.SELECT_REPLACEMENT_MAPPING_INFO;
            String query = sqlQueries.getReplacementIncludeConstraintsQuery(dlId, jobId);

            try (PreparedStatement ps = conn.prepareStatement(query)) {
                // ps.setString(1, jobId);
                // ps.setString(2, dlId);
                // ps.setString(3, dlId);
                // ps.setString(4, jobId);
                // ps.setString(1, dlId);
                // ps.setString(2, dlId);
                // ps.setString(3, jobId);
        
                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String, Object>> results = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Table_Name", rs.getString("Table_Name"));
                        row.put("DL_Column_Names", rs.getString("DL_Column_Names"));
                        row.put("Constraints", rs.getString("Constraints"));
                        row.put("Source_Name", rs.getString("Source_Name"));
                        row.put("Data_Type", rs.getString("Data_Type"));
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("Job_Id", rs.getString("Job_Id"));
                        results.add(row);
                    }
                    return results;
                }
            }
        }

        public List<Map<String, Object>> fetchReplacementExcludeConstraintsInfo(Connection conn, String jobId, String dlId) throws SQLException {
             String query = sqlQueries.getReplacementExcludeConstraintsQuery(dlId, jobId);
 
             try (PreparedStatement ps = conn.prepareStatement(query)) {
                 try (ResultSet rs = ps.executeQuery()) {
                     List<Map<String, Object>> results = new ArrayList<>();
                     while (rs.next()) {
                         Map<String, Object> row = new HashMap<>();
                         row.put("Table_Name", rs.getString("Table_Name"));
                         row.put("DL_Column_Names", rs.getString("DL_Column_Names"));
                         row.put("Constraints", rs.getString("Constraints"));
                         row.put("Source_Name", rs.getString("Source_Name"));
                         row.put("Data_Type", rs.getString("Data_Type"));
                         row.put("DL_Id", rs.getString("DL_Id"));
                         row.put("Job_Id", rs.getString("Job_Id"));
                         results.add(row);
                     }
                     return results;
                 }
             }
         }

        public Map<String, Map<String, Object>> getDataTypeConversionsForPKCleansinghValue(Connection conn) throws SQLException {
            String query = "SELECT " +
                           "`ELT_Datatype_Conversions`.`Id`, " +
                           "`ELT_Datatype_Conversions`.`Source_Data_Type`, " +
                           "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS Data_Type, " +
                           "`ELT_Datatype_Conversions`.`Java_Data_Type`, " +
                           "PK_Cleansing_Value " +
                           "FROM `ELT_Datatype_Conversions`";
    
            //List<Map<String, Object>> results = new ArrayList<>();
            Map<String, Map<String, Object>> conversionData = new HashMap<>();
            try (PreparedStatement ps = conn.prepareStatement(query);
                ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    String key = (String) rs.getObject("Data_Type");
                    row.put("Id", rs.getObject("Id"));
                    row.put("Source_Data_Type", rs.getObject("Source_Data_Type"));
                    row.put("IL_Data_Type", rs.getObject("Data_Type"));
                    row.put("Java_Data_Type", rs.getObject("Java_Data_Type"));
                    row.put("PK_Cleansing_Value", rs.getObject("PK_Cleansing_Value"));
                    //results.add(row);
                    conversionData.put(key, row);
                }
            }
            return conversionData;
        }
        
        public Map<String, Map<String, Object>> getDataTypeConversionsForCleansinghValue(Connection conn) throws SQLException {    
            String query = "SELECT `ELT_Datatype_Conversions`.`Id`, " +
                           "`ELT_Datatype_Conversions`.`Source_Data_Type`, " +
                           "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS `UI_Data_Type`, " +
                           "`ELT_Datatype_Conversions`.`Java_Data_Type`, " +
                           "Cleansing_Value " +
                           "FROM `ELT_Datatype_Conversions` " +
                           "WHERE Cleansing_Value != ''";

            //List<Map<String, Object>> results = new ArrayList<>();
            Map<String, Map<String, Object>> conversionData = new HashMap<>();

            try (PreparedStatement ps = conn.prepareStatement(query);
                ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    String key = (String) rs.getObject("UI_Data_Type");
                    row.put("Id", rs.getObject("Id"));
                    row.put("Source_Data_Type", rs.getObject("Source_Data_Type"));
                    row.put("IL_Data_Type", rs.getObject("UI_Data_Type"));
                    row.put("Java_Data_Type", rs.getObject("Java_Data_Type"));
                    row.put("Cleansing_Value", rs.getObject("Cleansing_Value"));
                    //results.add(row);
                    conversionData.put(key, row);
                }

            }
            return conversionData;
        }

        // Value NullReplacement - Method to perform in-memory inner join and process data
        public List<Map<String, Object>> processJoinedData(
                List<Map<String, Object>> replacementMappingInfo, 
                List<Map<String, Object>> dataTypeConversions) {

            // Convert the list of maps from dataTypeConversions into a lookup map for fast access
            Map<String, Map<String, Object>> dataTypeMap = dataTypeConversions.stream()
                .collect(Collectors.toMap(
                    map -> (String) map.get("Data_Type"),
                    map -> map));                         // Value: Map with Data_Type and other columns

            List<Map<String, Object>> joinedResults = new ArrayList<>();

            for (Map<String, Object> replacementMap : replacementMappingInfo) {
                String dataType = (String) replacementMap.get("Data_Type");
                if (dataType != null && dataTypeMap.containsKey(dataType)) {
                    Map<String, Object> dataTypeMapEntry = dataTypeMap.get(dataType);
                    Map<String, Object> joinedMap = new HashMap<>(replacementMap);
                    joinedMap.putAll(dataTypeMapEntry);

                    String columnNameAlias = (String) joinedMap.get("Column_Name_Alias");
                    String dataTypeContains = (String) joinedMap.get("Data_Type");
                    
                    // New columns
                    joinedMap.put("cleansing_Validation", columnNameAlias == null ? "EMPTY" : columnNameAlias);
                    joinedMap.put("date_formats", dataTypeContains != null && dataTypeContains.contains("date") ? "yyyy-MM-dd" : "");

                    joinedResults.add(joinedMap);
                }
            }

            return joinedResults;
        }

        // value General function
        public String getValueNamesFromJobPropertiesInfo(Connection conn, String component) {
            String query = SQLQueries.SELECT_VALUE_NAMES_FROM_ELT_JOB_PROPERTIES_INFO;
            StringBuilder script = new StringBuilder();
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, component);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String valueName = rs.getString("Value_Name");
                        script.append(valueName).append("\n");
                    }
                }
            }  catch (SQLException e) {
                e.printStackTrace();
                return "";
            }    
            return script.toString();
        }
        
        private boolean insertIntoEltDlValuesProperties(Connection conn, Map<String, String> rowDetails) {
            String insertSql = "INSERT INTO ELT_DL_VALUES_PROPERTIES (DL_Id, Job_Id, DL_Name, DL_Table_Name, value_file_name, Active_Flag) " +
                               "VALUES (?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                insertPs.setString(1, rowDetails.get("DL_Id"));
                insertPs.setString(2, rowDetails.get("Job_Id"));
                insertPs.setString(3, rowDetails.get("DL_Name"));
                insertPs.setString(4, rowDetails.get("DL_Table_Name"));
                insertPs.setString(5, rowDetails.get("value_file_name"));
                insertPs.setBoolean(6, rowDetails.get("DL_Active_Flag").equals("1"));

                insertPs.executeUpdate();
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public Map<String, String> selectActiveEltDlTableInfo(Connection conn, long dlId) {
        String selectSql = SQLQueries.SELECT_ACTIVE_ELT_DL_TABLE_INFO;

        Map<String, String> rowDetails = new HashMap<>();

        try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
            selectPs.setLong(1, dlId);
            try (ResultSet rs = selectPs.executeQuery()) {
                if (rs.next()) {
                    rowDetails.put("DL_Id", rs.getString("DL_Id"));
                    rowDetails.put("DL_Name", rs.getString("DL_Name"));
                    rowDetails.put("DL_Table_Name", rs.getString("DL_Table_Name"));
                    rowDetails.put("DL_Version", rs.getString("DL_Version"));
                    rowDetails.put("DL_Active_Flag", rs.getString("DL_Active_Flag"));
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return rowDetails;
    }

    public void printComponent(String currentComponent, String previousComponent) {
        System.out.println("CurrentComponent: " + currentComponent + ", PreviousComponent: " + previousComponent);   
    }

    public void printScript(String currentComponent, String script) {
        System.out.println("Script: " + currentComponent + "\n" + script);   
    }

    public static void writeToFile(String data, String fileName) {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(data);
            System.out.println("Data successfully written to " + fileName);
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }

    public String getCurrentDateFormatted() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
        String formattedDateTime = now.format(formatter);
        System.out.println("Formatted Date and Time: " + formattedDateTime);
        formattedDateTime = formattedDateTime.replace(" ", "_");

        // Remove dashes, colons and period
        formattedDateTime = formattedDateTime.replace("-", "").replace(":", "");
        formattedDateTime = formattedDateTime.replace(".", "");
        System.out.println("Formatted Date and Time: " + formattedDateTime);
        return formattedDateTime;
    }

    public class DataMartAlterScriptGenerator {
        public DataMartAlterScriptGenerator() {
         }

        public Status generateAlterScript() {
            boolean status = false;
            System.out.println("Generating alter script for DL_ID: " + dlId);
            // Step 1:
            status = updateActiveFlag(conn, String.valueOf(dlId));
            // Format of the dateTime is supposed to be "yyyy-MM-dd HH:mm:ss", Hence TimeStamp is used.
            Timestamp dateTime = getMaxUpdatedDate(conn, String.valueOf(dlId));
            boolean dlIdExists = checkDLIdExists(conn, String.valueOf(dlId));
            //TBD: Note: Below check moved earlier to be reused elsewhere
            String TargetDB = ""; // Input Param
            boolean tableExists = doesTableExist(conn, dlName, TargetDB);
            // Step 2:
            if (dlIdExists) {
                // call Alter delete funcitons
                String dlName = ""; // Input Param
                if (tableExists) {
                    System.out.println("Table Exists. Execute Alter Statement.");
                    status = fetchUniqueMappingInfoAndInsertIntoAlterScriptInfo(conn, String.valueOf(dlId));
                } else {
                    System.out.println("Table Doesn't Exists. Proceed with Create Statement.");
                    // TBD: This case is not specified properly. Is it redundant?
                }
            }
            // Step 3:
            boolean hasRecentUpdateForDLId = checkDLIdExistsWithUpdatedDate(conn, String.valueOf(dlId), dateTime);
            if (hasRecentUpdateForDLId) {
                // call Alter Services
                getPKColumnNames(conn, String.valueOf(dlId));
                buildChangeColumnNotNullQuery(conn, String.valueOf(dlId), dlName);
                buildChangeColumnNullQuery(conn, String.valueOf(dlId), dlName);
            }

            // Step 4:
            // TBD: This function must be changed to set either value 0,1
            status = updateActiveFlag(conn, String.valueOf(dlId));
            // Step 5:
            // TBD: Table Exists or not (tableExists)
            if (tableExists) { // TBD: any other check?
                System.out.println("Table Exists. Executing Alter Statement.");
            } else {
                System.out.println("Table Doesn't Exists. Proceeding with Create Statement");
            }
            // Step 6:
            //Delete columns. Call to get Alter script delete funcitons/script But without inser function (need refactor)
            // fetchUniqueMappingInfoAndInsertIntoAlterScriptInfo()
            // TBD: Can it be reused from above
            // TBD: it's date or time? refer to database. Though, script shows timestamp format.
            // Step 7:
            Timestamp maxUpdatedDate = getMaxUpdatedDate(conn, String.valueOf(dlId)); // TBD: isn't dlId and dl_name are 1-1 mapped. THis call is with dlName

            // Step 8:
            buildAndExecuteCompleteAlterScript(conn, String.valueOf(dlId), maxUpdatedDate);

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
    
    public String getPKColumnNames(Connection conn, String dlId) {
        String query = SQLQueries.JOIN_ELT_DL_MAPPING_INFO_TABLES_FOR_PK_COLUMNS;
        StringBuilder pkColumnNamesBuilder = new StringBuilder();
        StringBuilder lookupColumnNamesBuilder = new StringBuilder();
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, dlId);
            try (ResultSet rs = pstmt.executeQuery()) {
                boolean firstColumn = true;
                boolean firstLookupColumn = true;
                while (rs.next()) {
                    if (!firstColumn) {
                        pkColumnNamesBuilder.append(", ");
                    }
                    String columnName = rs.getString("DL_Column_Names");
                    pkColumnNamesBuilder.append("`").append(columnName).append("`");
                    firstColumn = false;

                    String lookupColumnName = rs.getString("lookup_column_names");
                    if (lookupColumnName != null) {
                        if (!firstLookupColumn) {
                            pkColumnNamesBuilder.append(", ");
                        }
                        lookupColumnNamesBuilder.append("`").append(lookupColumnName).append("`");
                        firstLookupColumn = false;
                    }
                }
            }
        String changeFlag = "N";
        if ("".equals(lookupColumnNamesBuilder)) {
            changeFlag = "Y";
        }
        System.out.println("Change Flag: " + changeFlag);
        System.out.println(pkColumnNamesBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pkColumnNamesBuilder.toString();
    }
    
    // Function to build Change Column Non Null query
    public String buildChangeColumnNotNullQuery(Connection conn, String dlId, String dlName) {
        String query = SQLQueries.JOIN_OUTER_ELT_DL_MAPPING_INFO_SAVED_AND_INFO;

        StringBuilder combinedChangeColumnDefBuilder = new StringBuilder();
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            // Set the DL_Id parameter in the query
            stmt.setString(1, dlId);
            stmt.setString(2, dlName);

            // Execute the query
            try (ResultSet rs = stmt.executeQuery()) {
                // Process the result set
                while (rs.next()) {
                    String savedDlId = rs.getString("DL_Id");
                    String savedColumnName = rs.getString("DL_Column_Names");
                    //String lookupColumnNames = rs.getString("lookup_column_names");
                    String lookupDataTypes = rs.getString("lookup_data_types");
                    String savedConstraints = rs.getString("saved_constraints");
                    String lookupConstraints = rs.getString("lookup_constraints");
                    // Form Change Column Not Null Definition and append to combined Definition
                    String changeColumnDefinition = sqlChangeColumnNotNullDefinition(savedColumnName, lookupDataTypes,
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
        return combinedChangeColumnDefBuilder.toString();
    }

    // Function to build Change Column Null query
    public String buildChangeColumnNullQuery(Connection conn, String dlId, String dlName) {
        String query = SQLQueries.JOIN_OUTER_ELT_DL_MAPPING_INFO_AS_MAIN_AND_SAVED_AS_LOOKUP;

        StringBuilder combinedChangeColumnDefBuilder = new StringBuilder();
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            // Set the DL_Id parameter in the query
            stmt.setString(1, dlId);
            stmt.setString(2, dlName);

            // Execute the query
            try (ResultSet rs = stmt.executeQuery()) {
                // Process the result set
                while (rs.next()) {
                    String mainDlId = rs.getString("DL_Id");
                    String columnName = rs.getString("DL_Column_Names");
                    //String lookupColumnNames = rs.getString("lookup_column_names");
                    String lookupDataTypes = rs.getString("lookup_data_types");
                    String mainConstraints = rs.getString("main_constraints");
                    String lookupConstraints = rs.getString("lookup_constraints");
                    // Form Change Column Null Definition and append to combined Definition
                    String changeColumnDefinition = sqlChangeColumnNullDefinition(columnName, lookupDataTypes,
                    mainConstraints, lookupConstraints);
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
        return combinedChangeColumnDefBuilder.toString();
    }

    private String sqlChangeColumnNotNullDefinition(String columnName, String lookupDataTypes, String savedConstraints,
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
        } // TBD: Check the null case too, it may retuen empty string?
        return finalScriptBuilder.toString();
    }

    private String sqlChangeColumnNullDefinition(String columnName, String lookupDataTypes, String mainConstraints,
            String lookupConstraints) {
        final String CHANGE_COLUMN_DEF_START = "Change column ";
        final String NOT_NULL = " NULL ";
        StringBuilder finalScriptBuilder = new StringBuilder();
        if (lookupDataTypes != null) {
            if ("pk".equalsIgnoreCase(mainConstraints) && "".equalsIgnoreCase(lookupConstraints)) {
                finalScriptBuilder.setLength(0); // ensure start afresh
                finalScriptBuilder.append(CHANGE_COLUMN_DEF_START).append(" ")
                        .append('`').append(columnName).append('`').append(" ")
                        .append('`').append(columnName).append('`').append(" ")
                        .append(NOT_NULL);
            }
        } // TBD: Check the null case too, it may retuen empty string?
        return finalScriptBuilder.toString();
    }

    public boolean buildAndExecuteCompleteAlterScript(Connection conn, String dlId, Timestamp maxUpdatedDate) {
        String query = SQLQueries.JOIN_ELT_DL_MAPPING_INFO_TABLES_RECENTLY_UPDATED;
    
        StringBuilder finalAlterScriptBuilder = new StringBuilder();
        StringBuilder combinedAlterScriptDefBuilder = new StringBuilder();

        StringBuilder sb = new StringBuilder();
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, dlId);
            pstmt.setTimestamp(2, maxUpdatedDate); // TBD: date vs Timestamp
    
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String savedDLName = rs.getString("DL_Name");
                    String savedColumnNames = rs.getString("DL_Column_Names");
                    String savedDataTypes = rs.getString("saved_data_types");
                    String savedConstraints = rs.getString("saved_constraints");

                    String lookupDLName = rs.getString("lookup_DL_Name");
                    String lookupColumnNames = rs.getString("lookup_column_names"); 
                    String lookupDataTypes = rs.getString("lookup_data_types");  
                    String lookupConstraints = rs.getString("lookup_constraints");

                    // Change bit to Tinybit DL_dataTypes
                    if (savedDataTypes.contains("bit")) {
                        savedDataTypes = "tinyint(1)";
                    }

                    sb.append("ADD COLUMN `").append(savedColumnNames).append("` ").append(lookupColumnNames);
                    // TBD: these consditions have to be studied
                    if ((!savedColumnNames.equals(lookupColumnNames) || !savedDataTypes.equals(lookupDataTypes) || savedConstraints.equals("PK"))) {
                        sb.append("CHANGE `").append(lookupColumnNames).append("` ").append(savedColumnNames).append("` ").append(savedDataTypes).append(" NOT NULL");
                    } else if (!savedColumnNames.equals(lookupColumnNames) || !savedDataTypes.equals(lookupDataTypes)) {
                        sb.append("CHANGE `").append(lookupColumnNames).append("` ").append(savedColumnNames).append("` ").append(savedDataTypes);
                    } else {
                        sb.append(""); // Do Nothing
                    }

                    if (!"".equals(combinedAlterScriptDefBuilder.toString())) {
                        if (combinedAlterScriptDefBuilder.length() > 0) {
                            combinedAlterScriptDefBuilder.append(", ");
                        }
                        combinedAlterScriptDefBuilder.append(sb);
                    }
                }
            // output from previous loop
            String finalAddingColumn = combinedAlterScriptDefBuilder.toString();
            // TBD: in exceptional cases, value must be ""
            String notNullFinalStatement = buildChangeColumnNotNullQuery(conn, dlId, dlName);
            finalAlterScriptBuilder.append(notNullFinalStatement).append(", ");
            String nullFinalStatement = buildChangeColumnNullQuery(conn, dlId, dlName);
            finalAlterScriptBuilder.append(nullFinalStatement).append(", ");
            // Check conditions if wither of them is "", null, empty or so on
            String finalStatement = notNullFinalStatement + nullFinalStatement;
            // Delete or Drop Column Script similar to above
            String deleteAlterString = ""; // TBD
            boolean dropFlag = true;
            if (deleteAlterString == null || "".equals(deleteAlterString)) {
                dropFlag = false;
            }

            StringBuilder sb2 = new StringBuilder();
            if (finalAddingColumn == null || "".equals(finalAddingColumn) || finalAddingColumn.isEmpty() ) {
                if (dropFlag == false) {
                    sb2.append("");
                } else // Drop Flag == true
                    sb2.append("ALTER TABLE `").append(dlName).append("` ").append(deleteAlterString);
            } else {
                if (dropFlag == false) {
                    sb2.append("ALTER TABLE `").append(dlName).append("`\n").append(finalStatement).append(finalAddingColumn);
                } else {
                    sb2.append("ALTER TABLE `").append(dlName).append("`\n").append(finalStatement).append(deleteAlterString).append(", ").append(finalAddingColumn);
                }
            }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }
    

    public class DataMartSavedScriptGenerator {
        public DataMartSavedScriptGenerator() {
         }

        public Status generateSavedScript() {
            System.out.println("Generating saved script for DL_ID: " + dlId);
            boolean status = false;
            status = deleteFromEltDlMappingInfo(conn, String.valueOf(dlId));
            // Copy the data from `ELT_DL_Mapping_Info_Saved` to `ELT_DL_Mapping_Info`
            status = insertMappingInfoFromSaved(conn, String.valueOf(dlId));

            return status? Status.SUCCESS : Status.FAILURE;
         }
    }

    // deleting records from ELT_DL_Mapping_Info
    public boolean deleteFromEltDlMappingInfo(Connection conn, String dlId) {
        String sql = SQLQueries.DELETE_FROM_ELT_DL_MAPPING_INFO;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dlId);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    // deleting records from ELT_DL_VALUES_PROPERTIES
    public boolean deleteFromEltDlValuesProperties(Connection conn, long dlId, long jobId) {
        String sql = SQLQueries.DELETE_FROM_ELT_DL_VALUES_PROPERTIES;        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, dlId);
            ps.setLong(2, jobId);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;            
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // deleting records from ELT_DL_CONFIG_PROPERTIES
    public boolean deleteFromEltDlConfigProperties(Connection conn, long dlId, long jobId) {
        String sql = SQLQueries.DELETE_FROM_ELT_DL_CONFIG_PROPERTIES;        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, dlId);
            ps.setLong(2, jobId);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    // Copy the data from `ELT_DL_Mapping_Info_Saved` to `ELT_DL_Mapping_Info`
    public boolean insertMappingInfoFromSaved(Connection conn, String dlId) {
        String sql = SQLQueries.INSERT_INTO_ELT_DL_MAPPING_INFO_FROM_SAVED;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dlId);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public class DataMartCreateScriptGenerator {
        public DataMartCreateScriptGenerator() {
        }

         public Status generateCreateScript() {
            try {
                Connection conn = DBHelper.getConnection(DataSourceType.MYSQL);
    
                if (conn != null) {
                    System.out.println("DB Connection established");
    
                    String tableName = "";
                    String id = "";
                    String columnName = "";
                    StringBuilder combinedColumnDefinitions = new StringBuilder();
                    StringBuilder primaryKeys = new StringBuilder();
                    StringBuilder secondaryKeys = new StringBuilder();
    
                    try (PreparedStatement pstmt = conn.prepareStatement(SQLQueries.SELECT_DISTINCT_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY)) {
                        pstmt.setLong(1, dlId);
                        try (ResultSet rs = pstmt.executeQuery()) {
    
                            String columnDefinition = "";
                            while (rs.next()) {
                                id = rs.getString("DL_Id");
                                tableName = rs.getString("Table_Name");
                                columnName = rs.getString("Column_Name_Alias");
                                String constraints = rs.getString("Constraints");
                                String dataTypes = rs.getString("DL_Data_Types");
                               
                                // Consolidated Primary keys
                                if ("PK".equalsIgnoreCase(constraints)) {
                                    if (primaryKeys.length() > 0) {
                                        primaryKeys.append(", ");
                                    }
                                    primaryKeys.append("`").append(columnName).append("`");
                                }
                                // Consolidated Secondary keys
                                else if ("SK".equalsIgnoreCase(constraints)) {
                                    if (secondaryKeys.length() > 0) {
                                        secondaryKeys.append(", ");
                                    }
                                    secondaryKeys.append("`").append(columnName).append("`");
                                }
    
                                // Form Column Definition and append to combined Column Definition
                                columnDefinition = buildColumnDefinition(columnName, constraints, dataTypes);
                                if (combinedColumnDefinitions.length() > 0) {
                                    combinedColumnDefinitions.append(", ");
                                }
                                combinedColumnDefinitions.append(columnDefinition);
                            }
                        }
     
                        String sqlCreateTableDefinition = "CREATE TABLE IF NOT EXISTS `"+ tableName +"` (";
                        String key = primaryKeys.toString() + ", " + secondaryKeys.toString();
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
                        // TODO
                        // System.out.println(finalCreateScript);
    
                        // Delete the existing rows, if any.
                        deleteFromEltDlCreateInfo(conn, tableName);

                        // Final step that is to put data into the table.
                        insertIntoEltDlCreateInfo(conn, id, tableName, finalCreateScript);
                    }
                    conn.close();
                } else {
                    System.out.println("Failed to make connection!");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return Status.SUCCESS;
        }
        // Build the column definition
        private String buildColumnDefinition(String columnName, String constraint, String dataType) {
            StringBuilder columnDefinitionBuilder = new StringBuilder();
            if ("text".equalsIgnoreCase(dataType) && "pk".equalsIgnoreCase(constraint)) {
                dataType = "varchar(150)";
            }
    
            columnDefinitionBuilder.append("\n")
                    .append("`").append(columnName).append("`")
                    .append(" ")
                    .append(dataType)
                    .append(dataType.startsWith("varchar") ? " COLLATE utf8mb4_0900_ai_ci " : " ");
    
            if ("pk".equalsIgnoreCase(constraint)) {
                columnDefinitionBuilder.append(" NOT NULL DEFAULT ");
                columnDefinitionBuilder.append(DBHelper.getDefaultForDataType(dataType));
            } else if ("sk".equalsIgnoreCase(constraint)) {
                    columnDefinitionBuilder.append(" NOT NULL DEFAULT ");
                    columnDefinitionBuilder.append(DBHelper.getDefaultForDataType(dataType)); // TBD Why not
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
    }

            
    public Map<String, Map<String, String>> getTmpTableData(Connection connection, String tmpTable, boolean propertyDB) throws SQLException {
        String query = "";
        if (propertyDB == true ) {
            query = SQLQueries.buildQueryForTableForDB(tmpTable); // `property` = 'db'
        } else {
            query = SQLQueries.buildQueryForTableForNonDB(tmpTable); // `property` != 'db'
        }
        Map<String, Map<String, String>> tmpTableData = new HashMap<>();
        try (PreparedStatement pstmt = connection.prepareStatement(query);
            ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                Map<String, String> rowData = new HashMap<>();
                rowData.put("table_name", tableName);
                rowData.put("Final_Table_Name", rs.getString("Final_Table_Name"));
                rowData.put("property", rs.getString("property"));
    
                tmpTableData.put(tableName, rowData);
            }
        }
        return tmpTableData;
    }

    public static class SQLQueries {
        // Query to drop a table
        public String getDropTableQuery(String tableName) {
            return "DROP TABLE IF EXISTS `" + tableName + "`";
        }

        // SQL Query to update the Active_flag in ELT_DL_Alter_Script_Info table
        public static final String UPDATE_ACTIVE_FLAG = "UPDATE ELT_DL_Alter_Script_Info SET Active_flag = 0 WHERE DL_Id = ?";

        public static final String SELECT_MAX_UPDATED_DATE = "SELECT MAX(Updated_Date) FROM ELT_DL_Mapping_Info WHERE DL_Id = ?";


        // SQL Query for retrieving distinct table and column information
        public static final String SELECT_DISTINCT_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY = "SELECT DISTINCT " +
                "  DL_Name AS Table_Name, " +
                "  DL_Column_Names AS Column_Name_Alias, " +
                "  `Constraints`, " +
                "  CASE " +
                "    WHEN DL_Data_Types = 'bit(1)' THEN 'tinyint(1)' " +
                "    ELSE DL_Data_Types " +
                "  END AS DL_Data_Types, " +
                "  DL_Id " +
                "FROM " +
                "  ELT_DL_Mapping_Info_Saved " +
                "WHERE " +
                "  DL_Id = ?";

        // SQL Query for deleting records
        public static final String DELETE_FROM_ELT_DL_CREATE_INFO_QUERY = "DELETE FROM ELT_DL_Create_Info WHERE DL_Name = ?";

        // SQL Query for inserting records
        public static final String INSERT_INTO_ELT_DL_CREATE_INFO_QUERY = "INSERT INTO ELT_DL_Create_Info (DL_ID, DL_NAME, script) VALUES (?, ?, ?)";

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
        public static final String JOIN_OUTER_ELT_DL_MAPPING_INFO_AS_MAIN_AND_SAVED_AS_LOOKUP =
                "SELECT DISTINCT " +
                        "    main.DL_Id, " +
                        "    main.DL_Column_Names, " +
                        "    main.Constraints AS main_constraints, " +
                        "    lookup.Constraints AS lookup_constraints, " +
                        "    main.DL_Data_Types AS main_data_types, " +
                        "    lookup.DL_Data_Types AS lookup_data_types " +
                        "FROM " +
                        "    ELT_DL_Mapping_Info main " +
                        "LEFT OUTER JOIN " +
                        "    ELT_DL_Mapping_Info_Saved lookup " +
                        "ON " +
                        "    main.DL_Id = lookup.DL_Id " +
                        "    AND main.DL_Column_Names = lookup.DL_Column_Names " +
                        "WHERE " +
                        "    main.DL_Id = ? " + // DL_Id as a parameter
                        "    AND main.DL_Name = ? " + // DL_Name as a parameter
                        "    AND main.Constraints = 'PK' " +
                        "ORDER BY main.DL_Column_Names";
                        // TBD: refer above counterpart query. Optionally remove this if matching data types isn't required:
                        // " AND main.DL_Data_Types = lookup.DL_Data_Types " +
        public static final String JOIN_ELT_DL_MAPPING_INFO_TABLES_FOR_PK_COLUMNS =
                "SELECT DISTINCT " +
                        "    main.DL_Id, " +
                        "    main.DL_Column_Names, " +
                        "    lookup.DL_Column_Names AS lookup_column_names " +
                        "FROM " +
                        "    ELT_DL_Mapping_Info_Saved main " +
                        "LEFT OUTER JOIN " +
                        "    ELT_DL_Mapping_Info lookup " +
                        "ON " +
                        "    main.DL_Id = lookup.DL_Id " +
                        "    AND main.DL_Column_Names = lookup.DL_Column_Names " +
                        "WHERE " +
                        "    main.DL_Id = ? " + // A parameter
                        "    AND main.Constraints = 'PK' " +
                        "ORDER BY main.DL_Column_Names";
        // It's alternative to above query. try to execute this. Check the results and execution timing.
        public static final String JOIN_ELT_DL_MAPPING_INFO_TABLES_FOR_PK_COLUMNS_GROUP_CONCAT = 
                "SELECT " +
                        "    main.DL_Id, " +
                        "    main.DL_Column_Names, " +
                        "    GROUP_CONCAT(CONCAT('`', main.DL_Column_Names, '`') ORDER BY main.DL_Column_Names SEPARATOR ', ') AS column_list " +
                        "    GROUP_CONCAT(CONCAT('`', lookup.DL_Column_Names, '`') ORDER BY lookup.DL_Column_Names SEPARATOR ', ') AS lookup_column_list " +
                        "    lookup.DL_Column_Names AS lookup_column_names " +
                        "FROM " +
                        "    ELT_DL_Mapping_Info_Saved main " +
                        "LEFT OUTER JOIN " +
                        "    ELT_DL_Mapping_Info lookup " +
                        "ON " +
                        "    main.DL_Id = lookup.DL_Id " +
                        "    AND main.DL_Column_Names = lookup.DL_Column_Names " +
                        "WHERE " +
                        "    main.DL_Id = ? " + // A parameter
                        "    AND main.Constraints = 'PK'";
                    
        public static final String JOIN_ELT_DL_MAPPING_INFO_TABLES_RECENTLY_UPDATED =
                "SELECT DISTINCT " +
                        "    saved.DL_Id, " +
                        "    saved.DL_Name, " +
                        "    saved.DL_Column_Names, " +
                        "    saved.Constraints AS saved_constraints, " +
                        "    saved.DL_Data_Types AS saved_data_types, " +
                        "    lookup.DL_Name AS lookup_DL_Name, " +
                        "    lookup.DL_Column_Names AS lookup_column_names, " +
                        "    lookup.Constraints AS lookup_constraints, " +
                        "    lookup.DL_Data_Types AS lookup_data_types " +
                        "FROM " +
                        "    ELT_DL_Mapping_Info_Saved saved " +
                        "LEFT OUTER JOIN " +
                        "    ELT_DL_Mapping_Info lookup " +
                        "ON " +
                        "    saved.DL_Id = lookup.DL_Id " +
                        "    AND saved.DL_Column_Names = lookup.DL_Column_Names " +
                        "WHERE " +
                        "    saved.DL_Id = ? " +  // A parameter
                        "    AND saved.Updated_Date > ? " +  // A parameter
                        "ORDER BY " +
                        "    saved.DL_Column_Names";

        public static final String INSERT_INTO_ELT_DL_MAPPING_INFO_FROM_SAVED = 
                "INSERT INTO `ELT_DL_Mapping_Info` (" +
                        "    DL_Id, " +
                        "    DL_Name, " +
                        "    DL_Column_Names, " +
                        "    Constraints, " +
                        "    DL_Data_Types, " +
                        "    Column_Type, " +
                        "    Added_Date, " +
                        "    Added_User, " +
                        "    Updated_Date, " +
                        "    Updated_User" +
                        ") " +
                        "SELECT " +
                        "    saved.DL_Id, " +
                        "    saved.DL_Name, " +
                        "    saved.DL_Column_Names, " +
                        "    saved.Constraints, " +
                        "    saved.DL_Data_Types, " +
                        "    saved.Column_Type, " +
                        "    saved.Added_Date, " +
                        "    saved.Added_User, " +
                        "    saved.Updated_Date, " +
                        "    saved.Updated_User " +
                        "FROM `ELT_DL_Mapping_Info_Saved` saved " +
                        "WHERE saved.DL_Id = ?";

        // SQL Query for deleting records from ELT_DL_Mapping_Info
        public static final String DELETE_FROM_ELT_DL_MAPPING_INFO = "DELETE FROM ELT_DL_Mapping_Info " +
                "WHERE DL_Id = ?";
                    
        // SQL Query for deleting records from ELT_DL_VALUES_PROPERTIES
        public static final String DELETE_FROM_ELT_DL_VALUES_PROPERTIES = 
                "DELETE FROM ELT_DL_VALUES_PROPERTIES " +
                        "WHERE DL_Id = ? " +
                        "AND Job_Id = ?";
        // SQL Query for deleting records from DELETE_FROM_ELT_DL_CONFIG_PROPERTIES
        public static final String DELETE_FROM_ELT_DL_CONFIG_PROPERTIES = 
                "DELETE FROM ELT_DL_CONFIG_PROPERTIES WHERE DL_Id = ? AND Job_Id = ?";

        public static final String SELECT_ACTIVE_ELT_DL_TABLE_INFO = 
            "SELECT DL_Id, DL_Name, DL_Table_Name, DL_Version, DL_Active_Flag " +
                "FROM ELT_DL_Table_Info " +
                "WHERE DL_Active_Flag = '1' AND DL_Id = ?";
        
        // Config File
        public static final String SELECT_JOB_PROPERTIES_INFO_WITH_COMPONENT = "SELECT " +
                "    `ELT_Job_Properties_Info`.`Id`, " +
                "    `ELT_Job_Properties_Info`.`Job_Type`, " +
                "    `ELT_Job_Properties_Info`.`Component`, " +
                "    `ELT_Job_Properties_Info`.`Key_Name`, " +
                "    `ELT_Job_Properties_Info`.`Value_Name`, " +
                "    `ELT_Job_Properties_Info`.`Active_Flag`, " +
                "    `ELT_Job_Properties_Info`.`Dynamic_Flag` " +
                "FROM `ELT_Job_Properties_Info` " +
                "WHERE Job_Type = 'DL' " +
                "  AND Component = ? " +
                "  AND Active_Flag = 1";
    // TODO Config file both two can be merged
        public static final String SELECT_ELT_JOB_PROPERTIES_INFO = 
                "SELECT " +
                        "`ELT_Job_Properties_Info`.`Id`, " +
                        "`ELT_Job_Properties_Info`.`Job_Type`, " +
                        "`ELT_Job_Properties_Info`.`Component`, " +
                        "`ELT_Job_Properties_Info`.`Key_Name`, " +
                        "`ELT_Job_Properties_Info`.`Value_Name`, " +
                        "`ELT_Job_Properties_Info`.`Active_Flag`, " +
                        "`ELT_Job_Properties_Info`.`Dynamic_Flag` " +
                        "FROM `ELT_Job_Properties_Info` " +
                        "WHERE `Job_Type` = 'DL' " +
                        "AND `Component` IN (?) " +
                        "AND `Active_Flag` = 1";
//                        "AND `Component` IN ('partitionsourcesql_dl', 'sourcesql') " +

        // Value 
        public static final String SELECT_VALUE_NAMES_FROM_ELT_JOB_PROPERTIES_INFO = 
                "SELECT DISTINCT `ELT_Job_Properties_Info`.`Value_Name` " +
                "FROM `ELT_Job_Properties_Info` " +
                "WHERE Job_Type='DL' AND Component IN (?) " +
                "AND Active_Flag=1 AND Dynamic_Flag=1";
        
        // TODO 11 below
        public static String SELECT_REPLACEMENT_MAPPING_INFO = 
                "SELECT DISTINCT " +
                "    '' AS Table_Name, " +
                "    m.DL_Column_Names, " +
                "    m.Constraints, " +
                "    '' AS Source_Name, " +
                "    LOWER(SUBSTRING_INDEX(m.DL_Data_Types, '(', 1)) AS Data_Type, " +
                "    m.DL_Id, " +
                "    11 AS Job_Id " +
                "FROM ELT_DL_Mapping_Info_Saved m " +
                // "INNER JOIN ELT_DL_Join_Mapping_Info j " +
                // "    ON m.DL_Id = j.DL_Id " +
                // "    AND 11 = j.Job_Id " +
                // "    AND m.DL_Column_Names = j.Join_Column_Alias " +
                "WHERE m.Constraints IN ('Pk', 'SK') " +
                "    AND m.DL_Id = ? " +
                "    AND m.DL_Column_Names NOT IN ( " +
                "        SELECT DISTINCT Column_Alias_Name " +
                "        FROM ELT_DL_Derived_Column_Info " +
                "        WHERE DL_ID = ? AND Job_Id = ? " +
                "    )";
        // TODO see if above query is related. INNER Join part is must
        public String getReplacementExcludeConstraintsQuery(String dlId, String jobId) {
            return "SELECT DISTINCT " +
                    "'' AS Table_Name, " +
                    "DL_Column_Names, " +
                    "Constraints, " +
                    "'' AS Source_Name, " +
                    "LOWER(SUBSTRING_INDEX(DL_Data_Types, '(', 1)) AS Data_Type, " +
                    "DL_Id, " +
                    "'" + jobId + "' AS Job_Id " +
                    "FROM ELT_DL_Mapping_Info_Saved " +
                    "WHERE Constraints NOT IN ('Pk','SK') " +
                    "AND DL_Id = '" + dlId + "' " +
                    "AND DL_Column_Names NOT IN ( " +
                    "SELECT DISTINCT Column_Alias_Name " +
                    "FROM ELT_DL_Derived_Column_Info " +
                    "WHERE DL_ID='" + dlId + "' AND Job_Id='" + jobId + "')";
        }

        public String getReplacementIncludeConstraintsQuery(String dlId, String jobId) {
            return "SELECT DISTINCT " +
                    "'' AS Table_Name, " +
                    "DL_Column_Names, " +
                    "Constraints, " +
                    "'' AS Source_Name, " +
                    "LOWER(SUBSTRING_INDEX(DL_Data_Types, '(', 1)) AS Data_Type, " +
                    "DL_Id, " +
                    "'" + jobId + "' AS Job_Id " +
                    "FROM ELT_DL_Mapping_Info_Saved " +
                    "WHERE Constraints IN ('Pk','SK') " +
                    "AND DL_Id = '" + dlId + "' " +
                    "AND DL_Column_Names NOT IN ( " +
                    "SELECT DISTINCT Column_Alias_Name " +
                    "FROM ELT_DL_Derived_Column_Info " +
                    "WHERE DL_ID='" + dlId + "' AND Job_Id='" + jobId + "')";
        }

        public static final String SELECT_FILTER_GROUP_BY_INFO =
                "SELECT DISTINCT " +
                "    `ELT_DL_FilterGroupBy_Info`.`DL_Id`, " +
                "    `ELT_DL_FilterGroupBy_Info`.`Job_Id`, " +
                "    `ELT_DL_FilterGroupBy_Info`.`Group_By_Id`, " +
                "    `ELT_DL_FilterGroupBy_Info`.`Filter_Id`, " +
                "    `ELT_DL_FilterGroupBy_Info`.`Flow`, " +
                "    `ELT_DL_FilterGroupBy_Info`.`Settings_Position` " +
                "FROM " +
                "    `ELT_DL_FilterGroupBy_Info` " +
                "WHERE " +
                "    Settings_Position = ? " + // Parameter for Settings_Position
                "    AND Job_Id = ? " +
                "    AND DL_Id = ?";

        public static final String SELECT_ELT_DL_DERIVED_COLUMN_INFO_BY_JOB_AND_DL_ID = "SELECT DISTINCT " +
                "  ELT_DL_Derived_Column_Info.DL_Id, " +
                "  ELT_DL_Derived_Column_Info.Job_Id, " +
                "  ELT_DL_Derived_Column_Info.Level " +
                "FROM " +
                "  ELT_DL_Derived_Column_Info " +
                "WHERE " +
                "  Job_Id = ? " +
                "  AND DL_Id = ? " +
                "ORDER BY " +
                "  Level";

        public static final String SELECT_ELT_DL_DERIVED_COLUMN_INFO = "SELECT DISTINCT " +
                "    `ELT_DL_Derived_Column_Info`.`DL_Id`, " +
                "    `ELT_DL_Derived_Column_Info`.`Job_Id`, " +
                "    `ELT_DL_Derived_Column_Info`.`Level` " +
                "FROM `ELT_DL_Derived_Column_Info` " +
                "WHERE " +
                "    Expression_Type = ? " +
                "    AND Level = ? " + // Parameterized Level
                "    AND Job_Id = ? " + 
                "    AND DL_Id = ?"; 

        // Config Job 2 lkp/join
        // Java function to create a query as table name cannot be passed as parameter.
        // As 2nd and 3rd queries are same, both join are combined in one.
        // However, the separate ones are also added, in case they are needed.

        //TODO this query has to be reviewed. two separate tables must be used
        public static String buildConfigJoinQuery(String table, String join) {

            //return "SELECT DISTINCT ELT_DL_Join_Mapping_Info.Table_Name, ELT_DL_Join_Mapping_Info.Join_Table, ELT_DL_Join_Mapping_Info.Join_Table_Alias, ELT_DL_Join_Mapping_Info.Table_Name_Alias, CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name, t1.Final_Table_Name AS Table_Final_Table_Name, t1.property AS Table_Property, t2.Final_Table_Name AS Join_Final_Table_Name, t2.property AS Join_Property FROM ELT_DL_Join_Mapping_Info LEFT OUTER JOIN test_1911 t1 ON (ELT_DL_Join_Mapping_Info.Table_Name_Alias = t1.table_name AND t1.property != 'db') LEFT OUTER JOIN test_1911 t2 ON (ELT_DL_Join_Mapping_Info.Join_Table_Alias = t2.table_name AND t2.property != 'db') WHERE ELT_DL_Join_Mapping_Info.Job_Id = 11 AND ELT_DL_Join_Mapping_Info.DL_Id = 9 ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
            return "SELECT DISTINCT " +
                    "ELT_DL_Join_Mapping_Info.Table_Name, " +
                    "ELT_DL_Join_Mapping_Info.Join_Table, " +
                    "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
                    "ELT_DL_Join_Mapping_Info.Table_Name_Alias, " +
                    "CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name, " +
                    "t1" + ".Final_Table_Name AS Table_Final_Table_Name, " +
                    "t1" + ".property AS Table_Property, " +
                    "t2" + ".Final_Table_Name AS Join_Final_Table_Name, " +
                    "t2" + ".property AS Join_Property " +
                    "FROM ELT_DL_Join_Mapping_Info " +
                    "LEFT OUTER JOIN " + table + " t1 " +
                    "ON (ELT_DL_Join_Mapping_Info.Table_Name_Alias = " + "t1" + ".table_name AND " + "t1" + ".property != 'db') " +
                    "LEFT OUTER JOIN " + join + " t2 " +
                    "ON (ELT_DL_Join_Mapping_Info.Join_Table_Alias = " + "t2" + ".table_name AND " + "t2" + ".property != 'db') " + 
                    "WHERE ELT_DL_Join_Mapping_Info.Job_Id = ? " +
                    "AND ELT_DL_Join_Mapping_Info.DL_Id = ? " +
                    "ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
        }

        // public static String buildConfigJoinQuery(String table, String join) {
        //     return "SELECT DISTINCT " +
        //             "ELT_DL_Join_Mapping_Info.Table_Name, " +
        //             "ELT_DL_Join_Mapping_Info.Join_Table, " +
        //             "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
        //             "ELT_DL_Join_Mapping_Info.Table_Name_Alias, " +
        //             "CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name, " +
        //             table + ".Final_Table_Name AS Table_Final_Table_Name, " +
        //             table + ".property AS Table_Property, " +
        //             join + ".Final_Table_Name AS Join_Final_Table_Name, " +
        //             join + ".property AS Join_Property " +
        //             "FROM ELT_DL_Join_Mapping_Info " +
        //             "LEFT OUTER JOIN " + table + " " +
        //             "ON (ELT_DL_Join_Mapping_Info.Table_Name_Alias = " + table + ".table_name AND " + table + ".property != 'db') " +
        //             "OR (ELT_DL_Join_Mapping_Info.Join_Table_Alias = " + join + ".table_name AND " + join + ".property != 'db') " + 
        //             "WHERE ELT_DL_Join_Mapping_Info.Job_Id = ? " +
        //             "AND ELT_DL_Join_Mapping_Info.DL_Id = ? " +
        //             "ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
        // }

        // TBD: refer above. May be deleted
        // Query 1: Primary query from ELT_DL_Join_Mapping_Info
        public static final String QUERY1 = "SELECT DISTINCT " +
                "ELT_DL_Join_Mapping_Info.Table_Name, " +
                "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
                "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
                "ELT_DL_Join_Mapping_Info.Table_Name_Alias, " +
                "CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name "
                +
                "FROM ELT_DL_Join_Mapping_Info " +
                "WHERE Job_Id = ? AND DL_Id = ? " +
                "ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
        // TBD: refer above. May be deleted
        // Method to dynamically build Query 2 with the table name passed as a parameter
        public static String buildQuery2(String table2) {
            return "SELECT " +
                    table2 + ".`table_name`, " +
                    table2 + ".`Final_Table_Name`, " +
                    table2 + ".`property` " +
                    "FROM " + table2 + " WHERE " + table2 + ".`property` != 'db'";
        }
        // TBD: refer above. May be deleted
        // Method to dynamically build Query 3 with the table name passed as a parameter
        public static String buildQuery3(String table3) {
            return "SELECT " +
                    table3 + ".`table_name`, " +
                    table3 + ".`Final_Table_Name`, " +
                    table3 + ".`property` " +
                    "FROM " + table3 + " WHERE " + table3 + ".`property` != 'db'";
        }
        // Selction from tmp table `property` = 'db'
        public static String buildQueryForTableForDB(String table) {
            return "SELECT " +
                    table + ".`table_name`, " +
                    table + ".`Final_Table_Name`, " +
                    table + ".`property` " +
                    "FROM " + table + " WHERE " + table + ".`property` = 'db'";
        }
        // Selction from tmp table `property` != 'db'
        public static String buildQueryForTableForNonDB(String table) {
            return "SELECT " +
            table + ".`table_name`, " +
                    table + ".`Final_Table_Name`, " +
                    table + ".`property` " +
                    "FROM " + table + " WHERE " + table + ".`property` != 'db'";
        }


        // TBD: refer above. May be deleted
        // Full Join Query: Joining ELT_DL_Join_Mapping_Info with tables in Query 2 and Query 3
        public static String buildFullJoinQuery(String table2, String table3) {
            return "SELECT DISTINCT " +
                    "ELT_DL_Join_Mapping_Info.Table_Name, " +
                    "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
                    "ELT_DL_Join_Mapping_Info.Table_Name_Alias, " +
                    "CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name, "
                    +
                    "t2.Final_Table_Name AS Table2_Final_Table_Name, " +
                    "t3.Final_Table_Name AS Table3_Final_Table_Name " +
                    "FROM ELT_DL_Join_Mapping_Info " +
                    "LEFT OUTER JOIN (" + buildQuery2(table2) + ") t2 " +
                    "ON ELT_DL_Join_Mapping_Info.Table_Name_Alias = t2.table_name " +
                    "LEFT OUTER JOIN (" + buildQuery3(table3) + ") t3 " +
                    "ON ELT_DL_Join_Mapping_Info.Join_Table_Alias = t3.table_name " +
                    "WHERE ELT_DL_Join_Mapping_Info.Job_Id = ? " +
                    "AND ELT_DL_Join_Mapping_Info.DL_Id = ? " +
                    "ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
        }

        // TBD: See if this and above queries if one of them can be got rid of!! Job - Source
        public static final String SELECT_ELT_DL_FILTER_GROUP_BY_INFO = 
                "SELECT " +
                "`ELT_DL_FilterGroupBy_Info`.`DL_Id`, " +
                "`ELT_DL_FilterGroupBy_Info`.`Job_Id`, " +
                "`ELT_DL_FilterGroupBy_Info`.`Table_Name`, " +
                "`ELT_DL_FilterGroupBy_Info`.`Table_Name_Alias`, " +
                "Settings_Position " +
                "FROM `ELT_DL_FilterGroupBy_Info` " +
                "WHERE Settings_Position IN ('Lookup_Table', 'Driving_Table') " +
                "AND Group_By_Id <> '0' " +
                "AND DL_Id = ? " + 
                "AND Job_Id = ?";

        // Job - Source
        // TODO Query 2 : similar but shorter to 1 and 3, 4 difference is Settings_Position is additional
        public static final String SELECT_ELT_DL_DRIVING_AND_LOOKUP_TABLE_INFO = 
                "SELECT " +
                "DL_Id, " +
                "Job_Id, " +
                "`Table_Name`, " +
                "`Table_Name` AS `Table_Name_Alias`, " +
                "'Driving_Table' AS Settings_Position " +
                "FROM `ELT_DL_Driving_Table_Info` " +
                "WHERE DL_Id = ? " +
                "AND Job_Id = ? " + 
                "UNION ALL " +
                "SELECT " +
                "DL_Id, " +
                "Job_Id, " +
                "`Table_Name`, " +
                "`Table_Name_Alias` AS `Table_Name_Alias`, " +
                "'Lookup_Table' AS Settings_Position " +
                "FROM `ELT_DL_Lookup_Table_Info` " +
                "WHERE DL_Id = ? " + 
                "AND Job_Id = ?";
        // TBD: Alternate approach of above - inner join in query itself. efficient approach
        public static final String SELECT_ELT_JOINED_FILTER_GROUP_BY_DRIVING_AND_LOOKUP = 
                "SELECT " +
                "  fgi.DL_Id, " +
                "  fgi.Job_Id, " +
                "  fgi.Table_Name, " +
                "  fgi.Table_Name_Alias, " +
                "  fgi.Settings_Position, " +
                "  dli.Table_Name AS Driving_Lookup_Table_Name, " +
                "  dli.Table_Name_Alias AS Driving_Lookup_Table_Alias, " +
                "  dli.Settings_Position AS Driving_Lookup_Settings_Position " +
                "FROM " +
                "  ELT_DL_FilterGroupBy_Info fgi " +
                "INNER JOIN (" +
                "  (SELECT " +
                "    ELT_DL_Driving_Table_Info.DL_Id, " +
                "    ELT_DL_Driving_Table_Info.Job_Id, " +
                "    ELT_DL_Driving_Table_Info.Table_Name, " +
                "    ELT_DL_Driving_Table_Info.Table_Name AS Table_Name_Alias, " +
                "    'Driving_Table' AS Settings_Position " +
                "  FROM " +
                "    ELT_DL_Driving_Table_Info " +
                "  WHERE " +
                "    ELT_DL_Driving_Table_Info.DL_Id = ? " +
                "    AND ELT_DL_Driving_Table_Info.Job_Id = ?) " +
                "  UNION ALL " +
                "  (SELECT " +
                "    ELT_DL_Lookup_Table_Info.DL_Id, " +
                "    ELT_DL_Lookup_Table_Info.Job_Id, " +
                "    ELT_DL_Lookup_Table_Info.Table_Name, " +
                "    ELT_DL_Lookup_Table_Info.Table_Name_Alias AS Table_Name_Alias, " +
                "    'Lookup_Table' AS Settings_Position " +
                "  FROM " +
                "    ELT_DL_Lookup_Table_Info " +
                "  WHERE " +
                "    ELT_DL_Lookup_Table_Info.DL_Id = ? " +
                "    AND ELT_DL_Lookup_Table_Info.Job_Id = ?" +
                ") dli " +
                "ON fgi.DL_Id = dli.DL_Id " +
                "  AND fgi.Job_Id = dli.Job_Id " +
                "  AND fgi.Table_Name = dli.Table_Name " +
                "  AND fgi.Table_Name_Alias = dli.Table_Name_Alias " +
                "  AND fgi.Settings_Position = dli.Settings_Position " +
                "WHERE " +
                "  fgi.Settings_Position IN ('Lookup_Table', 'Driving_Table') " +
                "  AND fgi.Group_By_Id <> '0' " +
                "  AND fgi.DL_Id = ? " +
                "  AND fgi.Job_Id = ?";
        //value SQL Expression
        public static final String SELECT_COLUMN_NAME_ALIASES = 
            "SELECT concat('`', Column_Name_Alias, '`') " +
                "FROM ELT_DL_Driving_Table_Info " +
                "WHERE DL_Id = ? AND Job_Id = ? " +
                "UNION ALL " +
                "SELECT concat('`', Column_Name_Alias, '`') " +
                "FROM ELT_DL_Lookup_Table_Info " +
                "WHERE DL_Id = ? AND Job_Id = ?";
        // Value Source 
        public static final String SOURCE_MAIN_QUERY = "SELECT DISTINCT " +
            "ELT_DL_FilterGroupBy_Info.DL_Id, " +
            "ELT_DL_FilterGroupBy_Info.Job_Id, " +
            "ELT_DL_FilterGroupBy_Info.Settings_Position, " +
            "ELT_DL_FilterGroupBy_Info.Table_Name, " +
            "ELT_DL_FilterGroupBy_Info.Table_Name_Alias, " +
            "ELT_DL_FilterGroupBy_Info.Group_By_Id, " +
            "ELT_DL_FilterGroupBy_Info.Flow, " +
            "ELT_DL_Group_By_Info.Column_Name, " +
            "CASE WHEN ELT_DL_Group_By_Info.Aggregation = 'random' THEN '' ELSE ELT_DL_Group_By_Info.Aggregation END AS Aggregation, " +
            "ELT_DL_Group_By_Info.Flag " +
            "FROM ELT_DL_FilterGroupBy_Info " +
            "INNER JOIN ELT_DL_Group_By_Info ON ELT_DL_FilterGroupBy_Info.Group_By_Id = ELT_DL_Group_By_Info.Group_By_Id " +
            "WHERE Job_Id = ? AND DL_Id = ? AND Settings_Position IN ('Lookup_Table', 'Driving_Table')";
        // Value Source 
        public static final String GET_FILTER_GROUP_BY_INFO_QUERY =
            "SELECT DISTINCT " +
            "ELT_DL_FilterGroupBy_Info.DL_Id, " +
            "ELT_DL_FilterGroupBy_Info.Job_Id, " +
            "ELT_DL_FilterGroupBy_Info.Settings_Position, " +
            "ELT_DL_FilterGroupBy_Info.Table_Name, " +
            "ELT_DL_FilterGroupBy_Info.Table_Name_Alias, " +
            "ELT_DL_FilterGroupBy_Info.Filter_Id, " +
            "ELT_DL_FilterGroupBy_Info.Flow, " +
            "ELT_DL_Filter_Info.Filter_Condition, " +
            "ELT_DL_Filter_Info.Filter_Columns " +
            "FROM ELT_DL_FilterGroupBy_Info " +
            "INNER JOIN ELT_DL_Filter_Info " +
            "ON ELT_DL_FilterGroupBy_Info.Filter_Id = ELT_DL_Filter_Info.Filter_Id " +
            "WHERE Job_Id = ? AND DL_Id = ? AND Settings_Position IN ('Lookup_Table','Driving_Table')";

       // Value Source Execute SQL
        // TODO Query 3 : similar to 1 and 4
       public static final String SELECT_DISTINCT_FROM_DRIVING_LOOKUP_TABLE_INFO =
            " Select Distinct " +
            " Job_Id, " +
            " DL_Id, " +
            " Table_Name, " +
            " Table_Name as Table_Name_Alias, " +
            " Column_Name, " +
            " Column_Name_Alias, " +
            " Data_Type " +
            " from ELT_DL_Driving_Table_Info where Job_Id = ? and DL_Id = ? " +
            " union all " +
            " select Distinct " +
            " Job_Id, " +
            " DL_Id, " +
            " Table_Name, " +
            " Table_Name_Alias, " +
            " Column_Name, " +
            " Column_Name_Alias, " +
            " Data_Type " +
            " from ELT_DL_Lookup_Table_Info where Job_Id = ? and DL_Id = ?";
            // Value Source Execute SQL
        public static final String SELECT_DISTINCT_FROM_FILTER_GROUP_BY_INFO =
            " Select Distinct " +
            " ELT_DL_FilterGroupBy_Info.DL_Id, " +
            " ELT_DL_FilterGroupBy_Info.Job_Id, " +
            " ELT_DL_FilterGroupBy_Info.Settings_Position, " +
            " ELT_DL_FilterGroupBy_Info.Table_Name, " +
            " ELT_DL_FilterGroupBy_Info.Table_Name_Alias, " +
            " ELT_DL_FilterGroupBy_Info.Group_By_Id, " +
            " ELT_DL_FilterGroupBy_Info.Flow, " +
            " ELT_DL_Group_By_Info.Column_Name, " +
            " case when ELT_DL_Group_By_Info.Aggregation = 'random' then 'LAST' else ELT_DL_Group_By_Info.Aggregation end as Aggregation, " +
            " ELT_DL_Group_By_Info.Flag, " +
            " ELT_DL_Group_By_Info.Column_Name_Alias " +
            " from ELT_DL_FilterGroupBy_Info " +
            " inner join ELT_DL_Group_By_Info on ELT_DL_FilterGroupBy_Info.Group_By_Id = ELT_DL_Group_By_Info.Group_By_Id " +
            " where ELT_DL_FilterGroupBy_Info.Job_Id = ? " +
            " and ELT_DL_FilterGroupBy_Info.DL_Id = ? " +
            " and ELT_DL_FilterGroupBy_Info.Settings_Position in ('Lookup_Table', 'Driving_Table')";
        // Value Source Execute Sql
        public static final String SELECT_FROM_FILTER_GROUP_BY_INFO = 
            "SELECT " +
            "`ELT_DL_FilterGroupBy_Info`.`DL_Id`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Job_Id`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Table_Name`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Table_Name_Alias`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Flow`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Filter_Id`, " +
            "`ELT_DL_FilterGroupBy_Info`.`Group_By_Id` " +
            "FROM `ELT_DL_FilterGroupBy_Info` " +
            "WHERE Group_By_Id<>0 AND Job_Id = ? AND DL_Id = ? AND Settings_Position IN ('Lookup_Table', 'Driving_Table')";
        // Value Source Execute Sql main flow
        // Query 4 : similar to 1 and 3
        public static final String SELECT_FROM_DRIVING_LOOKUP_TABLE_INFO = 
            " Select Distinct " +
            " Job_Id, " +
            " DL_Id, " +
            " Table_Name, " +
            " Table_Name as Table_Name_Alias, " +
            " Column_Name, " +
            " Column_Name_Alias, " +
            " Data_Type " +
            " from ELT_DL_Driving_Table_Info where Job_Id = ? and DL_Id = ? " +
            " union all " +
            " select Distinct " +
            " Job_Id, " +
            " DL_Id, " +
            " Table_Name, " +
            " Table_Name_Alias, " +
            " Column_Name, " +
            " Column_Name_Alias, " +
            " Data_Type " +
            " from ELT_DL_Lookup_Table_Info where Job_Id = ? and DL_Id = ?";
        // Query 5 : similar to 1 and 3
        public static final String SELECT_TABLE_NAME_FROM_DRIVING_LOOKUP_TABLE = 
                " Select Distinct " +
                " Table_Name, " +
                " Table_Name as Table_Name_Alias " +
                " from ELT_DL_Driving_Table_Info where Job_Id = ? and DL_Id = ? " +
                " union all " +
                " select Distinct " +
                " Table_Name, " +
                " Table_Name_Alias " +
                " from ELT_DL_Lookup_Table_Info where Job_Id = ? and DL_Id = ?";

        public static String buildAppPropertiesQuery(String DL_Id, String Job_Id) {
            String query = "SELECT DISTINCT " +
                    DL_Id + " AS `DL_Id`, " +
                    Job_Id + " AS `Job_Id`, " +
                    "CASE WHEN `ELT_App_Names_Properties`.`CharacterValue` = ' ' THEN ',' ELSE `CharacterValue` END, " +
                    "`ELT_App_Names_Properties`.`Id` " +
                    "FROM `ELT_App_Names_Properties` " +
                    "WHERE `CharacterValue` NOT IN ('_', '-', '/', '|') " +
                    "LIMIT 30";
            return query;
        }
    }

    
    // Context class to hold the Input values - APP_DB, Target_DB and so on
    public static class Context {

        // Declare the singleton instance (volatile for thread safety)
        private static volatile Context instance;

        private String jobId;
        private String dlId;
        private String tgtHost;
        private String tgtPort;
        private String tgtDbName;
        private String tgtUser;
        private String tgtPassword;
        private String dlName;
        private String sinkValue;

        private Context() {
            System.out.println("Context has been intialized");
            init();
        }

        private void init() {
            tgtHost = "172.25.25.124";
            tgtPort = "4475";
            tgtDbName = "Mysql8_2_1009427";
            tgtUser = "XXX_U_DONT_CHANGE";
            tgtPassword = "XXX_P_DONT_CHANGE";
            dlName = "test_1";
            // sinkValue = ;
        }

        // The method to provide access to the singleton instance
        public static Context getContext() {
            if (instance == null) {
                synchronized (Context.class) {
                    if (instance == null) {
                        instance = new Context();
                    }
                }
            }
            return instance;
        }
        public String getJobId() {
            return jobId;
        }

        public String getDlId() {
            return dlId;
        }

        public String getTgtHost() {
            return tgtHost;
        }

        public String getTgtPort() {
            return tgtPort;
        }

        public String getTgtDbName() {
            return tgtDbName;
        }

        public String getTgtUser() {
            return tgtUser;
        }

        public String getTgtPassword() {
            return tgtPassword;
        }

        public String getDlName() {
            return dlName;
        }

        public String getSinkValue() {
            return sinkValue;
        }
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

        // Helper function to get the default value based on the data type
        static String getDefaultForDataType(String dataType) {
            dataType = dataType.toLowerCase();
            
            if (dataType.startsWith("varchar") || dataType.startsWith("text") || dataType.startsWith("char")) {
                return "''";
            } else if (dataType.contains("int")) {
                return "'0'";
            } else if (dataType.contains("float") || dataType.contains("decimal")) {
                return "'0.0'";
            } else if (dataType.contains("boolean")) {
                return "0";
            } else if (dataType.contains("date")) {
                return "'0000-00-00'";
            } else {
                return " "; // Default to empty string for unknown types
            }
        }
    }

    private void executeDropTableQuery(Connection conn, String tableName) {
        String sql = sqlQueries.getDropTableQuery(tableName);
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Table dropped: " + tableName);
        } catch (SQLException e) {
            System.err.println("Error executing drop table query: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        long clientId = 1009427;
        long tableId = 9; //DL_Id
        String dlName = "test_1";
        long jobId = 11;

        System.out.println("########################## Program Starts ####################");
        System.out.println("Inputs: clientId: " + clientId);
        System.out.println("Inputs: dlId: " + tableId + ", JobId: " + jobId + ", dlName: " + dlName);
        DataMartStructureScriptGenerator generator = new DataMartStructureScriptGenerator(clientId, DataSourceType.MYSQL, tableId, jobId, dlName);
        generator.generateScripts();
    }
}
