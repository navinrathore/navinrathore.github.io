package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import javax.xml.transform.Source;

public class DataMartStructureScriptGenerator {
    private DataSourceType dataSourceType;
    private String dlId;
    private String dlName;
    private String jobId;

    SQLQueries sqlQueries;
    Connection conn;

    public DataMartStructureScriptGenerator(DataSourceType type, String dlId) {
        this.dataSourceType = type;
        this.dlId = dlId;
        init();
    }

    private void init() {
        sqlQueries = new SQLQueries(); // Creating instance of SQLQueries inner class
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

    public String getDlId() {
        return dlId;
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
        executeDropTableQuery(conn, dlName, dlId, jobId);
    }

    public class DataMartConfigScriptGenerator {
        public DataMartConfigScriptGenerator() {
         }

        public Status generateConfigScript() {
            System.out.println("Generating config script for DL_ID: " + dlId);
            boolean status = false;

//######################################
            // Job 1 Source

            try {
                String settings = fetchLoadConfigsSettings(conn, dlId, jobId);

            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


            // Job 2 lkp/join
            String previousComponent = ""; // TBD
            String componentJoin = "join";
            String propsJoin = fetchAndFormatProperties(conn, componentJoin);
            //String joinScript = propsJoin.replace("Dynamic_Join_Name", previousComponent);
            //previousComponent = "Join";

            // Both tables carry same name that is context.DL_Name+context.DL_Id+context.Job_Id
            // Trying to use simplified Join
            String table = dlName + dlId + jobId;
            // String finalQuery = SQLQueries.buildFullJoinQuery(table);   
            //String finalQuery = SQLQueries.buildFullJoinQuery(table, table); If two tables are joined separatly.

            try {
                String JoinComponent = executeJoinQueryAndBuildJoinComponent(conn, table);
                // previousComponent = ; TBD returned from above function
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            // Job 3 Recoercing
            String componentEmptyRecoercing = "empty_recoercing";
            String propsEmptyRecoercing = fetchAndFormatProperties(conn, componentEmptyRecoercing);
            String recoercingScript = propsEmptyRecoercing.replace("Dynamic_Join_Name", previousComponent);
            previousComponent = "Recoercing";

            // Job 4 NullReplacement
            String componentNullReplacement = "executesqlfiltergroupby";
            String propsNullReplacement = fetchAndFormatProperties(conn, componentNullReplacement);
            String emptyScript = propsNullReplacement.replace("Dynamic_Join_Name", previousComponent);
            previousComponent = "Cleansing_Fields";

            // SubJob 5 - GroupbyJoin
            String componentFilterGroupBy = "executesqlfiltergroupby";
            String filterGroupBy = fetchAndFormatProperties(conn, componentFilterGroupBy);
            String dynamicFilterGroupByName = filterGroupBy.replace("Dynamic_FilterGroupby_Name", "Join_Aggregation");
            String dynamicFilterGroupBySource = dynamicFilterGroupByName.replace("Dynamic_FilterGroupby_Source", previousComponent);
            String previousName = "Join_Aggregation";
            String joinDynamicGroupbyFilterConfig = dynamicFilterGroupBySource;
            System.out.println("Previous Name: " + previousName);
            System.out.println("Derived Dynamic Groupby Filter Config: " + joinDynamicGroupbyFilterConfig);

            // Job 6 Derived
                    // Subjob "Child"
            try {
                List<Map<String, String>> results = getDerivedColumnInfoByJobAndDLId(conn, jobId, dlId);

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
                        String expressionComponent = processDerivedColumnInfoForTypeSQL(result, jobPropertiesJava);

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
                    String expressionComponent = finalExpressionComponentBuilder.toString();

            } catch (SQLException e) {
                e.printStackTrace();
            }


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
            // Job 8 remit
            String componentEmptyRemit = "empty_remit";
            String propsEmptyRemit = fetchAndFormatProperties(conn, componentEmptyRemit);
            // No replacement
            String emptyRetainRename = "empty_Component"; // TBD: it has to come from env or some previopus setting!!
            final String defaultJoinNameRemit = "Emit_UnWanted_Columns";
            String emptyRemitComponent = propsEmptyRemit; // output of this component
            String previousJoinName = ""; // TBD or NULL
            // TBD: Below funciton/query seems not doing any actual processing. Seems redundent. Check!!
            try {
                previousJoinName = processDerivedColumnInfo(conn, jobId, dlId, defaultJoinNameRemit, emptyRetainRename);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            previousComponent = previousJoinName; // output of this component
            //#########################
            // Job 9 Rename
            String componentEmptyRename = "empty_rename";
            String propsEmptyRename = fetchAndFormatProperties(conn, componentEmptyRename);
            String EmptyRenameScript = propsEmptyRename.replace("Dynamic_Previous_Component", previousComponent);
            String EmptyRenameComponent = EmptyRenameScript; /// output of this component
            final String defaultJoinNameRename = "Rename_Derived_Columns";
           // String emptyRetainRename = "empty_Component"; // TBD: it has to come from env or some previopus setting!!
            previousJoinName = ""; // TBD or NULL; see above
            // TBD: Below funciton/query seems not doing any actual processing. Seems redundent. Check!!
            try {
                previousJoinName = processDerivedColumnInfo(conn, jobId, dlId, defaultJoinNameRename, emptyRetainRename);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            previousComponent = previousJoinName; // output of this component
            //#########################
            // Job 10 Sink
            String componentSink = "sqlsink";
            String propsSink = fetchAndFormatProperties(conn, componentSink);
            String sinkScript = propsSink.replace("Dynamic_Sink_Source", previousComponent);
            String sinkComponent = sinkScript;
            //previousComponent = ""; Should it be set here?
    //######################################


            //config filename
            // script = result
            //TBD: to be filled up values built in previous steps
            String configFileName = "";
            String script = "";
            Map<String, String> rowDetails = selectActiveEltDlTableInfo(conn, dlId);
            if (rowDetails == null) {
                System.out.println("No record found with DL_Id: " + dlId);
                return Status.FAILURE;
            }
            rowDetails.put("JobId", jobId);
            rowDetails.put("config_file_name", configFileName);
            rowDetails.remove("DL_Version");
            // TBD where is the script used??
            //rowDetails.put("script", script);
            status = deleteFromEltDlConfigProperties(conn, dlId, dlId);
            status = insertIntoEltDlConfigProperties(conn, rowDetails);

            return status ? Status.SUCCESS : Status.FAILURE;
        }

        public String fetchAndFormatProperties(Connection conn, String component) {
            String query = SQLQueries.SELECT_JOB_PROPERTIES_INFO_WITH_COMPONENT;
        
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
    
        public String fetchLoadConfigsSettings(Connection conn, String dlId, String jobId) throws SQLException {
            String query = "SELECT `ELT_DL_Load_Configs`.`Settings` FROM `ELT_DL_Load_Configs` WHERE DL_Id = ? AND Job_Id = ?";
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, dlId);
                ps.setString(2, jobId);
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

        // Function to execute both queries and perform an inner join using a hash map
        // for efficiency
        public List<Map<String, Object>> executeAndJoinTables(Connection conn, String dlId, String jobId)
                throws SQLException {
            // Step 1: Fetch Filter Group By Info and store in a hash map
            Map<String, Map<String, Object>> filterGroupByInfoMap = fetchFilterGroupByInfoMap(conn, dlId, jobId);

            // Step 2: Fetch Driving and Lookup Table Info and perform the join
            return joinWithDrivingAndLookupTableInfo(conn, dlId, jobId, filterGroupByInfoMap);
        }

        // Function to fetch SELECT_ELT_DL_FILTER_GROUP_BY_INFO and store in a hash map
        // for fast lookups
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

                        // Create a unique key using the join fields
                        String key = createJoinKey(
                                rs.getString("DL_Id"),
                                rs.getString("Job_Id"),
                                rs.getString("Table_Name"),
                                rs.getString("Table_Name_Alias"),
                                rs.getString("Settings_Position"));

                        // Store the row in the map using the key
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
                ps.setString(1, dlId); // Set DL_Id for Driving Table
                ps.setString(2, jobId); // Set Job_Id for Driving Table
                ps.setString(3, dlId); // Set DL_Id for Lookup Table
                ps.setString(4, jobId); // Set Job_Id for Lookup Table

                try (ResultSet rs = ps.executeQuery()) {
                    List<Map<String, Object>> joinedResults = new ArrayList<>();

                    while (rs.next()) {
                        // Create a unique key using the join fields
                        String key = createJoinKey(
                                rs.getString("DL_Id"),
                                rs.getString("Job_Id"),
                                rs.getString("Table_Name"),
                                rs.getString("Table_Name_Alias"),
                                rs.getString("Settings_Position"));

                        // Check if there's a matching row in the filterGroupByInfoMap
                        if (filterGroupByInfoMap.containsKey(key)) {
                            Map<String, Object> filterRow = filterGroupByInfoMap.get(key);
                            Map<String, Object> drivingRow = new HashMap<>();
                            drivingRow.put("DL_Id", rs.getString("DL_Id"));
                            drivingRow.put("Job_Id", rs.getString("Job_Id"));
                            drivingRow.put("Table_Name", rs.getString("Table_Name"));
                            drivingRow.put("Table_Name_Alias", rs.getString("Table_Name_Alias"));
                            drivingRow.put("Settings_Position", rs.getString("Settings_Position"));

                            // Merge the filterRow and drivingRow
                            Map<String, Object> joinedRow = new HashMap<>(filterRow);
                            joinedRow.putAll(drivingRow);

                            // Add the joined row to the result list
                            joinedResults.add(joinedRow);
                        }
                    }

                    return joinedResults;
                }
            }
        }

        // Helper function to create a unique key for joining
        private String createJoinKey(String dlId, String jobId, String tableName, String tableAlias, String settingsPosition) {
            return dlId + "_" + jobId + "_" + tableName + "_" + tableAlias + "_" + settingsPosition;
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

        public List<Map<String, String>> getDerivedColumnInfoByJobAndDLId(Connection conn, String jobId, String dlId) {
            List<Map<String, String>> results = new ArrayList<>();

            String query = SQLQueries.SELECT_ELT_DL_DERIVED_COLUMN_INFO_BY_JOB_AND_DL_ID;
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, jobId);
                stmt.setString(2, dlId);
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

        public String processDerivedColumnInfo(Connection conn, String jobId, String dlId, final String joinName, String rename) throws SQLException {
            String query = "SELECT DISTINCT DL_Id FROM ELT_DL_Derived_Column_Info WHERE Job_Id = ? AND DL_Id = ?";
            String previousJoinName = null;
            String emptyRetainRename = null;
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, jobId);
                ps.setString(2, dlId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String dlIdResult = rs.getString("DL_Id");
                        if (dlIdResult == null) {
                            previousJoinName = null;
                            emptyRetainRename = null;
                        } else {
                            previousJoinName = joinName;
                            emptyRetainRename = rename;
                        }
                        System.out.println("DL_Id: " + dlIdResult);
                    }
                    System.out.println("Previous_Join_Name: " + previousJoinName);
                    System.out.println("Empty Retain Rename: " + emptyRetainRename);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SQLException("Error while processing derived column info", e);
            }
            return previousJoinName;
        }
        
        public String executeJoinQueryAndBuildJoinComponent(Connection conn, String table) throws SQLException {
            String query = SQLQueries.buildFullJoinQuery(table);
            
            try (PreparedStatement ps = conn.prepareStatement(query);
                 ResultSet rs = ps.executeQuery()) {
                  
                String joinComponent = ""; // Value used ion all iterations
                String previousJoinName = ""; // Value used ion all iterations
                StringBuilder finalDynamicJoinSources = new StringBuilder();

                while (rs.next()) {
                    String tableName = rs.getString("Table_Name");
                    String joinTableName = rs.getString("Join_Table_Name"); // TOD: Recheck the name
                    String tableNameAlias = rs.getString("Table_Name_Alias");
                    String joinTableAlias = rs.getString("Join_Table_Alias");
                    String joinName = rs.getString("Join_Name");

                    String finalTableName = rs.getString("Final_Table_Name");
                    String joinProperty = rs.getString("property");

                    // Process aliases
                    tableNameAlias = tableNameAlias.replace(" ", "_");
                    joinTableAlias = joinTableAlias.replace(" ", "_");
                    tableName = (joinProperty == null) ? tableNameAlias : finalTableName + "_ExecuteSql";
                    joinTableName = (joinProperty == null) ? joinTableAlias : finalTableName + "_ExecuteSql";
                
                    //String currentJoinName = tableName + joinTableAlias;
                    tableName = tableName.replace("\\$", "\\\\$");
                    joinTableName = joinTableName.replace("\\$", "\\\\$");

                    String joinSourceTables = previousJoinName == null ? (tableName + "," + joinTableName) : (previousJoinName + "," + joinTableName);
                    joinSourceTables = joinSourceTables.replace("\\$", "\\\\$"); // TBD: redundant? to be removed done above in parts
                    joinComponent = finalDynamicJoinSources.toString(); // from previous iteration
                    String dynamicJoinName = joinComponent.replace("Dynamic_Join_Name", tableName + "_" + joinTableName);
                    String dynamicJoinSources = dynamicJoinName.replace("Dynamic_Join_Sources", joinSourceTables);

                     // Append to final dynamic join sources
                    if (finalDynamicJoinSources.length() > 0) {
                        finalDynamicJoinSources.append("\n");
                    }
                    finalDynamicJoinSources.append(dynamicJoinSources);
                    // Below to be used in next iteration
                    joinComponent = finalDynamicJoinSources.toString();
                    previousJoinName = joinName; // From resultSet object
                }

                // Print or use finalDynamicJoinSources and previousJoinName. They are end Results.
                System.out.println("Join Component: " + joinComponent);
                System.out.println("Join Name: " + previousJoinName);

                return joinComponent;
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

        public List<Map<String, Object>> fetchDerivedColumnInfoByLevel(Connection conn, String expressionType, String level, String jobId, String dlId) throws SQLException {
            String query = SQLQueries.SELECT_ELT_DL_DERIVED_COLUMN_INFO;
            
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, expressionType);
                ps.setString(2, level);
                ps.setString(3, jobId);
                ps.setString(4, dlId);
                
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
                               "VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                insertPs.setString(1, rowDetails.get("DL_Id"));
                insertPs.setString(2, rowDetails.get("Job_Id"));
                insertPs.setString(2, rowDetails.get("DL_Name"));
                insertPs.setString(3, rowDetails.get("DL_Table_Name"));
                insertPs.setString(6, rowDetails.get("config_file_name"));
                insertPs.setString(5, rowDetails.get("DL_Active_Flag"));

                insertPs.executeUpdate();
                return true;
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
            System.out.println("Generating value script for DL_ID: " + dlId);
            boolean status = false;

//######################################
            // Job 1 Source
            // WORK IN PROGRESS

            // Job 2 SourceExecutesql
            // WORK IN PROGRESS

            // Job 3 lkp/join
            String componentJoin = "join";
            String joinValue = componentJoinValue(componentJoin); // Output

            // Job 4 Recoercing
            // WORK IN PROGRESS
            String componentEmptyRecoercing = "empty_recoercing";
            String XXXX = componentRecoercing(componentEmptyRecoercing);
            //String propsEmptyRecoercing = fetchAndFormatProperties(conn, componentEmptyRecoercing);

            // Job 5 NullReplacement
            String tgtPwd = ""; // TBD: TODO use input, replace literal $
            tgtPwd = tgtPwd.replace("$", "\\$");
            String componentNullReplacement = "'empty'";
            String emptyValueScript = componentNullReplacementValue(componentNullReplacement); 

            // Job 6 FilterValue
            String componentFilterGroupBy = "executesqlfiltergroupby";
            componentFilterGroupBy(componentEmptyRecoercing); // Two output values inside

            // Job 7 Expression
            String componentExpression = "'expression'";
            String expressionType = "JAVA";
            List<String> levels = getDistinctLevels(conn, jobId, dlId); // TODO name of output
            String expressionValue = executeExpressionChildComponentValue(conn, levels, jobId, dlId);

            // Job 8 Sql_Expression
            String componentSQLExpression = "'executesql'";
            String MappingSQLExpressionValue = componentSQLExpressionValue(componentSQLExpression); // TODO name of output

            // Job 9 remit
            String componentRemit = "'empty_remit'";
            String MappingRemitValue = componentRemitValue(componentRemit);

            // Job 10 Rename
            String componentRename = "'empty_rename'";
            String MappingRetainValue = componentRenameValue(componentRename);

            // Job 11 Sink
            // WORK IN PROGRESS

//######################################

            //config filename
            // script = result
            //TBD: to be filled up values built in previous steps
            String configFileName = "";
            String script = "";
            Map<String, String> rowDetails = selectActiveEltDlTableInfo(conn, dlId);
            if (rowDetails == null) {
                System.out.println("No record found with DL_Id: " + dlId);
                return Status.FAILURE;
            }
            rowDetails.put("JobId", jobId);
            rowDetails.put("value_file_name", configFileName);
            rowDetails.remove("DL_Version");
            // TBD where is the script used??
            //rowDetails.put("script", script);
            status = deleteFromEltDlValuesProperties(conn, dlId, dlId);
            status = insertIntoEltDlValuesProperties(conn, rowDetails);

            return status ? Status.SUCCESS : Status.FAILURE;
        }

        // Value Expression
        public List<String> getDistinctLevels(Connection connection, String jobId, String dlId){
            List<String> levels = new ArrayList<>();
            String query = "SELECT DISTINCT `ELT_DL_Derived_Column_Info`.`Level` " +
                        "FROM `ELT_DL_Derived_Column_Info` " +
                        "WHERE Expression_Type = 'JAVA' AND Job_Id = ? AND DL_Id = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, jobId);
                preparedStatement.setString(2, dlId);
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

        private String executeExpressionChildComponentValue(Connection connection, List<String> levels, String jobId, String dlId) {
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
                    String finalDerivedValue = processDerivedExpressions(connection, derivedValue, columnArguments, javaDataType, level, expressions, jobId, dlId);
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
        public List<Map<String, Object>> executeQueries(Connection connection, String level, String jobId, String dlId) throws SQLException {
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
                stmt1.setString(2, jobId);
                stmt1.setString(3, dlId);
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
                stmt2.setString(1, jobId);
                stmt2.setString(2, dlId);
                stmt2.setString(3, jobId);
                stmt2.setString(4, dlId);
                stmt2.setString(5, jobId);
                stmt2.setString(6, dlId);
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
        public String fetchColumnExpressions(Connection connection, String jobId, String dlId, String level) throws SQLException {
            
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
                statement.setString(1, jobId);
                statement.setString(2, dlId);
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
                String queryColumns = getQueryColumnNames(conn, jobId, dlId);
                executeSQLExpressionValue = fetchAndProcessColumnInfoForSQLExpression(conn, executeSqlValue, queryColumns, dlId, jobId); 
                // TODO queryColumns is updated inside aabove fn. and is an output        
            } catch (SQLException e) {
                // TODO Auto-generated catch block
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
                    String lastComponent = null;
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
                    return String.join(", ", queryColumns);;
                }
            }
        }
        // remit
        private String componentRemitValue(String component) {
            String mappingRemitValue = "";
            try {
                String mappingRetainValueRemit = getValueNamesFromJobPropertiesInfo(conn, component);
                mappingRemitValue = fetchAndProcessColumnInfoForRemit(conn, jobId, dlId, mappingRetainValueRemit);          
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
                mappingRenameValue = fetchAndProcessColumnInfo(conn, jobId, dlId, mappingRetainValue);          
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

        // Value 3. lkp/Join
        private String componentJoinValue(String component) {
            try {
                String joinValue = getValueNamesFromJobPropertiesInfo(conn, component);
                String tmpTable = dlName + dlId + jobId;
                joinValue = joinTablesAndFetchDerivedValues(conn, joinValue, tmpTable, jobId, dlId);            
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return joinValue;
        }

        public String joinTablesAndFetchDerivedValues(Connection connection, String joinValue, String tmpTable, String jobId, String dlId) throws SQLException {
            String sqlQuery = "SELECT " +
                    "    main.DL_Id, " +
                    "    main.Job_Id, " +
                    "    CONCAT(main.Table_Name_Alias, '_', main.Join_Table_Alias) AS Join_Name, " +
                    "    GROUP_CONCAT(main.Column_Name_Alias) AS Left_Hand_Fields, " +
                    "    GROUP_CONCAT(main.Join_Column_Alias) AS Right_Hand_Fields, " +
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
                    "        Column_Name_Alias, " +
                    "        Join_Column_Alias " +
                    "    FROM ELT_DL_Join_Mapping_Info " +
                    "    WHERE Job_Id = ? " +  // placeholder for Job_Id
                    "      AND DL_Id = ? " +   // placeholder for DL_Id
                    "    GROUP BY Table_Name_Alias, Join_Table_Alias " +
                    "    ORDER BY Join_Level " +
                    ") AS main " +
                    "LEFT JOIN ( " +
                    "    SELECT " +
                    "        LOWER(SUBSTRING_INDEX(table_name, '(', 1)) AS table_name, " +
                    "        Final_Table_Name, " +
                    "        property " +
                    "    FROM " + tmpTable +
                    "    WHERE property != 'db' " +
                    ") AS lookup2 " +
                    "ON main.Table_Name_Alias = lookup2.table_name " +
                    "LEFT JOIN ( " +
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
           // try {
                String emptyRecoercingValue = getValueNamesFromJobPropertiesInfo(conn, component);

            
            // } catch (SQLException e) {
            //     // TODO Auto-generated catch block
            //     e.printStackTrace();
            // }
            return "joinValue";
        }
        // Value 4. Recoercing
        public Map<String, Map<String, Object>> executeJoinQuery(Connection conn, String dlId, String jobId) throws SQLException {
            // TODO: Table_Name, Source_Name, Precision_Val and Scale_Val should be reviews in case of error
            String query = "SELECT DISTINCT " +
                           "m.DL_Id, " +
                           "'' AS Table_Name, " +
                           "'' AS Source_Name, " +
                           "m.DL_Column_Names, " +
                           "m.Constraints, " +
                           "m.DL_Data_Types, " +
                           "j.Join_Table, " +
                           "j.Join_Column_Alias, " +
                           "j.DL_Id, " +
                           "j.Job_Id " +
                           "SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(m.DL_Data_Types, '(', -1), ')', 1), ',', 1) AS Precision_Val, " +
                           "SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(m.DL_Data_Types, '(', -1), ')', 1), ',', -1) AS Scale_Val " +
                           "FROM ELT_DL_Mapping_Info_Saved m " +
                           "INNER JOIN ELT_DL_Join_Mapping_Info j " +
                           "ON m.DL_Id = j.DL_Id " +
                           "AND m.Job_Id = j.Job_Id " +
                           "AND m.DL_Column_Names = j.Join_Column_Alias " +
                           "WHERE m.DL_Id = ? " +
                           "AND m.Job_Id = ? " +
                           "AND m.DL_Column_Names NOT IN ( " +
                           "SELECT DISTINCT Column_Alias_Name " +
                           "FROM ELT_DL_Derived_Column_Info " +
                           "WHERE DL_ID = ? AND Job_Id = ?)";
    
            Map<String, String> lookupMap = getDataTypeConversions(conn);
            Map<String, Map<String, Object>> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, dlId);
                pstmt.setString(2, jobId);
                pstmt.setString(3, dlId);
                pstmt.setString(4, jobId);
    
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("DL_Id") + "-" + rs.getString("Job_Id") + "-" + rs.getString("Join_Column_Alias");
                        Map<String, Object> row = new HashMap<>();
                        row.put("DL_Id", rs.getString("DL_Id"));
                        row.put("DL_Column_Names", rs.getString("DL_Column_Names"));
                        row.put("Constraints", rs.getString("Constraints"));
                        row.put("DL_Data_Types", rs.getString("DL_Data_Types"));
                        row.put("Join_Table", rs.getString("Join_Table"));
                        row.put("Join_Column_Alias", rs.getString("Join_Column_Alias"));
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
                        String javaDataType = lookupMap.getOrDefault(dataType, "Unknown"); // TODO: what should be default value
                        row.put("Java_Data_Type", javaDataType);

                        // from Map 6
                        row.put("recoerce_to_format", dataType.contains("date")?"yyyy-MM-dd":"");
                        row.put("recoerce_to_type", javaDataType);
                        row.put("recoerce_decimal_precisions", precisionVal);
                        row.put("recoerce_decimal_scales", scaleVal);

                        resultMap.put(key, row);
                    }
                }
            }
            return resultMap;
        }
        // value 4;   Recoercing
        // TODO there is similar copy of it. check and see if one of them can be can be removed. (executeDataTypeConversionsQuery)
        public Map<String, String> getDataTypeConversions(Connection conn) throws SQLException {
            String query = "SELECT "
                    + "`ELT_Datatype_Conversions`.`Source_Data_Type`, "
                    + "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS IL_Data_Type, "
                    + "`ELT_Datatype_Conversions`.`Java_Data_Type` "
                    + "FROM `ELT_Datatype_Conversions`";
            
            Map<String, String> resultMap = new HashMap<>();
            try (PreparedStatement pstmt = conn.prepareStatement(query);
                ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String ilDataType = rs.getString("IL_Data_Type"); // TODO check the key
                    String javaDataType = rs.getString("Java_Data_Type");
                    resultMap.put(ilDataType, javaDataType);
                }
            }
            return resultMap;
        }

        // Value 6. filterGroupByValue
        private String componentFilterGroupBy(String component) {
            try {
                // Part 1
                String filterGroupByValue = getValueNamesFromJobPropertiesInfo(conn, component);
                // Part 2
                String settingsPosition = "'Join_Columns'";
                String scriptJoinFilterGroupby = getScriptFilterGroupByForSettingPosition(filterGroupByValue, settingsPosition); // output
                // Part 3
                settingsPosition = "'Derived_Columns'";
                String scriptDerivedFilterGroupby = getScriptFilterGroupByForSettingPosition(filterGroupByValue, settingsPosition);  // Output          
                // TODO: Part2 and part 3 are similar. A. settingsPosition is different. B. Internal final processing is different
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return "";
        }

        private String getScriptFilterGroupByForSettingPosition(String filterGroupByValue, String settingsPosition)
                throws SQLException {
            ResultSet rs = executeSelectFromEltDlFilterGroupByInfo(conn, settingsPosition, dlId, jobId);
            //  step 2a, 2b
            Map<String, AggregationData> mapGroupByData = executeSelectGroupByInfo(conn, settingsPosition, dlId, jobId);
            Map<String, Map<String, Object>> mapFilterData = executeFilterGroupByInfoQuery(conn, dlId, jobId, settingsPosition);
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
            try {
                getValueNamesFromJobPropertiesInfo(conn, component);
                List<Map<String, Object>> replacementMappingInfoList = fetchReplacementMappingInfo(conn, jobId, dlId);
                List<Map<String, Object>> dataTyperConversionList = executeDataTypeConversionsQuery(conn);
            
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return ""; //TODO
        }

        public List<Map<String, Object>> fetchReplacementMappingInfo(Connection conn, String jobId, String dlId) throws SQLException {
            String query = SQLQueries.SELECT_REPLACEMENT_MAPPING_INFO;
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setString(1, jobId);
                ps.setString(2, dlId);
                ps.setString(3, dlId);
                ps.setString(4, jobId);
        
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

        public List<Map<String, Object>> executeDataTypeConversionsQuery(Connection conn) throws SQLException {
            String query = "SELECT " +
                           "`ELT_Datatype_Conversions`.`Id`, " +
                           "`ELT_Datatype_Conversions`.`Source_Data_Type`, " +
                           "LOWER(SUBSTRING_INDEX(ELT_UI_Data_Type, '(', 1)) AS Data_Type, " +
                           "`ELT_Datatype_Conversions`.`Java_Data_Type`, " +
                           "PK_Cleansing_Value " +
                           "FROM `ELT_Datatype_Conversions`";
    
            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(query);
                ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("Id", rs.getObject("Id"));
                    row.put("Source_Data_Type", rs.getObject("Source_Data_Type"));
                    row.put("Data_Type", rs.getObject("Data_Type"));
                    row.put("Java_Data_Type", rs.getObject("Java_Data_Type"));
                    row.put("PK_Cleansing_Value", rs.getObject("PK_Cleansing_Value"));
                    results.add(row);
                }
            }
            return results;
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

        // value function
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
                               "VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                insertPs.setString(1, rowDetails.get("DL_Id"));
                insertPs.setString(2, rowDetails.get("Job_Id"));
                insertPs.setString(2, rowDetails.get("DL_Name"));
                insertPs.setString(3, rowDetails.get("DL_Table_Name"));
                insertPs.setString(6, rowDetails.get("value_file_name"));
                insertPs.setString(5, rowDetails.get("DL_Active_Flag"));

                insertPs.executeUpdate();
                return true;
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public Map<String, String> selectActiveEltDlTableInfo(Connection conn, String dlId) {
        String selectSql = SQLQueries.SELECT_ACTIVE_ELT_DL_TABLE_INFO;

        Map<String, String> rowDetails = new HashMap<>();

        try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
            selectPs.setString(1, dlId);
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

    public class DataMartAlterScriptGenerator {
        public DataMartAlterScriptGenerator() {
         }

        public Status generateAlterScript() {
            boolean status = false;
            System.out.println("Generating alter script for DL_ID: " + dlId);
            // Step 1:
            status = updateActiveFlag(conn, dlId);
            // Format of the dateTime is supposed to be "yyyy-MM-dd HH:mm:ss", Hence TimeStamp is used.
            Timestamp dateTime = getMaxUpdatedDate(conn, dlId);
            boolean dlIdExists = checkDLIdExists(conn, dlId);
            //TBD: Note: Below check moved earlier to be reused elsewhere
            String TargetDB = ""; // Input Param
            boolean tableExists = doesTableExist(conn, dlName, TargetDB);
            // Step 2:
            if (dlIdExists) {
                // call Alter delete funcitons
                String dlName = ""; // Input Param
                if (tableExists) {
                    System.out.println("Table Exists. Execute Alter Statement.");
                    status = fetchUniqueMappingInfoAndInsertIntoAlterScriptInfo(conn, dlId);
                } else {
                    System.out.println("Table Doesn't Exists. Proceed with Create Statement.");
                    // TBD: This case is not specified properly. Is it redundant?
                }
            }
            // Step 3:
            boolean hasRecentUpdateForDLId = checkDLIdExistsWithUpdatedDate(conn, dlId, dateTime);
            if (hasRecentUpdateForDLId) {
                // call Alter Services
                getPKColumnNames(conn, dlId);
                buildChangeColumnNotNullQuery(conn, dlId, dlName);
                buildChangeColumnNullQuery(conn, dlId, dlName);
            }

            // Step 4:
            // TBD: This function must be changed to set either value 0,1
            status = updateActiveFlag(conn, dlId);
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
            Timestamp maxUpdatedDate = getMaxUpdatedDate(conn, dlId); // TBD: isn't dlId and dl_name are 1-1 mapped. THis call is with dlName

            // Step 8:
            buildAndExecuteCompleteAlterScript(conn, dlId, maxUpdatedDate);

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
            stmt.setString(1, dlName);

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
            stmt.setString(1, dlName);

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

                    if (!"".equals(combinedAlterScriptDefBuilder)) {
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
            status = deleteFromEltDlMappingInfo(conn, dlId);
            status = insertMappingInfoFromSaved(conn, dlId);

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
    public boolean deleteFromEltDlValuesProperties(Connection conn, String dlId, String jobId) {
        String sql = SQLQueries.DELETE_FROM_ELT_DL_VALUES_PROPERTIES;        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dlId);
            ps.setString(2, jobId);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;            
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // deleting records from ELT_DL_CONFIG_PROPERTIES
    public boolean deleteFromEltDlConfigProperties(Connection conn, String dlId, String jobId) {
        String sql = SQLQueries.DELETE_FROM_ELT_DL_CONFIG_PROPERTIES;        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, dlId);
            ps.setString(2, jobId);
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
            System.out.println("Generating create script for DL_ID: " + dlId);
            getDataSourceType();
            dataSourceType = DataSourceType.MYSQL;
            getDlId();
            return Status.SUCCESS;
         }
    }

    public static class SQLQueries {
        // Query to drop a table
        public String getDropTableQuery(String dlName, String dlId, String jobId) {
            return "DROP TABLE IF EXISTS `" + dlName + dlId + jobId + "`";
        }

        // SQL Query to update the Active_flag in ELT_DL_Alter_Script_Info table
        public static final String UPDATE_ACTIVE_FLAG = "UPDATE ELT_DL_Alter_Script_Info SET Active_flag = 0 WHERE DL_Id = ?";

        public static final String SELECT_MAX_UPDATED_DATE = "SELECT MAX(Updated_Date) FROM ELT_DL_Mapping_Info WHERE DL_Id = ?";

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
                        // TBD: refer above counterpart query. Optionally remove this if matching data types isn't required:
                        // " AND main.DL_Data_Types = lookup.DL_Data_Types " +
                        "WHERE " +
                        "    main.DL_Id = ? " + // DL_Id as a parameter
                        "    AND main.DL_Name = ? " + // DL_Name as a parameter
                        "    AND main.Constraints = 'PK' " +
                        "ORDER BY main.DL_Column_Names";

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

        // Value 
        public static final String SELECT_VALUE_NAMES_FROM_ELT_JOB_PROPERTIES_INFO = 
                "SELECT DISTINCT `ELT_Job_Properties_Info`.`Value_Name` " +
                "FROM `ELT_Job_Properties_Info` " +
                "WHERE Job_Type='DL' AND Component IN (?) " +
                "AND Active_Flag=1 AND Dynamic_Flag=1";
        
        public static String SELECT_REPLACEMENT_MAPPING_INFO = 
                "SELECT DISTINCT " +
                "    '' AS Table_Name, " +
                "    m.DL_Column_Names, " +
                "    m.Constraints, " +
                "    '' AS Source_Name, " +
                "    LOWER(SUBSTRING_INDEX(m.DL_Data_Types, '(', 1)) AS Data_Type, " +
                "    m.DL_Id, " +
                "    ? AS Job_Id " +
                "FROM ELT_DL_Mapping_Info_Saved m " +
                "INNER JOIN ELT_DL_Join_Mapping_Info j " +
                "    ON m.DL_Id = j.DL_Id " +
                "    AND m.Job_Id = j.Job_Id " +
                "    AND m.DL_Column_Names = j.Join_Column_Alias " +
                "WHERE m.Constraints IN ('Pk', 'SK') " +
                "    AND m.DL_Id = ? " +
                "    AND m.DL_Column_Names NOT IN ( " +
                "        SELECT DISTINCT Column_Alias_Name " +
                "        FROM ELT_DL_Derived_Column_Info " +
                "        WHERE DL_ID = ? AND Job_Id = ? " +
                "    )";

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
        public static String buildFullJoinQuery(String table) {
            return "SELECT DISTINCT " +
                    "ELT_DL_Join_Mapping_Info.Table_Name, " +
                    "ELT_DL_Join_Mapping_Info.Join_Table_Alias, " +
                    "ELT_DL_Join_Mapping_Info.Table_Name_Alias, " +
                    "CONCAT(ELT_DL_Join_Mapping_Info.Table_Name_Alias, '_', ELT_DL_Join_Mapping_Info.Join_Table_Alias) AS Join_Name, " +
                    table + ".Final_Table_Name AS Final_Table_Name, " +
                    table + ".property AS Property " +
                    "FROM ELT_DL_Join_Mapping_Info " +
                    "LEFT OUTER JOIN " + table + " " +
                    "ON (ELT_DL_Join_Mapping_Info.Table_Name_Alias = " + table + ".table_name AND " + table + ".property != 'db') " +
                    "OR (ELT_DL_Join_Mapping_Info.Join_Table_Alias = " + table + ".table_name AND " + table + ".property != 'db') " + 
                    "WHERE ELT_DL_Join_Mapping_Info.Job_Id = ? " +
                    "AND ELT_DL_Join_Mapping_Info.DL_Id = ? " +
                    "ORDER BY ELT_DL_Join_Mapping_Info.Join_Level";
        }
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
                    String mysqlUrl = "jdbc:mysql://localhost:3306/databasename?noDatetimeStringSync=true";
                    String mysqlUser = "userName";
                    String mysqlPassword = "password";
                    connection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                                    
                    // Enable auto-commit
                    connection.setAutoCommit(true);
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
        public String getDefaultForDataType(String dataType) {
            switch (dataType.toLowerCase()) {
                case "varchar":
                case "text":
                case "char":
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
    }

    private void executeDropTableQuery(Connection conn, String dlName, String dlId, String jobId) {
        String sql = sqlQueries.getDropTableQuery(dlName, dlId, jobId);
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Table dropped: " + dlName + dlId + jobId);
        } catch (SQLException e) {
            System.err.println("Error executing drop table query: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String tableId = "XNXNXN";
        DataMartStructureScriptGenerator generator = new DataMartStructureScriptGenerator(DataSourceType.MYSQL, tableId);
        generator.generateScripts();
    }
}
