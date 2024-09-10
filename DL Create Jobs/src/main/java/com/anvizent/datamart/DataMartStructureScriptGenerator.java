package com.anvizent.datamart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

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

            // Job 2 lkp/join
            
            // Job 3 Recoercing
            
            // Job 4 NullReplacement
            
            // SubJob GroupbyJoin
            String previousComponent = ""; // TBD
            String componentFilterGroupBy = "executesqlfiltergroupby";
            String filterGroupBy = fetchAndFormatProperties(conn, componentFilterGroupBy);
            String dynamicFilterGroupByName = filterGroupBy.replace("Dynamic_FilterGroupby_Name", "Join_Aggregation");
            String dynamicFilterGroupBySource = dynamicFilterGroupByName.replace("Dynamic_FilterGroupby_Source", previousComponent);
            String previousName = "Join_Aggregation";
            String joinDynamicGroupbyFilterConfig = dynamicFilterGroupBySource;
            System.out.println("Previous Name: " + previousName);
            System.out.println("Derived Dynamic Groupby Filter Config: " + joinDynamicGroupbyFilterConfig);

            // Job 6 Derived
            
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
            
            // Job 8 remit
            
            // Job 9 Rename
            
            // Job 10 Sink
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

            // Job 2 SourceExecutesql

            // Job 3 lkp/join

            // Job 4 Recoercing

            // Job 5 NullReplacement

            // Job 6 FilterValue

            // Job 7 Expression

            // Job 8 Sql_Expression

            // Job 9 remit

            // Job 10 Rename

            // Job 11 Sink
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
