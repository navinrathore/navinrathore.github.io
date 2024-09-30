package com.anvizent.datamart;

class SQLQueries {

    /**
     * SQL Query for retrieving detailed information from the ELT_DL_Mapping_Info_Saved table.
     * The query retrieves the DL_Id, DL_Name, DL_Column_Names, Constraints, and DL_Data_Types columns
     * for a specific DL_Id. The DL_Id is used as a parameter to filter the results.
     */
    public static final String SELECT_DETAILS_FROM_ELT_DL_MAPPING_INFO_SAVED_QUERY =
        "SELECT " +
        "    `ELT_DL_Mapping_Info_Saved`.`DL_Id`, " +
        "    `ELT_DL_Mapping_Info_Saved`.`DL_Name`, " +
        "    `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`, " +
        "    `ELT_DL_Mapping_Info_Saved`.`Constraints`, " +
        "    `ELT_DL_Mapping_Info_Saved`.`DL_Data_Types` " +
        "FROM " +
        "    `ELT_DL_Mapping_Info_Saved` " +
        "WHERE " +
        "    `DL_Id` = ?";

    // SQL Query for deleting records
    public static final String DELETE_FROM_ELT_DL_CREATE_INFO_QUERY = 
    "DELETE FROM ELT_DL_Create_Info WHERE DL_Name = ?";

    // SQL Query for inserting records
    public static final String INSERT_INTO_ELT_DL_CREATE_INFO_QUERY =
    "INSERT INTO ELT_DL_Create_Info (DL_ID, DL_NAME, script) VALUES (?, ?, ?)";

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

    // SQL Query to perform the left outer join between ELT_DL_Mapping_Info_Saved
    // and ELT_DL_Mapping_Info
    public static final String JOIN_OUTER_ELT_DL_MAPPING_INFO_SAVED_AND_INFO = 
    // TBD: Performance - all are not keys?
            "SELECT DISTINCT " +
            "    saved.DL_Id, " +
            "    saved.DL_Column_Names, " +
            "    CONCAT('`', saved.DL_Column_Names, '`') AS tilt_columns, " + // Used
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
           // "    AND saved.DL_Data_Types = lookup.DL_Data_Types " + #TBD this check is not required??
           // TBD Check what should eb the functionality, check on types will become very restrictive
            "WHERE " +
            "    saved.DL_Id = ? " + // DL_Id as a parameter
            "    AND saved.DL_Name = ? " + // DL_Name as a parameter
            "    AND saved.Constraints = 'PK' " +
            "ORDER BY saved.DL_Column_Names";
    
}