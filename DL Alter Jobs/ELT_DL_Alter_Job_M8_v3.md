# Data Mart: Alter Job (ELT_DL_Alter_Job_M8_v3)

## Intent

Alter Table  to delete all the columns for the given `DL_ID`.

## Input Specifications
The component or service is dependent on the following input data:

- App DB Connection Details
- Target DB Connection Details (TGT_DBNAME ...)
- DL_Name - table name (optional)

## DB Connection 

Once the Input parameters are read into the system, the database connection is established using the provided details — such as URL, username, password, and properties. 

 - Auto commit should be enabled for App DB Connection
 - The database connection must be properly closed at the end of the operation.
 - Additional JDBC Parameters:
    * noDatetimeStringSync=true
    * allowMultiQueries=true


## Component 1 (Statement for PK Columns)

- Aggregate on the column `DL_ID`. 
- output column is DL_Column_Names. make a list of data of this field.

```sql
SELECT 
  `ELT_DL_Mapping_Info`.`DL_Id`, 
  `ELT_DL_Mapping_Info`.`DL_Column_Names`
FROM `ELT_DL_Mapping_Info` where DL_Id='"+ DL_Id+"' and Constraints='PK' order by DL_Column_Names
```

- Aggregate on the column `DL_ID`. 
- output columns are DL_Column_Names, tilt_columns. Make a list of data in thse fields. 

```sql
SELECT 
  `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,
  concat('`',`ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,'`') as tilt_columns
FROM `ELT_DL_Mapping_Info_Saved` where DL_Id='"+context.DL_Id+"' and Constraints='PK' order by DL_Column_Names
```

Map data 1

- The output fields are "Change_Flag" and "PKColumns". Track these fields for later use.

| Name         | Type   | Expression                                   | isNullable |
|--------------|--------|----------------------------------------------|------------|
| PKColumns    | String | ELT_DL_Mapping_Info_Saved.tilt_column_names                       | false      |
| Change_Flag  | String | ELT_DL_Mapping_Info.DL_Column_Names == null ? "Y" : "N"     | false      |


## Component 2 (Final Statement with Constraints)

Data extracted from `ELT_DL_Mapping_Info_Saved`. 
```sql
"SELECT 
  `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,
concat('`',`ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,'`') as tilt_columns,
DL_Data_Types,
case when Constraints is null then '' else Constraints end as Constraints
FROM `ELT_DL_Mapping_Info_Saved` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' and Constraints='PK' order by DL_Column_Names"
```
Data extracted from `ELT_DL_Mapping_Info`
```sql
"SELECT 
  `ELT_DL_Mapping_Info`.`DL_Id`, 
  `ELT_DL_Mapping_Info`.`DL_Column_Names`,
  concat('`',`ELT_DL_Mapping_Info`.`DL_Column_Names`,'`') as tilt_columns,
DL_Data_Types,
case when Constraints is null then '' else Constraints end as Constraints
FROM `ELT_DL_Mapping_Info` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' order by DL_Column_Names"
```



===================================================

### Fetch the records from the two Mapping Info tables

- Check if table `DL_Name` exists in the target DB. If it exists, execute alter statement on the table else execute create statement.

  Sample query to check if the table exists in the DB or not
```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_type <> 'VIEW' 
  AND table_schema = 'TGT_DBNAME' 
  AND (table_name='DL_Name')
ORDER BY table_name ASC;
```
* Fetch the selected fields from the table `ELT_DL_Mapping_Info` as mentioned below.
* Do the same step for the table `ELT_DL_Mapping_Info_Saved`
```sql
SELECT 
  `ELT_DL_Mapping_Info`.`DL_Id`, 
  `ELT_DL_Mapping_Info`.`DL_Name`, 
  `ELT_DL_Mapping_Info`.`DL_Column_Names`, 
  `ELT_DL_Mapping_Info`.`Constraints`, 
  `ELT_DL_Mapping_Info`.`DL_Data_Types`
FROM `ELT_DL_Mapping_Info`
where DL_Id='DL_Id'
```

### Map Data 1

Find unique columns from lookup tables `ELT_DL_Mapping_Info_Saved` and `ELT_DL_Mapping_Info` using Inner Join 

There are four fields in the output. The script field uses the field `DL_Column_Names` value.

| Name             | Type   | Expression                                                      | isNullable |
|------------------|--------|-----------------------------------------------------------------|------------|
| DL_Id            | Long   | ELT_DL_Mapping_Info.DL_Id                                       | true       |
| DL_Name          | String | ELT_DL_Mapping_Info.DL_Name                                     | true       |
| DL_Column_Names  | String | ELT_DL_Mapping_Info.DL_Column_Names                             | true       |
| Script           | String | "Drop Column `ELT_DL_Mapping_Info.DL_Column_Names`     | true       |


### Map Data 2

There are three fields in the output.


| Name         | Type   | Expression                    | isNullable |
|--------------|--------|-------------------------------|------------|
| DL_Id        | Long   | DL_Id            | true       |
| DL_Name      | String | DL_Name          | true       |
| Alter_Script | String | DL_Alter_Script (See details beow)         | false      |

 
The Alter script is formed to drop columns. The list of columns can be dropped in a single command or in multiple commands. Different databases support different syntax. Form the script, accordingly.

```sql
ALTER TABLE x DROP COLUMN y, DROP COLUMN z;
OR
ALTER TABLE x DROP COLUMN y;
ALTER TABLE x DROP COLUMN z;
```

## Component 3 (Final Statement with Constraints)

Data extracted from `ELT_DL_Mapping_Info_Saved`. 
```sql
"SELECT 
  `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,
concat('`',`ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,'`') as tilt_columns,
DL_Data_Types,
case when Constraints is null then '' else Constraints end as Constraints
FROM `ELT_DL_Mapping_Info_Saved` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' and Constraints='PK' order by DL_Column_Names"
```
Data extracted from `ELT_DL_Mapping_Info`
```sql
"SELECT 
  `ELT_DL_Mapping_Info`.`DL_Id`, 
  `ELT_DL_Mapping_Info`.`DL_Column_Names`,
  concat('`',`ELT_DL_Mapping_Info`.`DL_Column_Names`,'`') as tilt_columns,
DL_Data_Types,
case when Constraints is null then '' else Constraints end as Constraints
FROM `ELT_DL_Mapping_Info` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' order by DL_Column_Names"
```



#####################################################
<details>
<summary>Additional Details</summary>
Formation of DL_Alter_Script:

| Name             | Expression                                                                      |
|------------------|---------------------------------------------------------------------------------|
| Drop_column      | Result.Script|
| Final_Drop_Column| Final_Drop_Column == null ? Drop_column + "," : Final_Drop_Column + Drop_column + "," |
| Script           | "ALTER TABLE " + DL_Name + " " + Final_Drop_Column                          |
| DL_Alter_Script | StringHandling.LEFT(Script, (StringHandling.LEN(Script) - 1)) + ";"                  |

</details>

### Aggregation of the rows

The alter script field of the last record is complete with the information of all columns that need to be dropped. 
Therefore, the rows generated in previous step have to be aggregated on `DL_ID`, `DL_NAME` columns and `Alter_Script` from the last row is selected. SQL clause Order_by alongwith LIMIT may be used.

### Addtional fields
Addtional fields are updated with appropriate values - Active_Flag (true),  Added_Date, Added_User, Updated_Date, Updated_User

## Store the Generated Data

The destination for the generated data is the table `ELT_DL_Alter_Script_Info` of Target DB.


## Appendix A

Schematic diagram of the component (Talend job).

![schematic diagram](./ELT_DL_Alter_Job_M8_v3_0.1.png "ELT_DL_Alter_Job_M8_v3")


## Appendix B

List of all Context Variables.


                                                      




