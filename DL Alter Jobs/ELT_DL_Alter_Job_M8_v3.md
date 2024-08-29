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


## Component 1: Statement for `Primary Key` PK Columns

- From both the tables, primary key records are fetcched.
- The values in the DL_Column_Names field must be enclosed in backticks (``) as required by MySQL for correct column name referencing. This modified value is stored in the derived column tilt_columns.
- Aggregate on the column `DL_ID`. 
- output column is DL_Column_Names. make a list of data of this field.

 <details>
<summary>Additional Details</summary>

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

### Map data 1

- The output fields are "Change_Flag" and "PKColumns". These fields are used later.

| Name         | Type   | Expression                                   | isNullable |
|--------------|--------|----------------------------------------------|------------|
| PKColumns    | String | ELT_DL_Mapping_Info_Saved.tilt_column_names                       | false      |
| Change_Flag  | String | ELT_DL_Mapping_Info.DL_Column_Names == null ? "Y" : "N"     | false      |

 </details>


## Component 2: Statement for `CHANGE COLUMN` with `NOT NULL`)

- Relevant data is extracted from `ELT_DL_Mapping_Info_Saved` (Set A) and `ELT_DL_Mapping_Info` (Set B).
    - select fields (DL_Id, DL_Column_Names, tilt_columns, DL_Data_Types, Constraints)
    - Retrieve rows related to primary keys in Set A.
    - make NULL constraint as Empty string (likely required in only Set B)
    - The values in the DL_Column_Names field must be enclosed in backticks (``) as required by MySQL for correct column name referencing. This modified value is stored in the derived column `tilt_columns`.
- Map the data as described below. Aggregate the data on `DL_ID`. Choose the last row as it has comprehensive details of DL_Column_Names.
- The above `statement` is used in a later step. [NOT_NULL_FINAL_STATEMENT]
 <details>
<summary>Additional Details</summary>

### Data extracted from `ELT_DL_Mapping_Info_Saved`. 
```sql
SELECT 
  `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,
  concat('`',`ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,'`') as tilt_columns,
  DL_Data_Types,
  case when Constraints is null then '' else Constraints end as Constraints
  FROM `ELT_DL_Mapping_Info_Saved` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' and Constraints='PK' order by DL_Column_Names
```
### Data extracted from `ELT_DL_Mapping_Info`
```sql
SELECT 
  `ELT_DL_Mapping_Info`.`DL_Id`, 
  `ELT_DL_Mapping_Info`.`DL_Column_Names`,
  concat('`',`ELT_DL_Mapping_Info`.`DL_Column_Names`,'`') as tilt_columns,
  DL_Data_Types,
  case when Constraints is null then '' else Constraints end as Constraints
  FROM `ELT_DL_Mapping_Info` where DL_Id='"+DL_Id+"' and DL_Name='"+DL_Name+"' order by DL_Column_Names
```
 ### Map 1

 - from the above two data sets A & B, `Change Column` Sql statement is formed.  Value is saved in `final_statement`.
    - Append `NOT NULL` to the DL_Column_Names values from set A if the corresponding constraint in set B is an empty string ("").
 - Values in Subsequent rows contains values of all the previous rows. 



Reference expressions. The SetA refers to first set of data. The SetB refers to second set of data.
```sql
  SetB.DL_Data_Types ==null ?null:
  (((StringHandling.DOWNCASE(SetA.Constraints ).contains("pk"))&&SetB.Constraints.equals("")) ? 

  "Change column "+SetA.tilt_columns+ " "+SetA.tilt_columns+" "+SetA.DL_Data_Types+" not NULL ":null ) 


  Var.final_statement==null? Var.condition :
  (Var.condition==null? Var.final_statement : Var.final_statement+","+Var.condition ) 
```
</details>


## Component 3: Statement for `CHANGE COLUMN` with `NULL`

It is very similar to Component 2, with the roles of the two tables reversed. Additionally, the `CHANGE COLUMN` clause is used in conjunction with `NULL`.

- Relevant data is extracted from `ELT_DL_Mapping_Info` (Set A) and `ELT_DL_Mapping_Info_Saved` (Set B).
    - select fields (DL_Id, DL_Column_Names, tilt_columns, DL_Data_Types, Constraints)
    - Retrieve rows related to primary keys in Set A.
    - make NULL constraint as Empty string (likely required in only Set B)
    - The values in the DL_Column_Names field must be enclosed in backticks (``) as required by MySQL for correct column name referencing. This modified value is stored in the derived column `tilt_columns`.
-  Map the data as described below. Aggregate the data on `DL_ID`. Choose the last row as it has comprehensive details of DL_Column_Names.
- The above `statement` is used in a later step. [NULL_FINAL_STATEMENT]

<details>
<summary>Additional Details</summary>

  ### Data extracted from `ELT_DL_Mapping_Info` (Set A)
  ```sql
  SELECT 
    `ELT_DL_Mapping_Info`.`DL_Id`, 
    `ELT_DL_Mapping_Info`.`DL_Column_Names`,
    concat('`',`ELT_DL_Mapping_Info`.`DL_Column_Names`,'`') as tilt_columns,
    DL_Data_Types,
    case when Constraints is null then '' else Constraints end as Constraints
    FROM `ELT_DL_Mapping_Info` where DL_Id='"+context.DL_Id+"' and DL_Name='"+context.DL_Name+"' and Constraints='PK' order by DL_Column_Names
  ```

  ### Data extracted from `ELT_DL_Mapping_Info_Saved` (Set B)
  ```sql
  SELECT 
    `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
    `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,
    concat('`',`ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`,'`') as tilt_columns,
    DL_Data_Types,
    case when Constraints is null then '' else Constraints end as Constraints
    FROM `ELT_DL_Mapping_Info_Saved` where DL_Id='"+context.DL_Id+"' and DL_Name='"+context.DL_Name+"' order by DL_Column_Names
  ```

  ### Map 1

  - from the above two data sets A & B, `Change Column` Sql statement is formed. Value is saved in `final_statement`.
      - Append ` NULL` to the DL_Column_Names values from set A if the corresponding constraint in set B is an empty string ("").
  - Values in Subsequent rows contains values of all the previous rows. 



  Reference expressions. The SetA refers to first set of data. The SetB refers to second set of data.
  ```sql
    SetB.DL_Data_Types ==null ?null:
    (((StringHandling.DOWNCASE(SetA.Constraints ).contains("pk"))&&SetB.Constraints.equals("")) ? 

    "Change column "+SetA.tilt_columns+ " "+SetA.tilt_columns+" "+SetA.DL_Data_Types+" NULL ":null ) 
  ```
</details>


#####################################################

## Appendix A

Schematic diagram of the component (Talend job).

![schematic diagram](./ELT_DL_Alter_Job_M8_v3_0.1.png "ELT_DL_Alter_Job_M8_v3")


## Appendix B

List of all Context Variables.


                                                      




