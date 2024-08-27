# Data Mart: Alter Delete (ELT_DL_Alter_Delete_Component)

## Intent

Alter or Create Table

//Purpose of this component is to copy all the records corresponding to the input `DL_ID` from the //table `ELT_DL_Mapping_Info_Saved` to the table `ELT_DL_Mapping_Info`.

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



## Map Data 1

Inner Join 

There are four fields in the output. The script field uses the field `DL_Column_Names` value.

| Name             | Type   | Expression                                                      | isNullable |
|------------------|--------|-----------------------------------------------------------------|------------|
| DL_Id            | Long   | ELT_DL_Mapping_Info.DL_Id                                       | true       |
| DL_Name          | String | ELT_DL_Mapping_Info.DL_Name                                     | true       |
| DL_Column_Names  | String | ELT_DL_Mapping_Info.DL_Column_Names                             | true       |
| Script           | String | "Drop Column `" + ELT_DL_Mapping_Info.DL_Column_Names + "`"     | true       |
```
the expression is formed to comprise all the information to create a table. Refer Details.

create table clause + columns names + Primary key + Secondary key + additional parameters

ENGINE=InnoDB 
DEFAULT CHARSET=utf8 
COLLATE=utf8_unicode_ci

```


















<details>
<summary>Additional Details</summary>

In the talend job, following fields are fetched and copied

```sql
"SELECT 
  `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Name`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`, 
  `ELT_DL_Mapping_Info_Saved`.`Constraints`, 
  `ELT_DL_Mapping_Info_Saved`.`DL_Data_Types`, 
  `ELT_DL_Mapping_Info_Saved`.`Column_Type`, 
  `ELT_DL_Mapping_Info_Saved`.`Added_Date`, 
  `ELT_DL_Mapping_Info_Saved`.`Added_User`, 
  `ELT_DL_Mapping_Info_Saved`.`Updated_Date`, 
  `ELT_DL_Mapping_Info_Saved`.`Updated_User`
FROM `ELT_DL_Mapping_Info_Saved`
where DL_Id='"+context.DL_Id+"'"
```
Before inserting the records, all existing relevant records are purged.

```sql
     "Delete from ELT_DL_Mapping_Info  where DL_Id='"+context.DL_Id+"'"
```
There is direct copies of all the fields fectched from the DB in the previous Step into the table `ELT_DL_Mapping_Info`. 


The schematic of the job is shown in the [attached diagram](#appendix-a).

</details>

## Appendix A

Schematic diagram of the component (Talend job). Java service 

![schematic diagram](./ELT_DL_Saved_Info_M8_v1_0.png "ELT_DL_Saved_Info_M8_v1")


## Appendix B

List of all Context Variables.


                                                      




