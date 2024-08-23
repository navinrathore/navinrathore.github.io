## DB Connection 

Once the Input parameters are read into the system, Database Connection is established. All the necessary information to create connection, namely, url, username, password, properties etc. are available as input parameters. 

Auto commit should be set.

At the end of operation, the above database connection should be properly closed.

<details>
<summary>Addtional info</summary>
Below are the details to setup a mysql connection

```java
properties_string = "noDatetimeStringSync=true"
dbUrl = "jdbc:mysql://" + context.APP_HOST + ":" + context.APP_PORT + "/" + context.APP_DBNAME + "?" + properties_string;
...

```
context.APP_UN, decryptedPassword_tDBConnection_1
</details>

## Read Data from ELT_DL_Mapping_Info_Saved
Following  columns from the table **ELT_DL_Mapping_Info_Saved** are fetched for the input param **DL_Id**:

`DL_Id`, `DL_Name`, `DL_Column_Names`, `Constraints`, `DL_Data_Types`
All the data in columns of type String and Char must be trimmed
<details>
<summary>details</summary>
Query

``` sql
SELECT 
    `ELT_DL_Mapping_Info_Saved`.`DL_Id`, 
    `ELT_DL_Mapping_Info_Saved`.`DL_Name`, 
    `ELT_DL_Mapping_Info_Saved`.`DL_Column_Names`, 
    `ELT_DL_Mapping_Info_Saved`.`Constraints`, 
    `ELT_DL_Mapping_Info_Saved`.`DL_Data_Types` 
FROM 
    `ELT_DL_Mapping_Info_Saved` 
WHERE 
    `DL_Id` = '" + DL_Id + "'
```

</details>

## Map Data to store

In the above step 5 fields in every row data are read. Using these fields below fields are mapped.



| Name	| Type	| isNullable	| Details|
| --- | ---| ---| ---|
| create	| String	| TRUE	| 	|
| Table_Name | String | TRUE	| 	|
| Column_Names | String | TRUE	| See details	|
| PKs | String | TRUE	| See details	|
| SK | String | TRUE	| See details	|
| DL_Id | Long | TRUE	| 	|

create
```
this.Table_Name = row.DL_Name 
create = "CREATE TABLE IF NOT EXISTS "+ this.Table_Name+" ("
```
Table_Name
```
this.Table_Name = row.DL_Name 
```

PKs (see details)
```java
If Constraint is 'PK',  
    PKs = row.Column_Names + ","
else
    PKs = null //default value
```

SK (see details)
```java
If Constraint is 'SK',
    SK = row.Column_Names + ","
else
    SK = null //default value
```

Column_Names (see details)
```python
Column_Names = "\n" + row.DL_Column_Names+" "+row.DL_Data_Types + " " 
[If datatype is varchar] -  + "COLLATE utf8_unicode_ci"
    else                    + " "
[If constraint is 'pk']  -  + "NOT NULL DEFAULT "  + DEFAULT_VALUE_FOR_TYPE
    else                 - +  " DEFAULT NULL"

Default values could be one of these based on the datatype
varchar -'',
int - 0,
decimal -  0.0,
float -  0.0,
boolean - 0,
date - '0000-00-00'
OTHER - " "
```


DL_Id
```
DL_Id = row.DL_Id
```


<details>

Below are the expressions configured in talend for the above properties.

| Name         | Expression                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Table_Name   | row1.DL_Name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| create       | "CREATE TABLE IF NOT EXISTS " + Var.Table_Name + " ("                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PKs          | StringHandling.DOWNCASE(row1.Constraints).equals("pk") ? (Var.PKs == null ? "" : Var.PKs) + row1.DL_Column_Names + "," : Var.PKs                                                                                                                                                                                                                                                                                                                                                                |
| PKs1         | Var.PKs == null ? "" : Var.PKs                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| SKs          | StringHandling.DOWNCASE(row1.Constraints).equals("sk") ? (Var.SKs == null ? "" : Var.SKs) + row1.DL_Column_Names + "," : Var.SKs                                                                                                                                                                                                                                                                                                                                                                |
| SKs1         | Var.SKs == null ? "" : Var.SKs                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Column_Names | "\n" + row1.DL_Column_Names + " " + row1.DL_Data_Types + " " + (StringHandling.DOWNCASE(row1.DL_Data_Types).startsWith("varchar") ? "COLLATE utf8_unicode_ci" : " ") + " " + (StringHandling.DOWNCASE(row1.Constraints).equals("pk") ? (" NOT NULL DEFAULT " + (StringHandling.DOWNCASE(row1.DL_Data_Types).startsWith("varchar") ? "''" : (StringHandling.DOWNCASE(row1.DL_Data_Types).contains("int") ? "0" : (StringHandling.DOWNCASE(row1.DL_Data_Types).contains("decimal") ? "'0.



</details>




