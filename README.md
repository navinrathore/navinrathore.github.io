# navinrathore.github.io

* Docker
  - Docker networking [https://www.geeksforgeeks.org/connecting-two-docker-containers-over-the-same-network/?ref=lbp]
* github pages


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
| create	| String	| TRUE	| Var.create	|
| Table_Name | String | TRUE	| Var.create	|
| Column_Names | String | TRUE	| Var.create	|
| PKs | String | TRUE	| Var.create	|
| SK | String | TRUE	| Var.create	|
| DL_Id | Long | TRUE	| Var.create	|

create
```
this.Table_Name = row.DL_Name 
create = "CREATE TABLE IF NOT EXISTS "+ this.Table_Name+" ("
```
Table_Name
```
this.Table_Name = row.DL_Name 
```
DL_Id
```
DL_Id = row.DL_Id
```






<details>

</details>


