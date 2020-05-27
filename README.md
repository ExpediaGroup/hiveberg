# Hive integration with [Iceberg](https://iceberg.apache.org/)

##  Overview
The intention of this project is to demonstrate how Hive could be integrated with Iceberg. For now it provides a Hive Input Format which can be used to *read* 
data from an Iceberg table. There is a unit test which demonstrates this. 

There are a number of dependency clashes with libraries used by Iceberg (e.g. Guava, Avro) 
that mean we need to shade the upstream Iceberg artifacts. To use Hiveberg you first need to check out our fork of Iceberg from 
https://github.com/ExpediaGroup/incubator-iceberg/tree/build-hiveberg-modules and then run
```
./gradlew publishToMavenLocal
```
Ultimately we would like to contribute this Hive Input Format to Iceberg so this would no longer be required.

## Features
### IcebergInputFormat

To use the `IcebergInputFormat` you need to create a Hive table using DDL:
```sql
CREATE TABLE source_db.table_a
  ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe'
  STORED AS
    INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  LOCATION 'path_to_iceberg_table_location'
  TBLPROPERTIES('iceberg.catalog' = ...);
```
You must ensure that your Hive table has the **same** name as your Iceberg table. The `InputFormat` uses the `name` property from the Hive table to load the correct Iceberg table for the read.
For example, if you created the Hive table as shown above then your Iceberg table must be created using a `TableIdentifier` as follows where both table names match: 
```
TableIdentifier id = TableIdentifier.parse("source_db.table_a");
```

#### Setting TBLPROPERTIES
You need to set one additional table property when using Hiveberg: 
- If you used **HadoopTables** to create your original Iceberg table, set this property:
``` 
TBLPROPERTIES('iceberg.catalog'='hadoop.tables')
```

- If you used **HadoopCatalog** to create your original Iceberg table, set this property:
``` 
TBLPROPERTIES('iceberg.catalog'='hadoop.catalog')
```

### IcebergStorageHandler
This is implemented as a simplified option for creating Hiveberg tables. The Hive DDL should instead look like:
```sql
CREATE TABLE source_db.table_a
  STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler'
  LOCATION 'path_to_iceberg_data_warehouse';
  TBLPROPERTIES('iceberg.catalog' = ...)
```
Include the same `TBLPROPERTIES` as described in the 'Setting TBLPROPERTIES correctly' section, depending on how you've created your Iceberg table.

### Predicate Pushdown
Pushdown of the HiveSQL `WHERE` clause has been implemented so that filters are pushed to the Iceberg `TableScan` level as well as the Parquet and ORC `Reader`'s.
**Note:** Predicate pushdown to the Iceberg table scan is only activated when using the `IcebergStorageHandler`. 


### Column Projection
The `IcebergInputFormat` will project columns from the HiveSQL `SELECT` section down to the Iceberg readers to reduce the number of columns read. 

### Time Travel and System Tables
You can view snapshot metadata from your table by creating a `__snapshots` system table linked to your data table. To do this: 
1. Create your regular table as outlined above.
1. Create another table where the Hive DDL looks similar to: 
```sql
CREATE TABLE source_db.table_a__snapshots
  STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler'
  LOCATION 'path_to_original_data_table'
    TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog')
```
#### Notes
- It is important that the table name ends with `__snapshots` as the `InputFormat` uses this to determine when to load the snapshot metadata table instead of the regular data table. 
- If you want to create a regular data table with a name that ends in `__snapshots` but do **not** want to load the snapshots table, you can override this default behaviour by setting the `iceberg.snapshots.table=false` in the `TBLPROPERTIES`.

Running a `SELECT * FROM table_a__snapshots` query will return you the table snapshot metadata with results similar to the following: 

committed_at | snapshot_id | parent_id | operation | manifest_list | summary 
--- | --- | --- | --- | --- | --- |
1588341995546 | 6023596325564622918 | null | append | /var/folders/sg/... | {"added-data-files":"1","added-records":"3","changed-partition-count":"1","total-records":"3","total-data-files":"1"} 
1588857355026 | 544685312650116825 | 6023596325564622918 | append | /var/folders/sg/... | {"added-data-files":"1","added-records":"3","changed-partition-count":"1","total-records":"6","total-data-files":"2"} 

Time travel is only available when using the `IcebergStorageHandler`. 

Specific snapshots can be selected from your Iceberg table using a provided virtual column that exposes the snapshot id. The default name for this column is `snapshot__id`. However, if you wish to change the name of the snapshot column, you can modify the name by setting a table property like so: 

```sql
TBLPROPERTIES ('iceberg.hive.snapshot.virtual.column.name' = 'new_column_name')
```

To achieve time travel, and query an older snapshot, you can execute a Hive query similar to: 
```sql
SELECT * FROM table_a WHERE snapshot__id = 1234567890 
```

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
