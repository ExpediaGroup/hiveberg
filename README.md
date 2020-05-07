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
  LOCATION 'path_to_iceberg_table_location';
```

**NOTE:** You need to set extra `TBLPROPERTIES` depending on how you've created your Iceberg table.
- **HadoopTables**: Add one extra configurations: 
```sql 
    TBLPROPERTIES('iceberg.catalog' = 'hadoop.tables')
```
- **HadoopCatalog**: Add these extra configurations: 
```sql 
    TBLPROPERTIES('iceberg.catalog' = 'hadoop.catalog', 'iceberg.warehouse.location' = 'path_to_warehouse_location')
```

You must ensure that your Hive table has the **same** name as your Iceberg table. 
For example, if you created the Hive table as shown above then your Iceberg table must be created using a `TableIdentifier` as follows where both table names match: 
```
TableIdentifier id = TableIdentifier.parse("source_db.table_a");
```

### IcebergStorageHandler
This is implemented as a simplified option for creating Hiveberg tables. The Hive DDL should instead look like:
```sql
CREATE TABLE source_db.table_a
  STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler'
  LOCATION 'path_to_iceberg_data_warehouse';
  TBLPROPERTIES('iceberg.catalog' = ...)
```
Include the same `TBLPROPERTIES` as described above depending on how you've created your Iceberg table.

### Predicate Pushdown
Pushdown of the HiveSQL `WHERE` clause has been implemented so that filters are pushed to the Iceberg `TableScan` level as well as the Parquet `Reader`. ORC implementations are still in the works.
**Note:** Predicate pushdown to the Iceberg table scan is only activated when using the `IcebergStorageHandler`. 


### Column Projection
The `IcebergInputFormat` will project columns from the HiveSQL `SELECT` section down to the Iceberg readers to reduce the number of columns read. 

### Time Travel and System Tables
You can view snapshot metadata from your table by creating a `__snapshots` system table linked to your data table. To do this: 
1. Create your regular table as outlined above.
1. Create another where the Hive DDL looks similar to: 
```sql
CREATE TABLE source_db.table_a__snapshots
  STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler'
  LOCATION 'path_to_original_data_table'
    TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='path_to_original_table_warehouse')
```
Notes: 
- It is important that the table name ends with `__snapshots` as that is how the InputFormat determines when to load the snapshot metadata. 
- If you want to create a regular data table with a name that ends in `__snapshots` but do NOT want to load the snapshots table, you can set the `iceberg.snapshots.table` property to `false` under `TBLPROPERTIES`.

Running a `SELECT * FROM table_a__snapshots` query will get you results similar to: 

committed_at | snapshot_id | parent_id | operation | manifest_list | summary 
--- | --- | --- | --- | --- | --- |
1588341995546 | 6023596325564622918 | null | append | /var/folders/sg/... | {"added-data-files":"1","added-records":"3","changed-partition-count":"1","total-records":"3","total-data-files":"1"} 
1588857355026 | 544685312650116825 | 6023596325564622918 | append | /var/folders/sg/... | {"added-data-files":"1","added-records":"3","changed-partition-count":"1","total-records":"6","total-data-files":"2"} 

Time travel is available when using the `IcebergStorageHandler`. 

The default column name to query for time travel is `snapshot__id`. If your table schema has a column with the same name as this, you can configure Hiveberg to use a different column name for the virtual column using: 

`'hiveberg.snapshot.virtual.column.name' = 'new_column_name'`


To query an older snapshot, execute a Hive query similar to: 
```sql
SELECT * FROM table_a WHERE snapshot__id = 1234567890 
```

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
