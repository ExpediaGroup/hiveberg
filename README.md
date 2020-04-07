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

To use the `IcebergInputFormat` you need to write a Hive query similar to:
```sql
CREATE TABLE source_db.table_a
   ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe'
   STORED AS
        INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
   LOCATION 'path_to_iceberg_data_warehouse';
```

You must ensure that:
- You are creating Iceberg tables using the Iceberg `HadoopCatalog` API. 
    - Using this means all your Iceberg tables are created under one common location which you must point the Hive table `LOCATION` at. 
- Ensure your Hive table has the **same** name as your Iceberg table. 

For example, if you created the Hive table as shown above then your Iceberg table must be created using a `TableIdentifier` as follows where both table names match: 
```
TableIdentifier id = TableIdentifier.parse("source_db.table_a");
```

### IcebergStorageHandler
This is implemented as a simplified option for creating Hiveberg tables. The Hive query should instead look like:
```sql
CREATE TABLE source_db.table_a
   STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler'
   LOCATION 'path_to_iceberg_data_warehouse';
```

### Predicate Pushdown
Pushdown of the HiveSQL `WHERE` clause has been implemented so that filter's are pushed to the Iceberg `TableScan` level as well as the Parquet `Reader`. ORC implementations are still in the works. 

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
