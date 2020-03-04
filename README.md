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

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
