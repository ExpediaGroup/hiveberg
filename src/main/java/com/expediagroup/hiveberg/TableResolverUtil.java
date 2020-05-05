/**
 * Copyright (C) 2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.hiveberg;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;

final class TableResolverUtil {

  static final String CATALOG_NAME = "iceberg.catalog";
  static final String HADOOP_CATALOG = "hadoop.catalog";
  static final String HADOOP_TABLES = "hadoop.tables";
  static final String HIVE_CATALOG = "hive.catalog";
  static final String SNAPSHOT_TABLE = "iceberg.snapshots.table";
  static final String TABLE_LOCATION = "location";
  static final String TABLE_NAME = "name";
  static final String WAREHOUSE_LOCATION = "iceberg.warehouse.location";

  private TableResolverUtil() {
  }

  static Table resolveTableFromJob(JobConf conf) throws IOException {
    Properties properties = new Properties();
      properties.setProperty(CATALOG_NAME, extractProperty(conf, CATALOG_NAME));
    if(conf.get(CATALOG_NAME).equals(HADOOP_CATALOG)) {
      properties.setProperty(WAREHOUSE_LOCATION, extractProperty(conf, WAREHOUSE_LOCATION));
      properties.setProperty(SNAPSHOT_TABLE, conf.get(SNAPSHOT_TABLE, "true"));
    }
      properties.setProperty(TABLE_LOCATION, extractProperty(conf, TABLE_LOCATION));
      properties.setProperty(TABLE_NAME, extractProperty(conf, TABLE_NAME));
    return resolveTableFromConfiguration(conf, properties);
  }

  static Table resolveTableFromConfiguration(Configuration conf, Properties properties) throws IOException {
    String catalogName = properties.getProperty(CATALOG_NAME);
    if (catalogName == null) {
      throw new IllegalArgumentException("Catalog property: 'iceberg.catalog' not set in JobConf");
    }
    switch (catalogName) {
      case HADOOP_TABLES:
        HadoopTables tables = new HadoopTables(conf);
        URI tableLocation = pathAsURI(properties.getProperty(TABLE_LOCATION));
        return tables.load(tableLocation.getPath());
      case HADOOP_CATALOG:
        String tableName = properties.getProperty(TABLE_NAME);
        URI warehouseLocation = pathAsURI(properties.getProperty(WAREHOUSE_LOCATION));
        HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
        TableIdentifier id = TableIdentifier.parse(tableName);
        if(tableName.endsWith("__snapshots")) {
          if(properties.getProperty(SNAPSHOT_TABLE, "true").equals("false")) {
            return catalog.loadTable(id);
          } else {
            return resolveMetadataTable(conf, properties.getProperty(WAREHOUSE_LOCATION), tableName);
          }
        } else {
          return catalog.loadTable(id);
        }
      case HIVE_CATALOG:
        //TODO Implement HiveCatalog
        return null;
    }
    return null;
  }

  static Table resolveMetadataTable(Configuration conf, String location, String tableName) throws IOException {
    URI tableLocation = pathAsURI(location);
    HadoopCatalog catalog = new HadoopCatalog(conf, tableLocation.getPath());
    String baseTableName = StringUtils.removeEnd(tableName, "__snapshots");

    TableIdentifier snapshotsId = TableIdentifier.parse(baseTableName + ".snapshots");
    return catalog.loadTable(snapshotsId);
  }

  static URI pathAsURI(String path) throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Path is null.");
    }
    try {
      return new URI(path);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + path + "'", e);
    }
  }

  private static String extractProperty(JobConf conf, String key) {
    String value = conf.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Property not set in JobConf: " + key);
    }
    return value;
  }
}
