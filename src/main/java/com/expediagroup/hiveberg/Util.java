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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;

public class Util {

  static final String CATALOG_NAME = "iceberg.catalog";
  static final String TABLE_LOCATION = "location";
  static final String TABLE_NAME = "name";
  static final String WAREHOUSE_LOCATION = "iceberg.warehouse.location";

  private Util() {
  }

  public static Table findTable(JobConf conf) throws IOException {
    Properties properties = new Properties();
      properties.setProperty(CATALOG_NAME, extractProperty(conf, CATALOG_NAME));
      properties.setProperty(TABLE_LOCATION, extractProperty(conf, TABLE_LOCATION));
      properties.setProperty(TABLE_NAME, extractProperty(conf, TABLE_NAME));
      if(conf.get(CATALOG_NAME).equals("hadoop.catalog")) {
        properties.setProperty(WAREHOUSE_LOCATION, extractProperty(conf, WAREHOUSE_LOCATION));
      }
    return findTable(conf, properties);
  }

  static Table findTable(Configuration conf, Properties properties) throws IOException {
    String catalogName = properties.getProperty(CATALOG_NAME);
    if (catalogName == null) {
      throw new IllegalArgumentException("Catalog property: 'iceberg.catalog' not set in JobConf");
    }
    switch (catalogName) {
      case "hadoop.tables":
        HadoopTables tables = new HadoopTables(conf);
        URI tableLocation = getPathURI(properties.getProperty(TABLE_LOCATION));
        return tables.load(tableLocation.getPath());
      case "hadoop.catalog":
        URI warehouseLocation = getPathURI(properties.getProperty(WAREHOUSE_LOCATION));
        HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
        TableIdentifier id = TableIdentifier.parse(properties.getProperty(TABLE_NAME));
        return catalog.loadTable(id);
      case "hive.catalog":
        //TODO Implement HiveCatalog
        return null;
    }
    return null;
  }

  public static URI getPathURI(String propertyPath) throws IOException {
    if (propertyPath == null) {
      throw new IllegalArgumentException("Path set to null in JobConf");
    }
    URI location;
    try {
      location = new URI(propertyPath);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + propertyPath + "'", e);
    }
    return location;
  }

  private static String extractProperty(JobConf conf, String key) {
    String value = conf.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Property not set in JobConf: " + key);
    }
    return value;
  }
}
