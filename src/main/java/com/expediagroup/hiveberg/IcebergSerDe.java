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
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

public class IcebergSerDe extends AbstractSerDe {

  static final String CATALOG_NAME = "iceberg.catalog";
  static final String TABLE_LOCATION = "location";
  static final String TABLE_NAME = "name";
  static final String WAREHOUSE_LOCATION = "iceberg.warehouse.location";

  private Schema schema;
  private ObjectInspector inspector;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties properties) throws SerDeException {
    Table table = null;
    try {
      table = findTable(configuration, properties);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to load table: ", e);
    }
    this.schema = table.schema();

    try {
      this.inspector = new IcebergObjectInspectorGenerator().createObjectInspector(schema);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private Table findTable(Configuration conf, Properties properties) throws IOException {
    String catalogName = properties.getProperty(CATALOG_NAME);
    if (catalogName == null) {
      throw new IllegalArgumentException("Catalog property: 'iceberg.catalog' not set in JobConf");
    }
    if (catalogName.equals("hadoop.tables")) {
      HadoopTables tables = new HadoopTables(conf);
      URI tableLocation = getPathURI(properties.getProperty(TABLE_LOCATION));
      return tables.load(tableLocation.getPath());
    } else if (catalogName.equals("hadoop.catalog")) {
      URI warehouseLocation = getPathURI(properties.getProperty(WAREHOUSE_LOCATION));
      HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
      TableIdentifier id = TableIdentifier.parse(properties.getProperty(TABLE_NAME));
      return catalog.loadTable(id);
    } else if (catalogName.equals("hive.catalog")) {
      //TODO Implement HiveCatalog
      return null;
    }
    return null;
  }

  private URI getPathURI(String propertyPath) throws IOException {
    if (propertyPath == null) {
      throw new IllegalArgumentException("Table 'iceberg.warehouse.location' not set in JobConf");
    }
    URI location;
    try {
      location = new URI(propertyPath);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + propertyPath + "'", e);
    }
    return location;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    IcebergWritable icebergWritable = (IcebergWritable) writable;
    Schema schema = icebergWritable.getSchema();
    List<Types.NestedField> fields = schema.columns();
    List<Object> row = new ArrayList<>();

    for(Types.NestedField field: fields){
      Object obj = ((IcebergWritable) writable).getRecord().getField(field.name());
      row.add(obj);
    }
    return Collections.unmodifiableList(row);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }
}
