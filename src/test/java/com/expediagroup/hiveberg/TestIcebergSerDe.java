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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIcebergSerDe {

  private File tableLocation;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, tableLocation.getAbsolutePath());
    TableIdentifier id = TableIdentifier.parse("source_db.table_a");
    Table table = catalog.createTable(id, schema, spec);

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createCustomRecord(schema, Arrays.asList("Michael", 3000L)));
    data.add(TestHelpers.createCustomRecord(schema, Arrays.asList("Andy", 3000L)));
    data.add(TestHelpers.createCustomRecord(schema, Arrays.asList("Berta", 4000L)));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);

    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testDeserializerFromSplit() throws IOException, SerDeException {
    IcebergInputFormat format = new IcebergInputFormat();
    JobConf conf = new JobConf();
    conf.set("location", tableLocation.getAbsolutePath() + "/source_db/table_a");
    conf.set("iceberg.warehouse.location", tableLocation.getAbsolutePath());
    conf.set("iceberg.catalog", "hadoop.catalog");
    conf.set("name", "source_db.table_a");
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader reader = format.getRecordReader(splits[0], conf, null);
    IcebergWritable value = (IcebergWritable) reader.createValue();
    reader.next(null, value);

    IcebergSerDe serde = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serde.deserialize(value);

    assertEquals("Michael", deserialized.get(0));
    assertEquals(3000L, deserialized.get(1));
  }

  @Test
  public void testDeserializeMap() {
    Schema schema = new Schema(required(1, "map_type", Types.MapType
        .ofRequired(18, 19, Types.StringType.get(), Types.StringType.get())));
    Map<String, String> expected = ImmutableMap.of("foo", "bar");
    List<Map> data = new ArrayList<>();
    data.add(expected);

    Record record = TestHelpers.createCustomRecord(schema, data);
    IcebergWritable writable = new IcebergWritable();
    writable.setRecord(record);
    writable.setSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    Map result = (Map) deserialized.get(0);

    assertEquals(expected, result);
    assertTrue(result.containsKey("foo"));
    assertTrue(result.containsValue("bar"));
  }

  @Test
  public void testDeserializeList() {
    Schema schema = new Schema(required(1, "list_type", Types.ListType.ofRequired(17, Types.LongType.get())));
    List<Long> expected = Arrays.asList(1000L, 2000L, 3000L);
    List<List> data = new ArrayList<>();
    data.add(expected);

    Record record = TestHelpers.createCustomRecord(schema, data);
    IcebergWritable writable = new IcebergWritable();
    writable.setRecord(record);
    writable.setSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    List result = (List) deserialized.get(0);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializePrimitives() {
    Schema schema = new Schema(required(1, "string_type", Types.StringType.get()),
        required(2, "int_type", Types.IntegerType.get()),
        required(3, "long_type", Types.LongType.get()),
        required(4, "boolean_type", Types.BooleanType.get()),
        required(5, "float_type", Types.FloatType.get()),
        required(6, "double_type", Types.DoubleType.get()),
        required(7, "date_type", Types.DateType.get()));

    List<?> expected = Arrays.asList("foo", 12, 3000L, true, 3.01F, 3.0D, "1998-11-13");

    Record record = TestHelpers.createCustomRecord(schema, expected);
    IcebergWritable writable = new IcebergWritable();
    writable.setRecord(record);
    writable.setSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> result = (List<Object>) serDe.deserialize(writable);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializeNestedList() {
    Schema schema = new Schema(required(1, "map_type", Types.MapType
        .ofRequired(18, 19, Types.StringType.get(), Types.ListType.ofRequired(17, Types.LongType.get()))));
    Map<String, List> expected = ImmutableMap.of("foo", Arrays.asList(1000L, 2000L, 3000L));
    List<Map> data = new ArrayList<>();
    data.add(expected);

    Record record = TestHelpers.createCustomRecord(schema, data);
    IcebergWritable writable = new IcebergWritable();
    writable.setRecord(record);
    writable.setSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    Map result = (Map) deserialized.get(0);

    assertEquals(expected, result);
    assertTrue(result.containsKey("foo"));
    assertTrue(result.containsValue(Arrays.asList(1000L, 2000L, 3000L)));
  }
}
