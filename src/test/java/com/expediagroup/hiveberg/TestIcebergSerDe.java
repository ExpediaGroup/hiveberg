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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSerDe {

  private File tableLocation;
  private Table table;

  @Before
  public void before() throws IOException {
    tableLocation = java.nio.file.Files.createTempDirectory("temp").toFile();
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, tableLocation.getAbsolutePath());
    TableIdentifier id = TableIdentifier.parse("source_db.table_a");
    Table table = catalog.createTable(id, schema, spec);

    DataFile fileA = DataFiles
        .builder(spec)
        .withPath("src/test/resources/test-table/data/00000-1-c7557bc3-ae0d-46fb-804e-e9806abf81c7-00000.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();

    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testDeserializer() throws IOException, SerDeException {
    IcebergInputFormat format = new IcebergInputFormat();
    JobConf conf = new JobConf();
    conf.set("location", tableLocation.getAbsolutePath());
    conf.set("name", "source_db.table_a");
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader reader = format.getRecordReader(splits[0], conf, null);
    IcebergWritable value = (IcebergWritable) reader.createValue();
    reader.next(null, value);

    IcebergSerDe serde = new IcebergSerDe();
    Object row = serde.deserialize(value);
  }
}
