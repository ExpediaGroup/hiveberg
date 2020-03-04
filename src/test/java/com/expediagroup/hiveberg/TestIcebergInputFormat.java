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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;

@RunWith(StandaloneHiveRunner.class)
public class TestIcebergInputFormat {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  private File tableLocation;

  @Before
  public void before() throws IOException {
    tableLocation = java.nio.file.Files.createTempDirectory("temp").toFile();
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables();
    Table table = tables.create(schema, spec, tableLocation.getAbsolutePath());

    DataFile fileA = DataFiles
        .builder(spec)
        .withPath("src/test/resources/test-table/data/00000-1-c7557bc3-ae0d-46fb-804e-e9806abf81c7-00000.parquet")
        .withFileSizeInBytes(0)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();
    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testGetSplits() {
    shell.execute("CREATE DATABASE source_db");
    shell
        .execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            // .append("(name STRING, salary INT) ")
            .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' ")
            .append("STORED AS ")
            .append("INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat' ")
            .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath())
            .append(
                "' TBLPROPERTIES ('avro.schema.literal' = '{\"namespace\":\"source_db\",\"type\":\"record\",\"name\":\"table_a\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null,\"field_id\":1},{\"name\":\"salary\",\"type\":[\"null\",\"long\"],\"default\":null,\"field_id\":2}]}')")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a WHERE salary=3000");
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(tableLocation);
  }
}
