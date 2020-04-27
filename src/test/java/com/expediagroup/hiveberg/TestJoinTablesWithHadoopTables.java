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

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestJoinTablesWithHadoopTables {

  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  private File tableLocationA;
  private File tableLocationB;

  @Before
  public void before() throws IOException {
    tableLocationA = java.nio.file.Files.createTempDirectory("tempA").toFile();
    tableLocationB = java.nio.file.Files.createTempDirectory("tempA").toFile();
    Schema schemaA = new Schema(optional(1, "first_name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()),
        optional(3, "id", Types.LongType.get()));
    Schema schemaB = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));

    PartitionSpec spec = PartitionSpec.unpartitioned();

    HadoopTables tables = new HadoopTables();

    Table tableA = tables.create(schemaA, spec, tableLocationA.getAbsolutePath());
    Table tableB = tables.create(schemaB, spec, tableLocationB.getAbsolutePath());

    DataFile fileA = DataFiles
        .builder(spec)
        .withPath("src/test/resources/test-table/data/table_a/00000-1-3c678cc3-412a-4290-99f4-5cc0a5612600-00000.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();

    DataFile fileB = DataFiles
        .builder(spec)
        .withPath("src/test/resources/test-table/data/table_b/00000-1-c7557bc3-ae0d-46fb-804e-e9806abf81c7-00000.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();

    tableA.newAppend().appendFile(fileA).commit();
    tableB.newAppend().appendFile(fileB).commit();
    shell.start();
  }

  @Test
  public void testJoinHivebergTablesWithStoredAs() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE EXTERNAL TABLE source_db.table_a ")
        .append("ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe' ")
        .append("STORED AS ")
        .append("INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat' ")
        .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
        .append("LOCATION '")
        .append(tableLocationA.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
        .append(")")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE EXTERNAL TABLE source_db.table_b ")
        .append("ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe' ")
        .append("STORED AS ")
        .append("INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat' ")
        .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
        .append("LOCATION '")
        .append(tableLocationB.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
        .append(")")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT table_a.first_name, table_b.name, table_b.salary " +
        "FROM source_db.table_a, source_db.table_b WHERE table_a.salary = table_b.salary");
    assertEquals(2, result.size());
    assertArrayEquals(new Object[]{"Ella", "Andy", 3000L}, result.get(0));
    assertArrayEquals(new Object[]{"Ella", "Michael", 3000L}, result.get(1));
  }

  @Test
  public void testJoinHivebergTablesWithStoredBy() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE EXTERNAL TABLE source_db.table_a ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocationA.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
        .append(")")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE EXTERNAL TABLE source_db.table_b ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocationB.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
        .append(")")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT table_a.first_name, table_b.name, table_b.salary " +
        "FROM source_db.table_a, source_db.table_b WHERE table_a.salary = table_b.salary");
    assertEquals(2, result.size());
    assertArrayEquals(new Object[]{"Ella", "Andy", 3000L}, result.get(0));
    assertArrayEquals(new Object[]{"Ella", "Michael", 3000L}, result.get(1));
  }
}
