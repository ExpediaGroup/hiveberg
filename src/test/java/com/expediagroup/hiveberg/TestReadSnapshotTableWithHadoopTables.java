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

import com.google.common.collect.Lists;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestReadSnapshotTableWithHadoopTables {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableLocation;
  private long snapshotId;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    HadoopTables tables = new HadoopTables();
    Table table = tables.create(schema, spec, tableLocation.getAbsolutePath());

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    data.add(TestHelpers.createSimpleRecord(2L, "Andy"));
    data.add(TestHelpers.createSimpleRecord(3L, "Berta"));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileA).commit();

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    snapshotId = snapshots.get(0).snapshotId();
  }

  @Test
  public void testReadSnapshotTable () {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables')")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables')")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a__snapshots");

    assertEquals(2, result.size());
  }

  @Test
  public void testTimeTravelRead () {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables')")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables')")
        .toString());

    List<Object[]> resultLatestTable = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(6, resultLatestTable.size());

    List<Object[]> resultFirstSnapshot = shell.executeStatement("SELECT * FROM source_db.table_a WHERE SNAPSHOT__ID = " + snapshotId);
    assertEquals(3, resultFirstSnapshot.size());

    List<Object[]> resultLatestSnapshotAgain = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(6, resultLatestSnapshotAgain.size());
  }
}
