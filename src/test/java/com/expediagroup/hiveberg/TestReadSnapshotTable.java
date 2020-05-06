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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
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
public class TestReadSnapshotTable {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableLocation;
  private Configuration conf = new Configuration();
  private HadoopCatalog catalog;;
  private Schema schema = new Schema(required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()));
  private long snapshotId;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    catalog = new HadoopCatalog(conf, tableLocation.getAbsolutePath());
    PartitionSpec spec = PartitionSpec.unpartitioned();

    TableIdentifier id = TableIdentifier.parse("source_db.table_a");
    Table table = catalog.createTable(id, schema, spec);

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    data.add(TestHelpers.createSimpleRecord(2L, "Andy"));
    data.add(TestHelpers.createSimpleRecord(3L, "Berta"));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    DataFile fileB = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    DataFile fileC = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileB).commit();
    table.newAppend().appendFile(fileC).commit();

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
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a__snapshots");

    assertEquals(3, result.size());
  }

  @Test
  public void testCreateRegularTableEndingWithSnapshots() throws IOException {
    TableIdentifier id = TableIdentifier.parse("source_db.table_a__snapshots");
    Table table = catalog.createTable(id, schema, PartitionSpec.unpartitioned());

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();

    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a__snapshots")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog','iceberg.snapshots.table'='false', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a__snapshots");

    assertEquals(1, result.size());
  }

  @Test
  public void testTimeTravelRead () {
    shell.execute("CREATE DATABASE source_db");

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a__snapshots ")
        .append("STORED BY 'com.expediagroup.hiveberg.IcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    List<Object[]> resultLatestTable = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(9, resultLatestTable.size());

    List<Object[]> resultFirstSnapshot = shell.executeStatement("SELECT * FROM source_db.table_a WHERE snapshot_id = " + snapshotId);
    assertEquals(3, resultFirstSnapshot.size());

    List<Object[]> resultLatestSnapshotAgain = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(9, resultLatestSnapshotAgain.size());
  }

  @Test
  public void testSnapshotFunc() throws IOException {
    TableIdentifier id = TableIdentifier.parse("source_db.table_a__snapshots");
    Table table = catalog.createTable(id, schema, PartitionSpec.unpartitioned());

    List<Record> data = new ArrayList<>();
    data.add(TestHelpers.createSimpleRecord(1L, "Michael"));
    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, data);
    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileA).commit();

    long mostRecentId = table.currentSnapshot().snapshotId();
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    long oldestId = snapshots.get(0).snapshotId();

    table.manageSnapshots().rollbackTo(oldestId).commit();
    table.refresh();
  }

}
