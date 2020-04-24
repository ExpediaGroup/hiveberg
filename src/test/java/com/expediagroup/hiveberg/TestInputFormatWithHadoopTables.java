package com.expediagroup.hiveberg;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestInputFormatWithHadoopTables {

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
        .withFileSizeInBytes(1024)
        .withRecordCount(3) // needs at least one record or else metrics will filter it out
        .build();

    table.newAppend().appendFile(fileA).commit();
  }

  @Test
  public void testInputFormat () {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("ROW FORMAT SERDE 'com.expediagroup.hiveberg.IcebergSerDe' ")
        .append("STORED AS ")
        .append("INPUTFORMAT 'com.expediagroup.hiveberg.IcebergInputFormat' ")
        .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath())
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
        .append(")")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a");

    assertEquals(3, result.size());
    assertArrayEquals(new Object[]{"Michael", 3000L}, result.get(0));
    assertArrayEquals(new Object[]{"Andy", 3000L}, result.get(1));
    assertArrayEquals(new Object[]{"Berta", 4000L}, result.get(2));
  }
}
