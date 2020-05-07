package com.expediagroup.hiveberg;

import com.google.common.collect.Lists;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static com.expediagroup.hiveberg.TableResolverUtil.pathAsURI;
import static com.expediagroup.hiveberg.TableResolverUtil.resolveTableFromJob;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestPredicatePushdown {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableLocation;
  private Table table;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
        optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, tableLocation.getAbsolutePath());
    TableIdentifier id = TableIdentifier.parse("source_db.table_a");
    table = catalog.createTable(id, schema, spec);
  }

  /**
   * This test is to recreate an old bug found in the StorageHandler where the filter property wasn't
   * getting reset between queries.
   */
  @Test
  public void oldVersionOfStorageHandler() throws IOException {
    List<Record> dataA = new ArrayList<>();
    dataA.add(TestHelpers.createSimpleRecord(1L, "Michael"));

    List<Record> dataB = new ArrayList<>();
    dataB.add(TestHelpers.createSimpleRecord(2L, "Andy"));

    List<Record> dataC = new ArrayList<>();
    dataC.add(TestHelpers.createSimpleRecord(3L, "Berta"));

    DataFile fileA = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, dataA);
    DataFile fileB = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, dataB);
    DataFile fileC = TestHelpers.writeFile(temp.newFile(), table, null, FileFormat.PARQUET, dataC);

    table.newAppend().appendFile(fileA).commit();
    table.newAppend().appendFile(fileB).commit();
    table.newAppend().appendFile(fileC).commit();

    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        .append("STORED BY 'com.expediagroup.hiveberg.TestPredicatePushdown$OldIcebergStorageHandler' ")
        .append("LOCATION '")
        .append(tableLocation.getAbsolutePath() + "/source_db/table_a")
        .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.catalog', 'iceberg.warehouse.location'='")
        .append(tableLocation.getAbsolutePath())
        .append("')")
        .toString());

    List<Object[]> resultFullTable = shell.executeStatement("SELECT * FROM source_db.table_a");
    assertEquals(3, resultFullTable.size());

    List<Object[]> resultFilterId = shell.executeStatement("SELECT * FROM source_db.table_a WHERE id = 1");
    assertEquals(1, resultFilterId.size());

    List<Object[]> resultFullTableAfterQuery = shell.executeStatement("SELECT * FROM source_db.table_a");
    //Will only return 1 row because filter from last query is still set in Conf
    assertEquals(1, resultFullTableAfterQuery.size());
  }

  private static class OldIcebergStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler {

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
      return OldIcebergInputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
      return IcebergSerDe.class;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
      getConf().set("iceberg.filter.serialized", SerializationUtilities.serializeObject(exprNodeDesc));

      DecomposedPredicate predicate = new DecomposedPredicate();
      predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
      return predicate;
    }
  }

  private static class OldIcebergInputFormat extends IcebergInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      String TABLE_FILTER_SERIALIZED = "iceberg.filter.serialized";
      table = resolveTableFromJob(job);
      URI location = pathAsURI(job.get(TABLE_LOCATION));

      String[] readColumns = ColumnProjectionUtils.getReadColumnNames(job);
      List<CombinedScanTask> tasks;
      if(job.get(TABLE_FILTER_SERIALIZED) == null) {
        tasks = Lists.newArrayList(table
            .newScan()
            .select(readColumns)
            .planTasks());
      } else {
        ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities.
            deserializeObject(job.get(TABLE_FILTER_SERIALIZED), ExprNodeGenericFuncDesc.class);
        SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
        Expression filter = IcebergFilterFactory.generateFilterExpression(sarg);

        tasks = Lists.newArrayList(table
            .newScan()
            .select(readColumns)
            .filter(filter)
            .planTasks());
      }
      return createSplits(tasks, location.toString());
    }
  }
}
