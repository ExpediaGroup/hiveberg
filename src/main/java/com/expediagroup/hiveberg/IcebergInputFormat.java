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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotsTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.expediagroup.hiveberg.SystemTableUtil.getVirtualColumnName;
import static com.expediagroup.hiveberg.SystemTableUtil.recordWithVirtualColumn;
import static com.expediagroup.hiveberg.SystemTableUtil.schemaWithVirtualColumn;
import static com.expediagroup.hiveberg.TableResolverUtil.pathAsURI;
import static com.expediagroup.hiveberg.TableResolverUtil.resolveTableFromJob;

/**
 * CombineHiveInputFormat.AvoidSplitCombination is implemented to correctly delegate InputSplit
 * creation to this class. See: https://stackoverflow.com/questions/29133275/
 * custom-inputformat-getsplits-never-called-in-hive
 */
public class IcebergInputFormat implements InputFormat,  CombineHiveInputFormat.AvoidSplitCombination {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String TABLE_LOCATION = "location";

  private Table table;
  private long currentSnapshotId;
  private String virtualSnapshotIdColumnName;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    table = resolveTableFromJob(job);
    URI location = pathAsURI(job.get(TABLE_LOCATION));

    // Set defaults for virtual column
    try {
      currentSnapshotId = table.currentSnapshot().snapshotId();
    } catch (NullPointerException e) {
      LOG.warn("No snapshot available for table - table is empty.");
    }
    virtualSnapshotIdColumnName = getVirtualColumnName(job);

    String[] readColumns = ColumnProjectionUtils.getReadColumnNames(job);
    List<CombinedScanTask> tasks;
    if(job.get(TableScanDesc.FILTER_EXPR_CONF_STR) == null) {
      tasks = Lists.newArrayList(table
          .newScan()
          .select(readColumns)
          .planTasks());
    } else {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities.
          deserializeObject(job.get(TableScanDesc.FILTER_EXPR_CONF_STR), ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      Expression filter = IcebergFilterFactory.generateFilterExpression(sarg);

      long snapshotIdToScan = extractSnapshotID(job, exprNodeDesc);

      tasks = Lists.newArrayList(table
          .newScan()
          .useSnapshot(snapshotIdToScan)
          .select(readColumns)
          .filter(filter)
          .planTasks());
    }
    return createSplits(tasks, location.toString());
  }

  private InputSplit[] createSplits(List<CombinedScanTask> tasks, String name) {
    InputSplit[] splits = new InputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new IcebergSplit(tasks.get(i), name);
    }
    return splits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration configuration) throws IOException {
    return true;
  }

  public class IcebergRecordReader implements RecordReader<Void, IcebergWritable> {
    private JobConf context;
    private IcebergSplit split;

    private Iterator<FileScanTask> tasks;
    private Iterable<Record> reader;
    private Iterator<Record> recordIterator;
    private Record currentRecord;

    public IcebergRecordReader(InputSplit split, JobConf conf) throws IOException {
      this.split = (IcebergSplit) split;
      this.context = conf;
      initialise();
    }

    private void initialise() {
      tasks = split.getTask().files().iterator();
      nextTask();
    }

    private void nextTask(){
      FileScanTask currentTask = tasks.next();
      DataFile file = currentTask.file();
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context);
      Schema tableSchema = table.schema();
      boolean reuseContainers = true; // FIXME: read from config

      IcebergReaderFactory readerFactory = new IcebergReaderFactory();
      reader = readerFactory.createReader(file, currentTask, inputFile, tableSchema, reuseContainers, table);
      recordIterator = reader.iterator();
    }

    @Override
    public boolean next(Void key, IcebergWritable value) {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        if (table instanceof SnapshotsTable) {
          value.setRecord(currentRecord);
        } else {
          value.setRecord(recordWithVirtualColumn(currentRecord, currentSnapshotId, table.schema(), virtualSnapshotIdColumnName));
          LOG.info("Set virtual column id to: " + currentSnapshotId);
        }
        return true;
      }

      if(tasks.hasNext()){
        nextTask();
        currentRecord = recordIterator.next();
        value.setRecord(currentRecord);
        return true;
      }
      return false;
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public IcebergWritable createValue() {
      IcebergWritable record = new IcebergWritable();
      record.setRecord(currentRecord);
      if(table instanceof SnapshotsTable) {
        record.setSchema(table.schema());
      } else {
        record.setSchema(schemaWithVirtualColumn(table.schema(), virtualSnapshotIdColumnName));
      }
      return record;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  /**
   * FileSplit is extended rather than implementing the InputSplit interface due to Hive's HiveInputFormat
   * expecting a split which is an instance of FileSplit.
   */
  private static class IcebergSplit extends FileSplit {

    private CombinedScanTask task;
    private String partitionLocation;

    public IcebergSplit() {
    }

    public IcebergSplit(CombinedScanTask task, String partitionLocation) {
      this.task = task;
      this.partitionLocation = partitionLocation;
    }

    @Override
    public long getLength() {
      return task.files().stream().mapToLong(FileScanTask::length).sum();
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      byte[] dataTask = SerializationUtil.serializeToBytes(this.task);
      out.writeInt(dataTask.length);
      out.write(dataTask);

      byte[] tableName = SerializationUtil.serializeToBytes(this.partitionLocation);
      out.writeInt(tableName.length);
      out.write(tableName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte[] data = new byte[in.readInt()];
      in.readFully(data);
      this.task = SerializationUtil.deserializeFromBytes(data);

      byte[] name = new byte[in.readInt()];
      in.readFully(name);
      this.partitionLocation = SerializationUtil.deserializeFromBytes(name);
    }

    @Override
    public Path getPath() {
      return new Path(partitionLocation);
    }

    @Override
    public long getStart() {
      return 0L;
    }

    public CombinedScanTask getTask() {
      return task;
    }
  }

  /**
   * Search all the leaves of the expression for the 'snapshot_id' column and extract value.
   * If snapshot_id column not found, return current table snapshot ID.
   */
  private long extractSnapshotID(Configuration conf, ExprNodeGenericFuncDesc exprNodeDesc) {
    SearchArgument sarg = ConvertAstToSearchArg.create(conf, exprNodeDesc);
    List<PredicateLeaf> leaves = sarg.getLeaves();
    for(PredicateLeaf leaf : leaves) {
      if(leaf.getColumnName().equals(virtualSnapshotIdColumnName)) {
        currentSnapshotId = (long) leaf.getLiteral();
        return (long) leaf.getLiteral();
      }
    }
    currentSnapshotId = table.currentSnapshot().snapshotId();
    return table.currentSnapshot().snapshotId();
  }

}
