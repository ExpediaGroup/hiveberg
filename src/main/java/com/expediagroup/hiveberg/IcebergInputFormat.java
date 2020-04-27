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
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CombineHiveInputFormat.AvoidSplitCombination is implemented to correctly delegate InputSplit
 * creation to this class. See: https://stackoverflow.com/questions/29133275/
 * custom-inputformat-getsplits-never-called-in-hive
 */
public class IcebergInputFormat implements InputFormat,  CombineHiveInputFormat.AvoidSplitCombination {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String CATALOG_NAME = "iceberg.catalog";
  static final String REUSE_CONTAINERS = "iceberg.mr.reuse.containers";
  static final String TABLE_LOCATION = "location";
  static final String TABLE_NAME = "name";
  static final String TABLE_FILTER_SERIALIZED = "iceberg.filter.serialized";
  static final String WAREHOUSE_LOCATION = "iceberg.warehouse.location";

  private Table table;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    table = findTable(job);
    URI location = getPathURI(job.get(TABLE_LOCATION));

    List<CombinedScanTask> tasks;
    if(job.get(TABLE_FILTER_SERIALIZED) == null) {
      tasks = Lists.newArrayList(table
          .newScan()
          .planTasks());
    } else {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities.
          deserializeObject(job.get(TABLE_FILTER_SERIALIZED), ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      Expression filter = IcebergFilterFactory.generateFilterExpression(sarg);

      tasks = Lists.newArrayList(table
          .newScan()
          .filter(filter)
          .planTasks());
    }
    return createSplits(tasks, location.toString());
  }

  private Table findTable(JobConf conf) throws IOException {
    String catalogName = conf.get(CATALOG_NAME);
    if (catalogName == null) {
      throw new IllegalArgumentException("Catalog property: 'iceberg.catalog' not set in JobConf");
    }
    switch (catalogName) {
      case "hadoop.tables":
        URI tableLocation = getPathURI(conf.get(TABLE_LOCATION));
        HadoopTables tables = new HadoopTables(conf);
        table = tables.load(tableLocation.getPath());
        break;
      case "hadoop.catalog":
        URI warehouseLocation = getPathURI(conf.get(WAREHOUSE_LOCATION));
        HadoopCatalog catalog = new HadoopCatalog(conf, warehouseLocation.getPath());
        TableIdentifier id = TableIdentifier.parse(conf.get(TABLE_NAME));
        table = catalog.loadTable(id);
        break;
      case "hive.catalog":
        //TODO Implement HiveCatalog
        break;
    }
    return table;
  }

  private URI getPathURI(String propertyPath) throws IOException {
    if (propertyPath == null) {
      throw new IllegalArgumentException("Path set to null in JobConf");
    }
    URI location;
    try {
      location = new URI(propertyPath);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + propertyPath + "'", e);
    }
    return location;
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
    private CloseableIterable<Record> reader;
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
      reader = readerFactory.createReader(file, currentTask, inputFile, tableSchema, reuseContainers);
      recordIterator = reader.iterator();
    }

    @Override
    public boolean next(Void key, IcebergWritable value) {
      if (recordIterator.hasNext()) {
        currentRecord = recordIterator.next();
        value.setRecord(currentRecord);
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
      record.setSchema(table.schema());
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

}
