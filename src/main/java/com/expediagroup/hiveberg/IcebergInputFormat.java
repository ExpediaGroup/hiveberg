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
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergInputFormat implements InputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  static final String TABLE_LOCATION = "location";
  static final String TABLE_NAME = "name";
  static final String TABLE_FILTER_SERIALIZED = "iceberg.filter.serialized";

  private Table table;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String tableDir = job.get(TABLE_LOCATION);
    URI location;
    try {
      location = new URI(tableDir);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to create URI for table location: '" + tableDir + "'");
    }
    HadoopCatalog catalog = new HadoopCatalog(job,location.getPath());
    TableIdentifier id = TableIdentifier.parse(job.get(TABLE_NAME));
    table = catalog.loadTable(id);

    //String[] readColumns = ColumnProjectionUtils.getReadColumnNames(job);
    List<CombinedScanTask> tasks;
    if(job.get(TABLE_FILTER_SERIALIZED) == null) {
      tasks = Lists.newArrayList(table
          .newScan()
          //.select(readColumns)
          .planTasks());
    } else {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities.
          deserializeObject(job.get( TABLE_FILTER_SERIALIZED), ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      Expression filter = IcebergFilterFactory.generateFilterExpression(sarg);

      tasks = Lists.newArrayList(table
          .newScan()
          .filter(filter)
          //.select(readColumns)
          .planTasks());
    }

    return createSplits(tasks);
  }

  private InputSplit[] createSplits(List<CombinedScanTask> tasks) {
    InputSplit[] splits = new InputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new IcebergSplit(tasks.get(i));
    }
    return splits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
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

  //TODO: can we put this back to private?
  public static class IcebergSplit implements InputSplit {

    private CombinedScanTask task;

    //TODO: can we remove this?
    public IcebergSplit() {
    }
    
    //TODO: can we put this back to default?
    public IcebergSplit(CombinedScanTask task) {
      this.task = task;
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

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    public CombinedScanTask getTask() {
      return task;
    }
  }

}
