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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.shaded.org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class IcebergInputFormat implements InputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  private Table table;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    //Change this to use whichever Catalog the table was made with i.e. HiveCatalog instead etc.
    HadoopTables tables = new HadoopTables(job);
    String tableDir = StringUtils.substringAfter(job.get("location"), "file:");
    table = tables.load(tableDir);

    List<CombinedScanTask> tasks = Lists.newArrayList(table.newScan().planTasks());
    return createSplits(tasks);
  }

  private InputSplit[] createSplits(List<CombinedScanTask> tasks) {
    InputSplit[] splits = new InputSplit[tasks.size()];
    for (int i = 0; i < splits.length; i++) {
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
    private Schema icebergSchema;

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
      icebergSchema = table.schema();
      CombinedScanTask task = split.getTask();
      tasks = task.files().iterator();
      nextTask();
    }

    private void nextTask(){
      FileScanTask currentTask = tasks.next();
      DataFile file = currentTask.file();
      InputFile inputFile = HadoopInputFile.fromLocation(file.path(), context);
      Schema tableSchema = table.schema();
      boolean reuseContainers = true; // FIXME: read from config

      switch (file.format()) {
        case AVRO:
          reader = buildAvroReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;
        case ORC:
          reader = buildOrcReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;
        case PARQUET:
          reader = buildParquetReader(currentTask, inputFile, tableSchema, reuseContainers);
          break;

        default:
          throw new UnsupportedOperationException(String.format("Cannot read %s file: %s", file.format().name(), file.path()));
      }

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

  private static class IcebergSplit implements InputSplit {

    private CombinedScanTask task;

    IcebergSplit(CombinedScanTask task) {
      this.task = task;
    }

    @Override
    public long getLength() throws IOException {
      return 0;
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

  // FIXME: use generic reader function
  private static CloseableIterable buildAvroReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Avro.ReadBuilder builder = Avro.read(file)
        .createReaderFunc(DataReader::create)
        .project(schema)
        .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildOrcReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    ORC.ReadBuilder builder = ORC.read(file)
//            .createReaderFunc() // FIXME: implement
        .schema(schema)
        .split(task.start(), task.length());

    return builder.build();
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildParquetReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Parquet.ReadBuilder builder = Parquet.read(file)
        .createReaderFunc(messageType -> GenericParquetReaders.buildReader(schema, messageType))
        .project(schema)
        .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }
}
