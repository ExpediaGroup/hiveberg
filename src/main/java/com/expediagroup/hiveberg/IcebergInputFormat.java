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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
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
    String tableDir = StringUtils.substringAfter(job.get("location"), "file:");

    //Change this to use whichever Catalog the table was made with i.e. HiveCatalog instead etc.
    HadoopTables tables = new HadoopTables(job);
    table = tables.load(tableDir);
    TableScan scan = table.newScan();

    //Change this to add filters from query
    CloseableIterable<FileScanTask> files = scan.planFiles();
    List<DataFile> dataFiles = getDataFiles(files);
    return createSplits(dataFiles);
  }

  private InputSplit[] createSplits(List<DataFile> dataFiles) {
    InputSplit[] splits = new InputSplit[dataFiles.size()];
    for (int i = 0; i < dataFiles.size(); i++) {
      splits[i] = new IcebergSplit(dataFiles.get(i));
    }
    return splits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
  }

  public class IcebergRecordReader implements RecordReader<Void, AvroGenericRecordWritable> {
    private JobConf context;
    private IcebergSplit split;
    private Iterator<GenericData.Record> recordIterator;
    private Schema icebergSchema;
    private org.apache.avro.Schema avroSchema;

    public IcebergRecordReader(InputSplit split, JobConf conf) throws IOException {
      this.split = (IcebergSplit) split;
      this.context = conf;
      initialise();
    }

    private void initialise() {
      //TODO Build different readers depending on file type
      icebergSchema = table.schema();
      avroSchema = getAvroSchema(AvroSchemaUtil.convert(icebergSchema, "temp").toString().replaceAll("-", "_"));

      CloseableIterable<GenericData.Record> reader = buildParquetReader(Files.localInput(split.getFile().path().toString()), icebergSchema, false);
      recordIterator = reader.iterator();
    }

    @Override
    public boolean next(Void key, AvroGenericRecordWritable value) {
      if (recordIterator.hasNext()) {
        GenericData.Record shadedRecord = recordIterator.next();
        org.apache.avro.generic.GenericData.Record record = getUnshadedRecord(avroSchema, shadedRecord);
        value.setRecord(record);
        return true;
      }
      return false;
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public AvroGenericRecordWritable createValue() {
      AvroGenericRecordWritable record = new AvroGenericRecordWritable();
      record.setFileSchema(avroSchema); //Would the schema ever change between records? Maybe, so this might need to be set in the next() method too
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

  private static class IcebergSplit implements Writable, InputSplit {

    private DataFile file;

    IcebergSplit(DataFile file) {
      this.file = file;
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

    public DataFile getFile() {
      return file;
    }
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildParquetReader(InputFile file, Schema schema, boolean reuseContainers) {
    Parquet.ReadBuilder builder = Parquet.read(file).project(schema);

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  private org.apache.avro.Schema getAvroSchema(String stringSchema) {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    return parser.parse(stringSchema);
  }

  private List<DataFile> getDataFiles(CloseableIterable<FileScanTask> files) {
    List<DataFile> dataFiles = Lists.newArrayList();
    Iterator<FileScanTask> iterator = files.iterator();
    while(iterator.hasNext()){
      dataFiles.add(iterator.next().file());
    }
    return dataFiles;
  }

  private org.apache.avro.generic.GenericData.Record getUnshadedRecord(org.apache.avro.Schema schema, GenericData.Record shadedRecord) {
    List<org.apache.iceberg.shaded.org.apache.avro.Schema.Field> fields = shadedRecord.getSchema().getFields();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (org.apache.iceberg.shaded.org.apache.avro.Schema.Field field : fields) {
      Object value = shadedRecord.get(field.name());
      builder.set(field.name(), value);
    }
    return builder.build();
  }
}
