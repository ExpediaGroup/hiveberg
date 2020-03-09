package com.expediagroup.hiveberg;

import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IcebergWritable implements Writable {

  private Record record;
  private Schema schema;

  public IcebergWritable() {}

  public void setRecord(Record record) {
    this.record = record;
  }

  public Record getRecord() {
    return record;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
