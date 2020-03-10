package com.expediagroup.hiveberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class IcebergReaderFactory {

  public IcebergReaderFactory() {

  }

  public static CloseableIterable<Record> getReader(DataFile file, FileScanTask currentTask, InputFile inputFile, Schema tableSchema, boolean reuseContainers) {
    switch (file.format()) {
      case AVRO:
        return buildAvroReader(currentTask, inputFile, tableSchema, reuseContainers);
      case ORC:
        return buildOrcReader(currentTask, inputFile, tableSchema, reuseContainers);
      case PARQUET:
        return buildParquetReader(currentTask, inputFile, tableSchema, reuseContainers);

      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s", file.format().name(), file.path()));
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
