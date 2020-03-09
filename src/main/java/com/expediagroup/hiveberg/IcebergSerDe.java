package com.expediagroup.hiveberg;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IcebergSerDe extends AbstractSerDe {

  private Schema schema;
  private TableMetadata metadata;
  private ObjectInspector oi;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties properties) throws SerDeException {
    String tableDir = properties.getProperty("location") + "/metadata/v2.metadata.json";
    this.metadata = TableMetadataParser.read(new HadoopFileIO(configuration), tableDir);
    this.schema = metadata.schema();

    try {
      IcebergObjectInspectorGenerator ioig = new IcebergObjectInspectorGenerator(schema);
      this.oi = ioig.createObjectInspector();
    } catch (Exception e) {
      //Error handling
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    IcebergWritable icebergWritable = (IcebergWritable) writable;
    Schema schema = icebergWritable.getSchema();
    List<Types.NestedField> fields = schema.columns();
    List<Object> row = new ArrayList<>();

    for(Types.NestedField field: fields){
      Object obj = ((IcebergWritable) writable).getRecord().getField(field.name());
      row.add(obj);
    }
    return row;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }
}
