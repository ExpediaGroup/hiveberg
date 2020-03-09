package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

public class IcebergObjectInspectorGenerator {

  private Schema schema;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private ObjectInspector oi;

  public IcebergObjectInspectorGenerator(Schema schema) throws Exception {
    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    this.schema = schema;
    this.columnNames = setColumnNames();
    this.columnTypes = converter.getColumnTypes(schema);
  }

  public List<String>  getColumnNames() {
    return  columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  protected ObjectInspector createObjectInspector() throws Exception {
    List<ObjectInspector> columnOIs = new ArrayList<>(columnNames.size());
    for(int i = 0; i < columnNames.size(); i++) {
      columnOIs.add(createObjectInspectorWorker(columnTypes.get(i)));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs, null);
  }

  //TODO Finish adding other types
  protected ObjectInspector createObjectInspectorWorker(TypeInfo typeInfo) throws Exception {
    ObjectInspector.Category typeCategory = typeInfo.getCategory();

    ObjectInspector result = null;
    switch(typeCategory) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo)typeInfo;
        result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
        break;
      case LIST:
        break;
      case MAP:
        break;
      case STRUCT:
        break;
      case UNION:
        break;
      default:
        throw new Exception("Couldn't create Object Inspector");
    }
    return result;
  }

  protected List<String> setColumnNames(){
    List<Types.NestedField> fields = schema.columns();
    List<String> fieldsList = new ArrayList<>(fields.size());
    for(Types.NestedField field : fields) {
      fieldsList.add(field.name());
    }
    return fieldsList;
  }

}
