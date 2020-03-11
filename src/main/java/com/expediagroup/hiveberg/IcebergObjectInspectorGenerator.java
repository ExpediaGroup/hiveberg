package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

public class IcebergObjectInspectorGenerator {

  public IcebergObjectInspectorGenerator() {
  }

  protected ObjectInspector createObjectInspector(Schema schema) throws Exception {
    List<String> columnNames = setColumnNames(schema);
    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> columnTypes = converter.getColumnTypes(schema);

    List<ObjectInspector> columnOIs = new ArrayList<>(columnTypes.size());
    for(int i = 0; i < columnTypes.size(); i++) {
      columnOIs.add(createObjectInspectorWorker(columnTypes.get(i)));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs, null);
  }

  protected ObjectInspector createObjectInspectorWorker(TypeInfo typeInfo) throws Exception {
    ObjectInspector.Category typeCategory = typeInfo.getCategory();

    switch(typeCategory) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo)typeInfo;
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
      case LIST:
        ListTypeInfo ati = (ListTypeInfo)typeInfo;
        return ObjectInspectorFactory
            .getStandardListObjectInspector(createObjectInspectorWorker(ati.getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mti = (MapTypeInfo)typeInfo;
        return ObjectInspectorFactory.getStandardMapObjectInspector(
            createObjectInspectorWorker(mti.getMapKeyTypeInfo()),
            createObjectInspectorWorker(mti.getMapValueTypeInfo()));
      case STRUCT:
        StructTypeInfo sti = (StructTypeInfo)typeInfo;
        List<ObjectInspector> ois = new ArrayList<>(sti.getAllStructFieldTypeInfos().size());
        for(TypeInfo structTypeInfos : sti.getAllStructFieldTypeInfos()) {
          ois.add(createObjectInspectorWorker(structTypeInfos));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(sti.getAllStructFieldNames(), ois);
      default:
        throw new Exception("Couldn't create Object Inspector for category: '" + typeCategory + "'");
    }
  }

  protected List<String> setColumnNames(Schema schema){
    List<Types.NestedField> fields = schema.columns();
    List<String> fieldsList = new ArrayList<>(fields.size());
    for(Types.NestedField field : fields) {
      fieldsList.add(field.name());
    }
    return fieldsList;
  }

}
