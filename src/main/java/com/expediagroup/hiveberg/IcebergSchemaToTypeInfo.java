package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Class to convert Iceberg types to Hive TypeInfo
 */
public class IcebergSchemaToTypeInfo {

  private static final Map<Type, TypeInfo> primitiveTypeToTypeInfo = initTypeMap();
  private static Map<Type, TypeInfo> initTypeMap() {
    Map<Type, TypeInfo> theMap = new Hashtable<Type, TypeInfo>();
    theMap.put(Types.BooleanType.get(), TypeInfoFactory.getPrimitiveTypeInfo("boolean"));
    theMap.put(Types.IntegerType.get(), TypeInfoFactory.getPrimitiveTypeInfo("int"));
    theMap.put(Types.LongType.get(), TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    theMap.put(Types.FloatType.get(), TypeInfoFactory.getPrimitiveTypeInfo("float"));
    theMap.put(Types.DoubleType.get(), TypeInfoFactory.getPrimitiveTypeInfo("double"));
    theMap.put(Types.BinaryType.get(), TypeInfoFactory.getPrimitiveTypeInfo("binary"));
    theMap.put(Types.StringType.get(), TypeInfoFactory.getPrimitiveTypeInfo("string"));
    theMap.put(Types.DateType.get(), TypeInfoFactory.getPrimitiveTypeInfo("date"));
    theMap.put(Types.TimestampType.withoutZone(), TypeInfoFactory.getPrimitiveTypeInfo("timestamp"));
    return Collections.unmodifiableMap(theMap);
  }

  public List<TypeInfo> getColumnTypes(Schema schema) throws Exception {
    List<Types.NestedField> fields = schema.columns();
    List<TypeInfo> types = new ArrayList<>(fields.size());
    for(Types.NestedField field: fields) {
      types.add(generateTypeInfo(field.type()));
    }
    return types;
  }

  private TypeInfo generateTypeInfo(Type type) throws Exception {
    if(primitiveTypeToTypeInfo.containsKey(type)){
      return primitiveTypeToTypeInfo.get(type);
    }
    switch(type.typeId()) {
      case UUID:
        return TypeInfoFactory.getPrimitiveTypeInfo("string");
      case FIXED:
        TypeInfoFactory.getPrimitiveTypeInfo("binary");
        return null;
      case TIME:
        return TypeInfoFactory.getPrimitiveTypeInfo("long");
      case DECIMAL:
        Types.DecimalType dec = (Types.DecimalType)type;
        int scale = dec.scale();
        int precision = dec.precision();
        try {
          HiveDecimalUtils.validateParameter(precision, scale);
        } catch (Exception e) {
          //TODO Log that precision / scale isn't valid
        }
        return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
      case STRUCT:
        return generateStructTypeInfo((Types.StructType)type);
      case LIST:
        return generateListTypeInfo((Types.ListType)type);
      case MAP:
        return generateMapTypeInfo((Types.MapType)type);
      default:
        throw new Exception("Can't map Iceberg type to Hive TypeInfo: '" + type.typeId() + "'"); //Create specific Iceberg exception
    }
  }

  private TypeInfo generateMapTypeInfo(Types.MapType type) throws Exception {
    Type keyType = type.keyType();
    Type valueType = type.valueType();
    return TypeInfoFactory.getMapTypeInfo(generateTypeInfo(keyType), generateTypeInfo(valueType));
  }

  private TypeInfo generateStructTypeInfo(Types.StructType type) throws Exception {
    List<Types.NestedField> fields = type.fields();
    List<String> fieldNames = new ArrayList<>(fields.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fields.size());

    for (Types.NestedField field : fields) {
      fieldNames.add(field.name());
      typeInfos.add(generateTypeInfo(field.type()));
    }
    return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
  }

  private TypeInfo generateListTypeInfo(Types.ListType type) throws Exception {
    return TypeInfoFactory.getListTypeInfo(generateTypeInfo(type.elementType()));
  }
}
