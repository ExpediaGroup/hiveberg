package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to convert Iceberg types to Hive TypeInfo
 */
public class IcebergSchemaToTypeInfo {

  public List<TypeInfo> getColumnTypes(Schema schema) throws Exception {
    List<Types.NestedField> fields = schema.columns();
    List<TypeInfo> types = new ArrayList<>(fields.size());
    for(Types.NestedField field: fields) {
      types.add(generateTypeInfo(field));
    }
    return types;
  }

  private TypeInfo generateTypeInfo(Types.NestedField field) throws Exception {
    Type.TypeID typeID = field.type().typeId();
    TypeInfo result = null;
    switch(typeID) {
      case BOOLEAN:
        result =  TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        break;
      case DATE:
        result = TypeInfoFactory.getPrimitiveTypeInfo("date");
        break;
      case LONG:
        result = TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        break;
      case FLOAT:
        result = TypeInfoFactory.getPrimitiveTypeInfo("float");
        break;
      case DOUBLE:
        result = TypeInfoFactory.getPrimitiveTypeInfo("double");
        break;
      case INTEGER:
        result = TypeInfoFactory.getPrimitiveTypeInfo("int");
        break;
      case UUID:
        break;
      case FIXED:
        break;
      case BINARY:
        result = TypeInfoFactory.getPrimitiveTypeInfo("binary");
        break;
      case TIME:
        result = TypeInfoFactory.getPrimitiveTypeInfo("long");
        break;
      case TIMESTAMP:
        result = TypeInfoFactory.getPrimitiveTypeInfo("timestamp");
        break;
      case STRING:
        result = TypeInfoFactory.getPrimitiveTypeInfo("string");
        break;
      case DECIMAL:
        break;
      case STRUCT:
        break;
      case LIST:
        break;
      case MAP:
        break;
      default:
        throw new Exception("Can't map Iceberg type to Hive TypeInfo"); //Create specific Iceberg exception
    }

    return result;
  }
}
