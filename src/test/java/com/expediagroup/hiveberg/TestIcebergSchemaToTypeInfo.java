package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestIcebergSchemaToTypeInfo {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      optional(3, "preferences", Types.StructType.of(
          required(8, "feature1", Types.BooleanType.get()),
          optional(9, "feature2", Types.BooleanType.get())
      ), "struct of named boolean options"),
      required(4, "locations", Types.MapType.ofRequired(10, 11,
          Types.StructType.of(
              required(20, "address", Types.StringType.get()),
              required(21, "city", Types.StringType.get()),
              required(22, "state", Types.StringType.get()),
              required(23, "zip", Types.IntegerType.get())
          ),
          Types.StructType.of(
              required(12, "lat", Types.FloatType.get()),
              required(13, "long", Types.FloatType.get())
          )), "map of address to coordinate"),
      optional(5, "points", Types.ListType.ofOptional(14,
          Types.StructType.of(
              required(15, "x", Types.LongType.get()),
              required(16, "y", Types.LongType.get())
          )), "2-D cartesian points"),
      required(6, "doubles", Types.ListType.ofRequired(17,
          Types.DoubleType.get()
      )),
      optional(7, "properties", Types.MapType.ofOptional(18, 19,
          Types.StringType.get(),
          Types.StringType.get()
      ), "string map of properties")
  );

  @Test
  public void testGeneratePrimitiveTypeInfo() throws Exception {
    Schema SCHEMA = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        required(8, "feature1", Types.BooleanType.get()),
        required(12, "lat", Types.FloatType.get()),
        required(15, "x", Types.LongType.get()),
        required(16, "date", Types.DateType.get()),
        required(17, "double", Types.DoubleType.get()),
        required(18, "binary", Types.BinaryType.get()));

    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> types = converter.getColumnTypes(SCHEMA);
  }

  @Test
  public void testGenerateMapTypeInfo() throws Exception {
    Schema SCHEMA = new Schema(
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties"));

    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> types = converter.getColumnTypes(SCHEMA);
  }

  @Test
  public void testGenerateListTypeInfo() throws Exception {
    Schema SCHEMA = new Schema(
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )));
    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> types = converter.getColumnTypes(SCHEMA);
  }

  @Test
  public void testGenerateStructTypeInfo() throws Exception {
    Schema SCHEMA = new Schema(
        required(4, "locations", Types.StructType.of(
            required(20, "address", Types.StringType.get()),
            required(21, "city", Types.StringType.get()),
            required(22, "state", Types.StringType.get()),
            required(23, "zip", Types.IntegerType.get())
        )));
    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> types = converter.getColumnTypes(SCHEMA);
  }
}
