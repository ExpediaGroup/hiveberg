package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSchemaToTypeInfo {

  @Test
  public void testGenerateTypeInfo() throws Exception {
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    IcebergSchemaToTypeInfo converter = new IcebergSchemaToTypeInfo();
    List<TypeInfo> types = converter.getColumnTypes(schema);
  }
}
