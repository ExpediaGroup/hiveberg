package com.expediagroup.hiveberg;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;

public class TestIcebergObjectInspectorGenerator {

  @Test
  public void testGetColumnNames() throws Exception {
    Schema schema = new Schema(optional(1, "name", Types.StringType.get()),
        optional(2, "salary", Types.LongType.get()));
    IcebergObjectInspectorGenerator oi = new IcebergObjectInspectorGenerator();

    List<String> fieldsNames = oi.setColumnNames(schema);
    assertEquals(fieldsNames.size(), 2);
  }
}
