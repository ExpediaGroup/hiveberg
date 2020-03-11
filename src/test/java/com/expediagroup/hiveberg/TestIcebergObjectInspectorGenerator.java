/**
 * Copyright (C) 2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.hiveberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

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
