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


import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTableResolverUtil {

  @Test
  public void extractWarehouseLocationRegularTable() {
    // This is the style of input expected from HiveConf
    String testLocation = "some/folder/database/table_a";
    String testTableName = "database.table_a";

    String expected = "some/folder/";
    String result = TableResolverUtil.extractWarehousePath(testLocation, testTableName);

    assertEquals(expected, result);
  }

  @Test
  public void extractPropertyFromJobConf() {
    JobConf conf = new JobConf();
    String key = "iceberg.catalog";
    String value = "hadoop.tables";

    conf.set(key, value);

    String result = TableResolverUtil.extractProperty(conf, key);

    assertEquals(value, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void extractNonExistentProperty() {
    JobConf conf = new JobConf();
    String key = "iceberg.catalog";

    TableResolverUtil.extractProperty(conf, key);
  }

}
