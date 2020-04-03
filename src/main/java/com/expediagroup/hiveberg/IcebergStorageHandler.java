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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

public class IcebergStorageHandler extends DefaultStorageHandler implements HiveStoragePredicateHandler {

  private Configuration conf;
  private String filter;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return IcebergInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveIgnoreKeyTextOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return IcebergSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
                                          Map<String, String> jobProperties) {
    // do nothing by default
    //jobProperties.put("hive.io.filter.text", "testing");
    //tableDesc.setJobProperties(jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
                                           Map<String, String> jobProperties) {
    // do nothing by default
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
                                          Map<String, String> jobProperties) {
    //do nothing by default
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    //do nothing by default
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  @Override
  public String toString() {
    return this.getClass().getName();
  }

  /**
   * Extract and serialize the filter expression and add it to the Configuration for the InputFormat to access.
   * @param jobConf - job configuration for InputFormat to access
   * @param deserializer - deserializer
   * @param exprNodeDesc - filter expression extracted by Hive
   * @return - decomposed predicate that tells Hive what parts of the predicate are handled by the StorageHandler
   * and what parts Hive needs to handle.
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    getConf().setBoolean("iceberg.filter", true);
    getConf().set("iceberg.filter.serialized", SerializationUtilities.serializeObject(exprNodeDesc));

    //TODO: Decide what Iceberg can handle and what to return to Hive
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }
}
