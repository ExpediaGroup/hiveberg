/**
 * Copyright (C) 2020 Expedia, Inc. and the Apache Software Foundation.
 *
 * This class was inspired by code written for converting to Parquet filters:
 *
 * https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/
 * hive/ql/io/parquet/read/ParquetFilterPredicateConverter.java#L46
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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;

public class IcebergFilterFactory {

  private IcebergFilterFactory() {}

  public static Expression generateFilterExpression(SearchArgument sarg) {
    return translate(sarg.getExpression(), sarg.getLeaves());
  }

  /**
   * Recursive method to traverse down the ExpressionTree to evaluate each expression and its leaf nodes.
   * @param tree Current ExpressionTree where the 'top' node is being evaluated.
   * @param leaves List of all leaf nodes within the tree.
   * @return Expression that is translated from the Hive SearchArgument.
   */
  private static Expression translate(ExpressionTree tree, List<PredicateLeaf> leaves) {
    List<ExpressionTree> childNodes = tree.getChildren();
    switch (tree.getOperator()) {
      case OR:
        Expression orResult = Expressions.alwaysFalse();
        for (ExpressionTree child : childNodes) {
          orResult = or(orResult, translate(child, leaves));
        }
        return orResult;
      case AND:
        Expression result = Expressions.alwaysTrue();
        for (ExpressionTree child : childNodes) {
          result = and(result, translate(child, leaves));
        }
        return result;
      case NOT:
        return not(translate(tree.getChildren().get(0), leaves));
      case LEAF:
        return translateLeaf(leaves.get(tree.getLeaf()));
      case CONSTANT:
        //We are unsure of how the CONSTANT case works, so using the approach of:
        //https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/read/
        // ParquetFilterPredicateConverter.java#L116
        throw new UnsupportedOperationException("CONSTANT operator is not supported");
      default:
        throw new IllegalStateException("Unknown operator: " + tree.getOperator());
    }
  }

  /**
   * Translate leaf nodes from Hive operator to Iceberg operator.
   * @param leaf Leaf node
   * @return Expression fully translated from Hive PredicateLeaf
   */
  private static Expression translateLeaf(PredicateLeaf leaf) {
    String column = leaf.getColumnName();
    if(column.equals("snapshot__id")) {
      return Expressions.alwaysTrue();
    }
    switch (leaf.getOperator()) {
      case EQUALS:
        return equal(column, leafToIcebergType(leaf));
      case NULL_SAFE_EQUALS:
        return equal(notNull(column).ref().name(), leafToIcebergType(leaf)); //TODO: Unsure..
      case LESS_THAN:
        return lessThan(column, leafToIcebergType(leaf));
      case LESS_THAN_EQUALS:
        return lessThanOrEqual(column, leafToIcebergType(leaf));
      case IN:
        return in(column, (List) leafToIcebergType(leaf));
      case BETWEEN:
        List<Object> icebergLiterals = leaf.getLiteralList();
        return and(greaterThanOrEqual(column, icebergLiterals.get(0)),
            lessThanOrEqual(column, icebergLiterals.get(1)));
      case IS_NULL:
        return isNull(column);
      default:
        throw new IllegalStateException("Unknown operator: " + leaf.getOperator());
    }
  }

  private static Object leafToIcebergType(PredicateLeaf leaf) {
    switch (leaf.getType()) {
      case LONG:
        return leaf.getLiteral() != null ? leaf.getLiteral() : leaf.getLiteralList();
      case FLOAT:
        return leaf.getLiteral() != null ? leaf.getLiteral() : leaf.getLiteralList();
      case STRING:
        return leaf.getLiteral() != null ? leaf.getLiteral() : leaf.getLiteralList();
      case DATE:
        //Hive converts a Date type to a Timestamp internally when retrieving literal
        if (leaf.getLiteral() != null) {
          return ((Timestamp) leaf.getLiteral()).toLocalDateTime().toLocalDate().toEpochDay();
        } else {
          //But not when retrieving the literalList
          List<Object> icebergValues = leaf.getLiteralList();
          icebergValues.replaceAll(value -> ((Date) value).toLocalDate().toEpochDay());
          return icebergValues;
        }
      case DECIMAL:
        if (leaf.getLiteral() != null) {
          return BigDecimal.valueOf(((HiveDecimalWritable) leaf.getLiteral()).doubleValue());
        } else {
          List<Object> icebergValues = leaf.getLiteralList();
          icebergValues.replaceAll(value -> BigDecimal.valueOf(((HiveDecimalWritable) value).doubleValue()));
          return icebergValues;
        }
      case TIMESTAMP:
        if (leaf.getLiteral() != null) {
          Timestamp timestamp = (Timestamp) leaf.getLiteral();
          return timestamp.toInstant().getEpochSecond() * 1000000 + timestamp.getNanos() / 1000;
        } else {
          List<Object> icebergValues = leaf.getLiteralList();
          icebergValues.replaceAll(value -> (
              (Timestamp) value).toInstant().getEpochSecond() * 1000000 + ((Timestamp) value).getNanos() / 1000);
          return icebergValues;
        }
      case BOOLEAN:
        return leaf.getLiteral() != null ? leaf.getLiteral() : leaf.getLiteralList();
      default:
        throw new IllegalStateException("Unknown type: " + leaf.getType());
    }
  }
}