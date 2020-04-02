package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.iceberg.expressions.Expression;

import java.util.List;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;

public class IcebergFilterFactory {

  IcebergFilterFactory () {}

  public static Expression getFilterExpression(SearchArgument sarg) {
    List<PredicateLeaf> children = sarg.getLeaves();
    List<ExpressionTree> expressionChildren = sarg.getExpression().getChildren();

    switch (sarg.getExpression().getOperator()) {
      case OR:
        ExpressionTree orLeft = expressionChildren.get(0);
        ExpressionTree orRight = expressionChildren.get(1);
        return or(recurseExpressionTree(orLeft, children), recurseExpressionTree(orRight, children));
      case AND:
        ExpressionTree andLeft = expressionChildren.get(0);
        ExpressionTree andRight = expressionChildren.get(1);
        if(expressionChildren.size() > 2) {
          Expression[] evaluatedChildren = getLeftoverLeaves(expressionChildren, children);
          return and(recurseExpressionTree(andLeft, children), recurseExpressionTree(andRight, children), evaluatedChildren);
        } else {
          return and(recurseExpressionTree(andLeft, children), recurseExpressionTree(andRight, children));
        }
      case NOT:
        return not(getLeaf(sarg.getLeaves().get(0)));
      case LEAF:
        return getLeaf(sarg.getLeaves().get(0));
      case CONSTANT:
        return getConstantExp(sarg.getExpression());
      default:
        return null;
    }
  }

  /**
   * Remove nodes already evaluated and return an array of the evaluated leftover nodes.
   * @param allLeaves - all child expression trees to be evaluated for the AND.
   * @param children - all the implementations of the child expression trees.
   * @return array list of leftover evaluated nodes.
   */
  private static Expression[] getLeftoverLeaves(List<ExpressionTree> allLeaves, List<PredicateLeaf> children) {
    allLeaves.remove(0);
    allLeaves.remove(0);

    Expression[] evaluatedLeaves = new Expression[allLeaves.size()];
    for(int i = 0; i < allLeaves.size(); i ++) {
      Expression filter = recurseExpressionTree(allLeaves.get(i), children);
      evaluatedLeaves[i] = filter;
    }
    return evaluatedLeaves;
  }

  private static Expression recurseExpressionTree(ExpressionTree tree, List<PredicateLeaf> leaves) {
    switch (tree.getOperator()) {
      case OR:
        return or(recurseExpressionTree(tree.getChildren().get(0), leaves), recurseExpressionTree(tree.getChildren().get(1), leaves));
      case AND:
        return and(recurseExpressionTree(tree.getChildren().get(0), leaves), recurseExpressionTree(tree.getChildren().get(1), leaves));
      case NOT:
        return not(recurseExpressionTree(tree.getChildren().get(0), leaves));
      case LEAF:
        return getLeaf(leaves.get(tree.getLeaf()));
      case CONSTANT:
        return getConstantExp(tree);
      default:
        return null;
    }
  }

  private static Expression getLeaf(PredicateLeaf leaf) {
    String column = leaf.getColumnName();
    switch (leaf.getOperator()){
      case EQUALS:
        return equal(column, leaf.getLiteral());
      case NULL_SAFE_EQUALS:
        return equal(notNull(column).ref().name(), leaf.getLiteral()); //TODO: Unsure..
      case LESS_THAN:
        return lessThan(column, leaf.getLiteral());
      case LESS_THAN_EQUALS:
        return lessThanOrEqual(column, leaf.getLiteral());
      case IN:
        return in(column, leaf.getLiteralList());
      case BETWEEN:
        return and((greaterThan(column, leaf.getLiteralList().get(0))), lessThan(column, leaf.getLiteralList().get(1)));
      case IS_NULL:
        return isNull(column);
      default:
        return null;
    }
  }

  private static Expression getConstantExp(ExpressionTree tree) {
    switch (tree.getConstant()) { //TODO: What to return for these? True, false?
      case YES:
        return null;
      case NO:
        return null;
      case NULL:
        return null;
      case YES_NULL:
        return null;
      case NO_NULL:
        return null;
      case YES_NO:
        return null;
      case YES_NO_NULL:
        return null;
      default:
        return null;
    }
  }
}
