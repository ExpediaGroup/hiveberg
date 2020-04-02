package com.expediagroup.hiveberg;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIcebergFilterFactory {

  @Test
  public void testEqualsOperand () {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.equal("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testNotEqualsOperand () {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startNot().equals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    Not expected = (Not) Expressions.not(Expressions.equal("salary", 3000L));
    Not actual = (Not) IcebergFilterFactory.getFilterExpression(arg);

    UnboundPredicate childExpressionActual = (UnboundPredicate) actual.child();

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.child().op(), expected.child().op());
    assertEquals(childExpressionActual.ref().name(), "salary");
    assertEquals(childExpressionActual.literal(), Literal.of(3000L));
  }

  @Test
  public void testLessThanOperand () {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().lessThan("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.lessThan("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testLessThanEqualsOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().lessThanEquals("salary", PredicateLeaf.Type.LONG, 3000L).end().build();

    UnboundPredicate expected = Expressions.lessThanOrEqual("salary", 3000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literal(), expected.literal());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testInOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().in("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    UnboundPredicate expected = Expressions.in("salary", 3000L, 4000L);
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.literals(), expected.literals());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testBetweenOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .between("salary", PredicateLeaf.Type.LONG, 3000L, 4000L).end().build();

    And expected = (And) Expressions
        .and(Expressions.greaterThan("salary", 3000L),Expressions.lessThan("salary", 3000L));
    And actual = (And) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testIsNullOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder.startAnd().isNull("salary", PredicateLeaf.Type.LONG).end().build();

    UnboundPredicate expected = Expressions.isNull("salary");
    UnboundPredicate actual = (UnboundPredicate) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.ref().name(), expected.ref().name());
  }

  @Test
  public void testAndOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("salary", PredicateLeaf.Type.LONG, 4000L)
        .end().build();

    And expected = (And) Expressions
        .and(Expressions.equal("salary", 3000L),Expressions.equal("salary", 4000L));
    And actual = (And) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void tesOrOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startOr()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("salary", PredicateLeaf.Type.LONG, 4000L)
        .end().build();

    Or expected = (Or) Expressions
        .or(Expressions.equal("salary", 3000L),Expressions.equal("salary", 4000L));
    Or actual = (Or) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.left().op(), expected.left().op());
    assertEquals(actual.right().op(), expected.right().op());
  }

  @Test
  public void testManyAndOperand() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument arg = builder
        .startAnd()
        .equals("salary", PredicateLeaf.Type.LONG, 3000L)
        .equals("job", PredicateLeaf.Type.LONG, 4000L)
        .equals("name", PredicateLeaf.Type.LONG, 9000L)
        .end()
        .build();

    And expected = (And) Expressions.and(
        Expressions.equal("salary", 3000L),
        Expressions.equal("job", 4000L),
        Expressions.equal("name", 9000L ));

    And actual = (And) IcebergFilterFactory.getFilterExpression(arg);

    assertEquals(actual.op(), expected.op());
    assertEquals(actual.right().op(), expected.right().op());
    assertEquals(actual.left().op(), expected.left().op());
  }

  /*@Test
  public void complexArgument() {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    SearchArgument searchArgument = builder
        .startAnd()
        .startNot()
        .isNull("name", PredicateLeaf.Type.STRING)
        .end()
        .startNot()
        .lessThanEquals("salary", PredicateLeaf.Type.LONG, 3000L)
        .end()
        .startNot()
        .between("salary", PredicateLeaf.Type.LONG, 2000L, 3000L)
        .end()
        .end()
        .build();

    Expression filter = IcebergFilterFactory.getFilterExpression(searchArgument);
  }*/
}
