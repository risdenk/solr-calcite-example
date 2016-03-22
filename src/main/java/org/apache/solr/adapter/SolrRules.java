/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.adapter;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.AbstractList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Rules and relational operators for
 * {@link SolrRel#CONVENTION}
 * calling convention.
 */
public class SolrRules {
  private SolrRules() {}

  static final RelOptRule[] RULES = {
//    CassandraFilterRule.INSTANCE,
//    CassandraProjectRule.INSTANCE,
//    CassandraSortRule.INSTANCE
  };

  static List<String> solrFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override
          public String get(int index) {
            return rowType.getFieldList().get(index).getName();
          }

          @Override
          public int size() {
            return rowType.getFieldCount();
          }
        });
  }

  /** Translator from {@link RexNode} to strings in Cassandra's expression
   * language. */
  static class RexToSolrTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    RexToSolrTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /** Base class for planner rules that convert a relational expression to
   * Solr calling convention. */
  abstract static class SolrConvererRule extends ConverterRule {
    final Convention out;

    public SolrConvererRule(Class<? extends RelNode> clazz, String description) {
      this(clazz, relNode -> true, description);
    }

    public <R extends RelNode> SolrConvererRule(Class<R> clazz, Predicate<? super R> predicate, String description) {
      super(clazz, predicate::test, Convention.NONE, SolrRel.CONVENTION, description);
      this.out = SolrRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link LogicalFilter} to a
   * {@link SolrFilter}.
   */
//  private static class CassandraFilterRule extends RelOptRule {
//    private static final Predicate<LogicalFilter> PREDICATE =
//            input -> {
//              // TODO: Check for an equality predicate on the partition key
//              // Right now this just checks if we have a single top-level AND
//              return RelOptUtil.disjunctions(input.getCondition()).size() == 1;
//            };
//
//    private static final CassandraFilterRule INSTANCE = new CassandraFilterRule();
//
//    private CassandraFilterRule() {
//      super(operand(LogicalFilter.class, operand(SolrTableScan.class, none())), "CassandraFilterRule");
//    }
//
//    @Override
//    public boolean matches(RelOptRuleCall call) {
//      // Get the condition from the filter operation
//      LogicalFilter filter = call.rel(0);
//      RexNode condition = filter.getCondition();
//
//      // Get field names from the scan operation
//      SolrTableScan scan = call.rel(1);
//      Pair<List<String>, List<String>> keyFields = scan.solrTable.getKeyFields();
//      Set<String> partitionKeys = new HashSet<>(keyFields.left);
//      List<String> fieldNames = SolrRules.solrFieldNames(filter.getInput().getRowType());
//
//      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
//      if (disjunctions.size() != 1) {
//        return false;
//      } else {
//        // Check that all conjunctions are primary key equalities
//        condition = disjunctions.get(0);
//        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
//          if (!isEqualityOnKey(predicate, fieldNames, partitionKeys, keyFields.right)) {
//            return false;
//          }
//        }
//      }
//
//      // Either all of the partition keys must be specified or none
//      return partitionKeys.size() == keyFields.left.size() || partitionKeys.size() == 0;
//    }
//
//    /** Check if the node is a supported predicate (primary key equality).
//     *
//     * @param node Condition node to check
//     * @param fieldNames Names of all columns in the table
//     * @param partitionKeys Names of primary key columns
//     * @param clusteringKeys Names of primary key columns
//     * @return True if the node represents an equality predicate on a primary key
//     */
//    private boolean isEqualityOnKey(RexNode node, List<String> fieldNames,
//                                    Set<String> partitionKeys, List<String> clusteringKeys) {
//      if (node.getKind() != SqlKind.EQUALS) {
//        return false;
//      }
//
//      RexCall call = (RexCall) node;
//      final RexNode left = call.operands.get(0);
//      final RexNode right = call.operands.get(1);
//      String key = compareFieldWithLiteral(left, right, fieldNames);
//      if (key == null) {
//        key = compareFieldWithLiteral(right, left, fieldNames);
//      }
//      if (key != null) {
//        return partitionKeys.remove(key) || clusteringKeys.contains(key);
//      } else {
//        return false;
//      }
//    }
//
//    /** Check if an equality operation is comparing a primary key column with a literal.
//     *
//     * @param left Left operand of the equality
//     * @param right Right operand of the equality
//     * @param fieldNames Names of all columns in the table
//     * @return The field being compared or null if there is no key equality
//     */
//    private String compareFieldWithLiteral(RexNode left, RexNode right, List<String> fieldNames) {
//      // FIXME Ignore casts for new and assume they aren't really necessary
//      if (left.isA(SqlKind.CAST)) {
//        left = ((RexCall) left).getOperands().get(0);
//      }
//
//      if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
//        final RexInputRef left1 = (RexInputRef) left;
//        return fieldNames.get(left1.getIndex());
//      } else {
//        return null;
//      }
//    }
//
//    /** @see org.apache.calcite.rel.convert.ConverterRule */
//    public void onMatch(RelOptRuleCall call) {
//      LogicalFilter filter = call.rel(0);
//      SolrTableScan scan = call.rel(1);
//      if (filter.getTraitSet().contains(Convention.NONE)) {
//        final RelNode converted = convert(filter, scan);
//        if (converted != null) {
//          call.transformTo(converted);
//        }
//      }
//    }
//
//    public RelNode convert(LogicalFilter filter, SolrTableScan scan) {
//      final RelTraitSet traitSet = filter.getTraitSet().replace(SolrRel.CONVENTION);
//      final Pair<List<String>, List<String>> keyFields = scan.solrTable.getKeyFields();
//      return new SolrFilter(
//          filter.getCluster(),
//          traitSet,
//          convert(filter.getInput(), SolrRel.CONVENTION),
//          filter.getCondition(),
//          keyFields.left,
//          keyFields.right,
//          scan.solrTable.getClusteringOrder());
//    }
//  }
//
//  /**
//   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
//   * to a {@link SolrProject}.
//   */
//  private static class CassandraProjectRule extends CassandraConverterRule {
//    private static final CassandraProjectRule INSTANCE = new CassandraProjectRule();
//
//    private CassandraProjectRule() {
//      super(LogicalProject.class, "CassandraProjectRule");
//    }
//
//    public RelNode convert(RelNode rel) {
//      final LogicalProject project = (LogicalProject) rel;
//      final RelTraitSet traitSet = project.getTraitSet().replace(out);
//      return new SolrProject(project.getCluster(), traitSet,
//          convert(project.getInput(), out), project.getProjects(),
//          project.getRowType());
//    }
//  }
//
//  /**
//   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
//   * {@link SolrSort}.
//   */
//  private static class CassandraSortRule extends RelOptRule {
//    private static final Predicate<Sort> SORT_PREDICATE =
//            input -> {
//              // CQL has no support for offsets
//              return input.offset == null;
//            };
//    private static final Predicate<SolrFilter> FILTER_PREDICATE =
//            input -> {
//              // We can only use implicit sorting within a single partition
//              return input.isSinglePartition();
//            };
//    private static final RelOptRuleOperand CASSANDRA_OP =
//        operand(SolrToEnumerableConverter.class,
//        operand(SolrFilter.class, null, FILTER_PREDICATE, any()));
//
//    private static final CassandraSortRule INSTANCE = new CassandraSortRule();
//
//    private CassandraSortRule() {
//      super(operand(Sort.class, null, SORT_PREDICATE, CASSANDRA_OP), "CassandraSortRule");
//    }
//
//    public RelNode convert(Sort sort, SolrFilter filter) {
//      final RelTraitSet traitSet =
//          sort.getTraitSet().replace(SolrRel.CONVENTION)
//              .replace(sort.getCollation());
//      return new SolrSort(sort.getCluster(), traitSet,
//          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
//          sort.getCollation(), filter.getImplicitCollation(), sort.fetch);
//    }
//
//    public boolean matches(RelOptRuleCall call) {
//      final Sort sort = call.rel(0);
//      final SolrFilter filter = call.rel(2);
//      return collationsCompatible(sort.getCollation(), filter.getImplicitCollation());
//    }
//
//    /** Check if it is possible to exploit native CQL sorting for a given collation.
//     *
//     * @return True if it is possible to achieve this sort in Cassandra
//     */
//    private boolean collationsCompatible(RelCollation sortCollation,
//        RelCollation implicitCollation) {
//      List<RelFieldCollation> sortFieldCollations = sortCollation.getFieldCollations();
//      List<RelFieldCollation> implicitFieldCollations = implicitCollation.getFieldCollations();
//
//      if (sortFieldCollations.size() > implicitFieldCollations.size()) {
//        return false;
//      }
//      if (sortFieldCollations.size() == 0) {
//        return true;
//      }
//
//      // Check if we need to reverse the order of the implicit collation
//      boolean reversed = reverseDirection(sortFieldCollations.get(0).getDirection())
//          == implicitFieldCollations.get(0).getDirection();
//
//      for (int i = 0; i < sortFieldCollations.size(); i++) {
//        RelFieldCollation sorted = sortFieldCollations.get(i);
//        RelFieldCollation implied = implicitFieldCollations.get(i);
//
//        // Check that the fields being sorted match
//        if (sorted.getFieldIndex() != implied.getFieldIndex()) {
//          return false;
//        }
//
//        // Either all fields must be sorted in the same direction
//        // or the opposite direction based on whether we decided
//        // if the sort direction should be reversed above
//        RelFieldCollation.Direction sortDirection = sorted.getDirection();
//        RelFieldCollation.Direction implicitDirection = implied.getDirection();
//        if ((!reversed && sortDirection != implicitDirection)
//            || (reversed && reverseDirection(sortDirection) != implicitDirection)) {
//          return false;
//        }
//      }
//
//      return true;
//    }
//
//    /** Find the reverse of a given collation direction.
//     *
//     * @return Reverse of the input direction
//     */
//    private RelFieldCollation.Direction reverseDirection(RelFieldCollation.Direction direction) {
//      switch(direction) {
//      case ASCENDING:
//      case STRICTLY_ASCENDING:
//        return RelFieldCollation.Direction.DESCENDING;
//      case DESCENDING:
//      case STRICTLY_DESCENDING:
//        return RelFieldCollation.Direction.ASCENDING;
//      default:
//        return null;
//      }
//    }
//
//    /** @see org.apache.calcite.rel.convert.ConverterRule */
//    public void onMatch(RelOptRuleCall call) {
//      final Sort sort = call.rel(0);
//      SolrFilter filter = call.rel(2);
//      final RelNode converted = convert(sort, filter);
//      if (converted != null) {
//        call.transformTo(converted);
//      }
//    }
//  }
}
