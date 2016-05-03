/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.adapter;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.solr.client.solrj.io.stream.metrics.*;

import java.util.*;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate} relational expression in Solr.
 */
public class SolrAggregate extends Aggregate implements SolrRel {
  public SolrAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
    assert getConvention() == SolrRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input,
                        boolean indicator, ImmutableBitSet groupSet,
                        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return new SolrAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    final List<String> inNames = SolrRules.solrFieldNames(getInput().getRowType());
    final List<String> outNames = SolrRules.solrFieldNames(getRowType());

    List<Metric> metrics = new ArrayList<>();
    Map<String, String> fieldMappings = new HashMap<>();
    for(AggregateCall aggCall : aggCalls) {
      Metric metric = toSolrMetric(aggCall.getAggregation(), inNames, aggCall.getArgList());
      metrics.add(metric);
      fieldMappings.put(aggCall.getName(), metric.getIdentifier());
    }
    implementor.addMetrics(metrics);
    implementor.addFieldMappings(fieldMappings);
//    List<String> list = new ArrayList<>();
//    int i = 0;
//    if (groupSet.cardinality() == 1) {
//      final String inName = inNames.get(groupSet.nth(0));
//      list.add("_id: " + SolrRules.maybeQuote("$" + inName));
//      ++i;
//    } else {
//      List<String> keys = new ArrayList<>();
//      for (int group : groupSet) {
//        final String inName = inNames.get(group);
//        keys.add(inName + ": " + SolrRules.quote("$" + inName));
//        ++i;
//      }
//      list.add("_id: " + Util.toString(keys, "{", ", ", "}"));
//    }
//    for (AggregateCall aggCall : aggCalls) {
//      list.add(SolrRules.maybeQuote(outNames.get(i++)) + ": " + toSolr(aggCall.getAggregation(), inNames, aggCall.getArgList()));
//    }
//    implementor.add(null, "{$group: " + Util.toString(list, "{", ", ", "}") + "}");
//    final List<String> fixups;
//    if (groupSet.cardinality() == 1) {
//      fixups = new AbstractList<String>() {
//        @Override
//        public String get(int index) {
//          final String outName = outNames.get(index);
//          return SolrRules.maybeQuote(outName) + ": " + SolrRules.maybeQuote("$" + (index == 0 ? "_id" : outName));
//        }
//
//        @Override
//        public int size() {
//          return outNames.size();
//        }
//      };
//    } else {
//      fixups = new ArrayList<>();
//      fixups.add("_id: 0");
//      i = 0;
//      for (int group : groupSet) {
//        fixups.add(SolrRules.maybeQuote(outNames.get(group)) + ": " + SolrRules.maybeQuote("$_id." + outNames.get(group)));
//        ++i;
//      }
//      for (AggregateCall ignored : aggCalls) {
//        final String outName = outNames.get(i++);
//        fixups.add(SolrRules.maybeQuote(outName) + ": " + SolrRules.maybeQuote("$" + outName));
//      }
//    }
//    if (!groupSet.isEmpty()) {
//      implementor.add(null, "{$project: " + Util.toString(fixups, "{", ", ", "}") + "}");
//    }
  }

  private Metric toSolrMetric(SqlAggFunction aggregation, List<String> inNames, List<Integer> args) {
    switch (args.size()) {
      case 0:
        if(aggregation.equals(SqlStdOperatorTable.COUNT)) {
          return new CountMetric();
        }
      case 1:
        final String inName = inNames.get(args.get(0));
        if (aggregation.equals(SqlStdOperatorTable.SUM) || aggregation.equals(SqlStdOperatorTable.SUM0)) {
          return new SumMetric(inName);
        } else if (aggregation.equals(SqlStdOperatorTable.MIN)) {
          return new MinMetric(inName);
        } else if (aggregation.equals(SqlStdOperatorTable.MAX)) {
          return new MaxMetric(inName);
        } else if (aggregation.equals(SqlStdOperatorTable.AVG)) {
          return new MeanMetric(inName);
        }
      default:
        throw new AssertionError("Invalid aggregation " + aggregation + " with args " + args + " with names" + inNames);
    }
  }
}

// End SolrAggregate.java
