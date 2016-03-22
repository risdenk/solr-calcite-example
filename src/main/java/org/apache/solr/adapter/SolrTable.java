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

import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.luke.FieldFlag;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class SolrTable extends AbstractTable implements QueryableTable, TranslatableTable {
  final CloudSolrClient cloudSolrClient;
  final String collection;
  final Map<String, LukeResponse.FieldInfo> fieldInfo;
  final List<String> allFields = new ArrayList<>();

  SolrTable(CloudSolrClient cloudSolrClient, String collection) {
    this.cloudSolrClient = cloudSolrClient;
    this.collection = collection;
    this.fieldInfo = getFieldInfo();
    this.allFields.addAll(this.fieldInfo.keySet());
  }

  private Map<String, LukeResponse.FieldInfo> getFieldInfo() {
    LukeRequest lukeRequest = new LukeRequest();
    lukeRequest.setNumTerms(0);
    LukeResponse lukeResponse;
    try {
      lukeResponse = lukeRequest.process(cloudSolrClient, collection);
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
    return lukeResponse.getFieldInfo();
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    for(Map.Entry<String, LukeResponse.FieldInfo> entry : this.fieldInfo.entrySet()) {
      EnumSet<FieldFlag> flags = entry.getValue().getFlags();
      RelDataType type;
      if(flags != null && flags.contains(FieldFlag.MULTI_VALUED)) {
        type = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.ANY), -1);
      } else {
        type = typeFactory.createSqlType(SqlTypeName.ANY);
      }
      builder.add(entry.getKey(), type);
    }

    return builder.build();
  }

  public Enumerable<Object> project(final int[] fields) {
    SolrTable solrTable = this;
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new SolrEnumerator(solrTable, fields);
      }
    };
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public Type getElementType() {
    return Object[].class;
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public RelNode toRel(
          RelOptTable.ToRelContext context,
          RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = SolrEnumerator.identityList(fieldCount);
    return new SolrTableScan(context.getCluster(), relOptTable, this, fields);
  }
}
