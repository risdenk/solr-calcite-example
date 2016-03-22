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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.util.Map;
import java.util.Set;

class SolrSchema extends AbstractSchema {
  private final CloudSolrClient cloudSolrClient;

  SolrSchema(String zk) {
    super();
    this.cloudSolrClient = new CloudSolrClient(zk);
    this.cloudSolrClient.connect();
  }

  @Override
  protected Map<String, Table> getTableMap() {
    Set<String> collections = this.cloudSolrClient.getZkStateReader().getClusterState().getCollections();
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (String collection : collections) {
      builder.put(collection, new SolrTable(this.cloudSolrClient, collection));
    }
    return builder.build();
  }
}
