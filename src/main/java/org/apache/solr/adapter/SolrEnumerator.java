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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.*;

class SolrEnumerator implements Enumerator<Object> {
  private final Iterator<SolrDocument> resultIterator;
  private final List<String> selectedFields;
  private Object current;

  SolrEnumerator(SolrTable solrTable, int[] fields) {
    selectedFields = new ArrayList<>(fields.length);
    for (int field : fields) {
      selectedFields.add(solrTable.allFields.get(field));
    }

    try {
      final ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set("q", "*:*");
      solrParams.set("fl", String.join(",", selectedFields));
      QueryResponse queryResponse = solrTable.cloudSolrClient.query(solrTable.collection, solrParams);
      this.resultIterator = queryResponse.getResults().iterator();
    } catch (SolrServerException|IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    try {
      if (resultIterator.hasNext()) {
        current = this.converter(resultIterator.next());
        return true;
      } else {
        current = null;
        return false;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
  }

  private Object converter(SolrDocument doc) {
    List<Object> row = new ArrayList<>(selectedFields.size());
    for(String field : selectedFields) {
      Object o = doc.get(field);
      if(o instanceof List) {
        row.add(((List)o).get(0));
      } else {
        row.add(o);
      }
    }

    Object[] ret = row.toArray(new Object[selectedFields.size()]);

    if(selectedFields.size() == 1) {
      return ret[0];
    } else {
      return ret;
    }
  }

  /** Returns an array of integers {0, ..., n - 1}. */
  static int[] identityList(int n) {
    int[] integers = new int[n];
    for (int i = 0; i < n; i++) {
      integers[i] = i;
    }
    return integers;
  }
}
