package org.apache.solr.adapter;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.common.params.CommonParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

class SolrEnumerator implements Enumerator<Object> {
  private final List<String> selectedFields;
  private final CloudSolrStream cloudSolrStream;
  private Object current;

  SolrEnumerator(SolrTable solrTable, int[] fields) {
    selectedFields = new ArrayList<>(fields.length);
    for (int field : fields) {
      selectedFields.add(solrTable.allFields.get(field));
    }

    try {
      Map<String, String> solrParams = new HashMap<>();
      solrParams.put(CommonParams.Q, "*:*");
      solrParams.put(CommonParams.FL, String.join(",", selectedFields) + ",_version_");
      solrParams.put(CommonParams.SORT, "_version_ desc");
      //solrParams.put(CommonParams.QT, "/export");

      cloudSolrStream = new CloudSolrStream(solrTable.cloudSolrClient.getZkHost(), solrTable.collection, solrParams);
      cloudSolrStream.open();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    try {
      Tuple tuple = cloudSolrStream.read();
      if (!tuple.EOF) {
        current = this.converter(tuple);
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
    try {
      cloudSolrStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Object converter(Tuple tuple) {
    List<Object> row = new ArrayList<>(selectedFields.size());
    for(String field : selectedFields) {
      Object o = tuple.get(field);
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
