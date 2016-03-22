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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.TupleStream;

import java.io.IOException;
import java.util.List;

/** Enumerator that reads from a Solr collection. */
class SolrEnumerator implements Enumerator<Object> {
  private final TupleStream tupleStream;
  private Tuple current;
  private List<RelDataTypeField> fieldTypes;

  /** Creates a SolrEnumerator.
   *
   * @param tupleStream Solr TupleStream
   * @param protoRowType The type of resulting rows
   */
  SolrEnumerator(TupleStream tupleStream, RelProtoDataType protoRowType) {
    this.tupleStream = tupleStream;
    this.current = null;

    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
  }

  /** Produce the next row from the results
   *
   * @return A new row from the results
   */
  public Object current() {
    if (fieldTypes.size() == 1) {
      // If we just have one field, produce it directly
      RelDataTypeField relDataTypeField = fieldTypes.get(0);
      return currentTupleField(relDataTypeField.getKey());
    } else {
      // Build an array with all fields in this row
      Object[] row = new Object[fieldTypes.size()];
      for (int i = 0; i < fieldTypes.size(); i++) {
        RelDataTypeField relDataTypeField = fieldTypes.get(i);
        row[i] = currentTupleField(relDataTypeField.getKey());
      }

      return row;
    }
  }

  /** Get a field for the current tuple from the underlying object.
   *
   * @param field String of field to get
   */
  private Object currentTupleField(String field) {
    return current.get(field);
  }

  public boolean moveNext() {
    try {
      Tuple tuple = this.tupleStream.read();
      if (tuple.EOF) {
        return false;
      } else {
        current = tuple;
        return true;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    if(this.tupleStream != null) {
      try {
        this.tupleStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
