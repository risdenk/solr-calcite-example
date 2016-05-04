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
package org.apache.solr.main;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.server.Main;
import org.apache.solr.handler.sql.CalciteSolrDriver;

import java.sql.SQLException;
import java.util.List;

public class TestAvaticaServer {
  public static void main(String[] args) throws Exception {
//    args[0]: the Meta.Factory class name
//    args[1+]: arguments passed along to Meta.Factory.create(java.util.List)
    String[] myArgs = new String[]{MyMetaFactory.class.getName()};

    Main.main(myArgs);
  }

  @SuppressWarnings("unused")
  public static class MyMetaFactory implements Meta.Factory {
    public MyMetaFactory() {
      super();
    }

    @Override
    public Meta create(List<String> args) {
      try {
        Class.forName(CalciteSolrDriver.class.getCanonicalName());

        return new JdbcMeta(CalciteSolrDriver.CONNECT_STRING_PREFIX);
      } catch(SQLException|ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
