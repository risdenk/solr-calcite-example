package org.apache.solr.handler.sql;

import org.apache.calcite.config.Lex;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

public class SolrAdapterTest extends TestBase {
  @BeforeClass
  public static void setUp() throws Exception {
    TestBase.setup();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TestBase.teardown();
  }

  @Override
  String getDriverClass() {
    return CalciteSolrDriver.class.getName();
  }

  @Override
  String getURL() {
    return CalciteSolrDriver.CONNECT_STRING_PREFIX;
  }

  @Override
  Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty("lex", Lex.MYSQL.toString());
    properties.setProperty("zk", getZkAddress());
    return properties;
  }
}
