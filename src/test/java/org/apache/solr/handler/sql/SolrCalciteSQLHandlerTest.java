package org.apache.solr.handler.sql;

import org.apache.solr.client.solrj.io.sql.DriverImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

public class SolrCalciteSQLHandlerTest extends TestBase {
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
    return DriverImpl.class.getName();
  }

  @Override
  String getURL() {
    return "jdbc:solr://" + getZkAddress() + "?collection=" + getCollectionName();
  }

  @Override
  Properties getProperties() {
    return new Properties();
  }
}
