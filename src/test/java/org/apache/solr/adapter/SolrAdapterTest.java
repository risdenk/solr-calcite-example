package org.apache.solr.adapter;

import org.apache.calcite.config.Lex;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class SolrAdapterTest {
  private static final String CONFIG_NAME = "test";
  private static final String COLLECTION_NAME = "test";

  private static MiniSolrCloudCluster miniSolrCloudCluster;
  private static Connection conn;

  @BeforeClass
  public static void setUp() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").build();
    Path tempDirectory = Files.createTempDirectory(SolrAdapterTest.class.getSimpleName());
    tempDirectory.toFile().deleteOnExit();
    miniSolrCloudCluster = new MiniSolrCloudCluster(1, tempDirectory, jettyConfig);
    URL solr_conf = SolrAdapterTest.class.getClassLoader().getResource("solr_conf");
    assertNotNull(solr_conf);
    miniSolrCloudCluster.uploadConfigDir(Paths.get(solr_conf.toURI()).toFile(), CONFIG_NAME);
    miniSolrCloudCluster.createCollection(COLLECTION_NAME, 1, 1, CONFIG_NAME, Collections.emptyMap());

    indexDocs();

    String driverClass = CalciteSolrDriver.class.getCanonicalName();
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    Properties properties = new Properties();
    properties.setProperty("lex", Lex.MYSQL.toString());
    properties.setProperty("zk", miniSolrCloudCluster.getZkServer().getZkAddress());
    conn = DriverManager.getConnection("jdbc:calcitesolr:", properties);
  }

  private static void indexDocs() throws IOException, SolrServerException {
    /*
    * id,fielda,fieldb,fieldc,fieldd_s,fielde_i
    * 1,a1,b1,1,d1,1
    * 2,a2,b2,2,d1,2
    * 3,a1,b3,3,,1
    * 4,a1,b4,4,d2,
    * 5,a2,b2,,d2,2
    */
    CloudSolrClient solrClient = miniSolrCloudCluster.getSolrClient();
    List<SolrInputDocument> docs = new ArrayList<>();
    docs.add(makeInputDoc(1, "a1", "b1", 1, "d1", 1));
    docs.add(makeInputDoc(2, "a2", "b2", 2, "d1", 1));
    docs.add(makeInputDoc(3, "a1", "b3", 3, null, 1));
    docs.add(makeInputDoc(4, "a1", "b4", 4, "d2", null));
    docs.add(makeInputDoc(5, "a2", "b2", null, "d2", 2));

    solrClient.add(COLLECTION_NAME, docs);
    solrClient.commit(COLLECTION_NAME);
  }

  private static SolrInputDocument makeInputDoc(Integer id, String fielda, String fieldb, Integer fieldc, String fieldd,
                                                Integer fielde) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    doc.addField("fielda", fielda);
    doc.addField("fieldb", fieldb);
    doc.addField("fieldc", fieldc);
    doc.addField("fieldd_s", fieldd);
    doc.addField("fielde_i", fielde);
    return doc;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if(conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      // Don't care if the connection failed to close. Just log the error
      e.printStackTrace(System.err);
    } finally {
      conn = null;
    }

    try {
      if(miniSolrCloudCluster != null) {
        miniSolrCloudCluster.shutdown();
      }
    } finally {
      miniSolrCloudCluster = null;
    }
  }

  @Test
  public void testSelectStar() throws Exception {
    String sql = "select * from test";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectStarLimit() throws Exception {
    String sql = "select * from test limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Ignore("Select * with no limit not supported")
  @Test
  public void testSelectStarWhereEquality() throws Exception {
    String sql = "select * from test where fielda = 'a1'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Ignore("Select * with no limit not supported")
  @Test
  public void testSelectStarWhereLuceneEquality() throws Exception {
    String sql = "select * from test where fielda = '(a1 a2)'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Ignore("Select * with no limit not supported")
  @Test
  public void testSelectStarWhereFieldEquality() throws Exception {
    String sql = "select * from test where fielda = fieldb";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleField() throws Exception {
    String sql = "select fielda from test";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldLimit() throws Exception {
    String sql = "select fielda from test limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldOrderByLimit() throws Exception {
    String sql = "select fielda from test order by fielda limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldWhereEquality() throws Exception {
    String sql = "select fielda from test where fielda = 'a1'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldWhereLuceneEquality() throws Exception {
    String sql = "select fielda from test where fielda = '(a1 a2)'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldAlias() throws Exception {
    String sql = "select fielda as abc from test";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldAliasLimit() throws Exception {
    String sql = "select fielda as abc from test limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectSingleFieldAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc from test order by abc limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFields() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsLimit() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsOrderByLimit() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test order by fielda limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsWhereEquality() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = 'a1'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsWhereLuceneEquality() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = '(a1 a2)'";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsAlias() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsAliasLimit() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

  @Test
  public void testSelectMultipleFieldsAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test order by abc limit 2";
    try (Statement stmt = conn.createStatement()) {
      printExplain(stmt, sql);
      printResult(stmt, sql);
    }
  }

//select fielda, fieldb, min(fieldc), max(fieldc), avg(fieldc), sum(fieldc) from test group by fielda, fieldb
//select fielda as abc, fieldb as def, min(fieldc) as `min`, max(fieldc) as `max`, avg(fieldc) as `avg`, sum(fieldc) as `sum` from test group by fielda, fieldb

  private void printExplain(Statement stmt, String sql) throws SQLException {
    String explainSQL = "explain plan for " + sql;
    System.out.println("-----" + System.lineSeparator() + explainSQL + System.lineSeparator() + "-----");
    try (ResultSet rs = stmt.executeQuery(explainSQL)) {
      ResultSetMetaData rsMetaData = rs.getMetaData();
      while (rs.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          List<Object> outputList = new ArrayList<>();
          outputList.add(rs.getString(i));
          System.out.println(outputList);
        }
      }
      System.out.println();
    }
  }

  private void printResult(Statement stmt, String sql) throws SQLException {
    System.out.println("-----" + System.lineSeparator() + sql + System.lineSeparator() + "-----");
    try (ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData rsMetaData = rs.getMetaData();
      while (rs.next()) {
        for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
          List<Object> outputList = new ArrayList<>();
          outputList.add(rsMetaData.getColumnName(i));
          outputList.add(rsMetaData.getColumnLabel(i));
          outputList.add(rsMetaData.getColumnTypeName(i));
          outputList.add(rs.getString(i));
          System.out.println(outputList);
        }
      }
      System.out.println();
    }
  }
}
