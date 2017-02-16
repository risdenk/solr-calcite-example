package org.apache.solr.handler.sql;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.VersionInfo;
import org.junit.*;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

abstract class TestBase {
  private static final String CONFIG_NAME = "test";
  private static final String COLLECTION_NAME = "test";

  private static Connection conn;
  private static MiniSolrCloudCluster miniSolrCloudCluster;
  private static Path tempLogDirectory;
  private static String zkAddress;

  String getZkAddress() {
    return zkAddress;
  }

  String getCollectionName() {
    return COLLECTION_NAME;
  }

  private Connection getConnection() {
    return conn;
  }

  abstract String getDriverClass();
  abstract String getURL();
  abstract Properties getProperties();

  private void setupConnection() throws Exception {
    try {
      Class.forName(getDriverClass());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    conn = DriverManager.getConnection(getURL(), getProperties());
  }

  static void setup() throws Exception {
    tempLogDirectory = Files.createTempDirectory(SolrAdapterTest.class.getCanonicalName());

    System.setProperty("solr.log.dir", tempLogDirectory.toString());

    setupSolr();
    indexDocs(miniSolrCloudCluster.getSolrClient(), COLLECTION_NAME);
  }

  private static void setupSolr() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").build();
    Path tempDirectory = Files.createTempDirectory(SolrAdapterTest.class.getSimpleName());
    tempDirectory.toFile().deleteOnExit();
    miniSolrCloudCluster = new MiniSolrCloudCluster(1, tempDirectory, jettyConfig);

    URL solr_conf = SolrAdapterTest.class.getClassLoader().getResource("solr_conf");
    assertNotNull(solr_conf);
    miniSolrCloudCluster.uploadConfigSet(Paths.get(solr_conf.toURI()), CONFIG_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
        .process(miniSolrCloudCluster.getSolrClient());

    zkAddress = miniSolrCloudCluster.getZkServer().getZkAddress();
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

  private static void indexDocs(CloudSolrClient solrClient, String collectionName) throws IOException, SolrServerException {
    /*
    * id,fielda,fieldb,fieldc,fieldd_s,fielde_i
    * 1,a1,b1,1,d1,1
    * 2,a2,b2,2,d1,2
    * 3,a1,b3,3,,1
    * 4,a1,b4,4,d2,
    * 5,a2,b2,,d2,2
    */
    List<SolrInputDocument> docs = new ArrayList<>();
    docs.add(makeInputDoc(1, "a1", "b1", 1, "d1", 1));
    docs.add(makeInputDoc(2, "a2", "b2", 2, "d1", 1));
    docs.add(makeInputDoc(3, "a1", "b3", 3, null, 1));
    docs.add(makeInputDoc(4, "a1", "b4", 4, "d2", null));
    docs.add(makeInputDoc(5, "a2", "b2", null, "d2", 2));

    solrClient.add(collectionName, docs);
    solrClient.commit(collectionName);
  }

  static void teardown() throws Exception {
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

    Files.deleteIfExists(tempLogDirectory);
  }

  @Before
  public void before() throws  Exception {
    setupConnection();
  }

  @Test
  public void testSelectStar() throws Exception {
    String sql = "select * from test";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }
  
  @Test
  public void testSelectStarLimit() throws Exception {
    String sql = "select * from test limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(fetch=[2])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereEqual() throws Exception {
    String sql = "select * from test where fielda = 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereNotEqual() throws Exception {
    String sql = "select * from test where fielda <> 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<>(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereLessThan() throws Exception {
    String sql = "select * from test where fielda < 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereLessThanEqual() throws Exception {
    String sql = "select * from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<=($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereGreaterThan() throws Exception {
    String sql = "select * from test where fielda > 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereGreaterThanEqual() throws Exception {
    String sql = "select * from test where fielda >= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>=($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereLuceneEqual() throws Exception {
    String sql = "select * from test where fielda = '(a1 a2)'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[=(CAST($0):VARCHAR(7) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '(a1 a2)')])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereFieldEqual() throws Exception {
    String sql = "select * from test where fielda = fielda";

    String explainPlan = "EnumerableCalc(expr#0..6=[{inputs}], expr#7=[CAST($t0):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#8=[=($t7, $t7)], proj#0..6=[{exprs}], $condition=[$t8])\n" +
        "  SolrToEnumerableConverter\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", null, "b2", "d2"});
    result.add(new Object[] {"a1", null, null, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleField() throws Exception {
    String sql = "select fielda from test";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldLimit() throws Exception {
    String sql = "select fielda from test limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldOrderByLimit() throws Exception {
    String sql = "select fielda from test order by fielda limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldOrderByDifferentFieldLimit() throws Exception {
    String sql = "select fielda from test order by fieldb limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5])\n" +
        "    SolrSort(sort0=[$5], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereEqual() throws Exception {
    String sql = "select fielda from test where fielda = 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereNotEqual() throws Exception {
    String sql = "select fielda from test where fielda <> 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[<>(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereLessThan() throws Exception {
    String sql = "select fielda from test where fielda < 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[<($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereLessThanEqual() throws Exception {
    String sql = "select fielda from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[<=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereGreaterThan() throws Exception {
    String sql = "select fielda from test where fielda > 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[>($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereGreaterThanEqual() throws Exception {
    String sql = "select fielda from test where fielda >= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[>=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereLuceneEqual() throws Exception {
    String sql = "select fielda from test where fielda = '(a1 a2)'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[=(CAST($0):VARCHAR(7) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '(a1 a2)')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereFieldEqual() throws Exception {
    String sql = "select fielda from test where fielda = fielda";

    String explainPlan = "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#2=[=($t1, $t1)], fielda=[$t0], $condition=[$t2])\n" +
        "  SolrToEnumerableConverter\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereLiteralEqual() throws Exception {
    String sql = "select fielda from test where 1 = 1";

    String explainPlan = "EnumerableCalc(expr#0=[{inputs}], expr#1=[1], expr#2=[=($t1, $t1)], fielda=[$t0], $condition=[$t2])\n" +
        "  SolrToEnumerableConverter\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldAlias() throws Exception {
    String sql = "select fielda as abc from test";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldAliasLimit() throws Exception {
    String sql = "select fielda as abc from test limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc from test order by abc asc limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFields() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsLimit() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsOrderByLimit() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test order by fielda limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereNotEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda <> 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[<>(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereLessThan() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda < 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[<($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereLessThanEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[<=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereGreaterThan() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda > 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[>($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereGreaterThanEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda >= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[>=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereLuceneEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = '(a1 a2)'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[=(CAST($0):VARCHAR(7) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", '(a1 a2)')])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereFieldEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = fielda";

    String explainPlan = "EnumerableCalc(expr#0..6=[{inputs}], expr#7=[CAST($t0):VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#8=[=($t7, $t7)], fielda=[$t0], fieldb=[$t5], fieldc=[$t4], fieldd_s=[$t6], fielde_i=[$t2], $condition=[$t8])\n" +
        "  SolrToEnumerableConverter\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsAlias() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(abc=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsAliasLimit() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(abc=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", null, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test order by abc limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(abc=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", null});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsAliasWhereMultipleOrderByLimit() throws Exception {
    String sql = "select fielda as abc, fieldb as def, fieldc, fieldd_s, fielde_i from test " +
        "where fielda = 'a1' and fieldb = 'b1' order by def limit 2";

    String explainPlan = "EnumerableLimit(fetch=[2])\n" +
        "  EnumerableSort(sort0=[$1], dir0=[ASC])\n" +
        "    EnumerableCalc(expr#0..6=[{inputs}], expr#7=[CAST($t0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#8=['a1'], expr#9=[=($t7, $t8)], expr#10=[CAST($t5):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], expr#11=['b1'], expr#12=[=($t10, $t11)], expr#13=[AND($t9, $t12)], abc=[$t0], def=[$t5], fieldc=[$t4], fieldd_s=[$t6], fielde_i=[$t2], $condition=[$t13])\n" +
        "      SolrToEnumerableConverter\n" +
        "        SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectCountStar() throws Exception {
    String sql = "select count(*) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
        "    SolrProject(DUMMY=[0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {5L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectCountOne() throws Exception {
    String sql = "select count(1) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
        "    SolrProject(DUMMY=[0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {5L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectCountField() throws Exception {
    String sql = "select count(fielda) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{5L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupBySingleField() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});
    result.add(new Object[]{"a2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupBySingleFieldHavingCountStar() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda having count(*) > 2";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>($1, 2)])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountOneGroupBySingleFieldOrderbySingleField() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda order by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$0], dir0=[ASC])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});
    result.add(new Object[]{"a2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupBySingleFieldOrderbyCountStar() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda order by count(*)";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$1], dir0=[ASC])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", 2L});
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupBySingleFieldOrderbySum() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda order by sum(fieldc)";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$2], dir0=[ASC])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()], agg#1=[SUM($4)])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", 2L});
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountOneGroupBySingleField() throws Exception {
    String sql = "select fielda, count(1) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});
    result.add(new Object[]{"a2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountOneGroupBySingleFieldHavingCountOne() throws Exception {
    String sql = "select fielda, count(1) from test group by fielda having count(1) > 2";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>($1, 2)])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountOneGroupBySingleFieldOrderbyCountOne() throws Exception {
    String sql = "select fielda, count(1) from test group by fielda order by count(1)";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$1], dir0=[ASC])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", 2L});
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountFieldGroupBySingleField() throws Exception {
    String sql = "select fielda, count(fielda) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT($0)])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});
    result.add(new Object[]{"a2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountFieldGroupBySingleFieldHavingCountField() throws Exception {
    String sql = "select fielda, count(fielda) from test group by fielda having count(fielda) > 2";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>($1, 2)])\n" +
        "    SolrAggregate(group=[{0}], EXPR$1=[COUNT($0)])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountDifferentFieldGroupBySingleField() throws Exception {
    String sql = "select fielda, count(fieldb) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT($5)])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", 3L});
    result.add(new Object[]{"a2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupByMultipleFields() throws Exception {
    String sql = "select fielda, fieldb, count(*) from test group by fielda, fieldb";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0, 5}], EXPR$2=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", "b2", 2L});
    result.add(new Object[]{"a1", "b1", 1L});
    result.add(new Object[]{"a1", "b4", 1L});
    result.add(new Object[]{"a1", "b3", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldCountStarGroupByMultipleFieldsOrderByAggregate() throws Exception {
    String sql = "select fielda, fieldb, count(*) from test group by fielda, fieldb order by count(*)";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$2], dir0=[ASC])\n" +
        "    SolrAggregate(group=[{0, 5}], EXPR$2=[COUNT()])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1", "b1", 1L});
    result.add(new Object[]{"a1", "b4", 1L});
    result.add(new Object[]{"a1", "b3", 1L});
    result.add(new Object[]{"a2", "b2", 2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldAggregates() throws Exception {
    String sql = "select min(fieldc) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[MIN($0)])\n" +
        "    SolrProject(fieldc=[$4])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectDistinctSingleField() throws Exception {
    String sql = "select distinct fielda from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a1"});
    result.add(new Object[]{"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectDistinctSingleFieldOrderByLimit() throws Exception {
    String sql = "select distinct fielda from test order by fielda desc limit 2";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$0], dir0=[DESC], fetch=[2])\n" +
        "    SolrAggregate(group=[{0}])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2"});
    result.add(new Object[]{"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectDistinctMultipleFields() throws Exception {
    String sql = "select distinct fielda, fieldb from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0, 5}])\n" +
        "    SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", "b2"});
    result.add(new Object[]{"a1", "b1"});
    result.add(new Object[]{"a1", "b4"});
    result.add(new Object[]{"a1", "b3"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectDistinctMultipleFieldsOrderByLimit() throws Exception {
    String sql = "select distinct fielda, fieldb from test order by fielda desc, fieldb desc limit 2";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrSort(sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[DESC], fetch=[2])\n" +
        "    SolrAggregate(group=[{0, 5}])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", "b2"});
    result.add(new Object[]{"a1", "b4"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectCountDistinctSingleField() throws Exception {
    String sql = "select count(distinct fielda) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT(DISTINCT $0)])\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{2L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSumDistinctSingleField() throws Exception {
    String sql = "select sum(distinct fieldc) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[SUM(DISTINCT $0)])\n" +
        "    SolrProject(fieldc=[$4])\n" +
        "      SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{10L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleAggregationsMultipleGroupBy() throws Exception {
    String sql = "select fielda, fieldb, min(fieldc), max(fieldc), cast(avg(1.0 * fieldc) as float), " +
        "sum(fieldc) from test group by fielda, fieldb";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$1], EXPR$2=[$2], EXPR$3=[$3], EXPR$4=[CAST($4):FLOAT], EXPR$5=[$5])\n" +
        "    SolrAggregate(group=[{0, 1}], EXPR$2=[MIN($2)], EXPR$3=[MAX($2)], agg#2=[AVG($3)], EXPR$5=[SUM($2)])\n" +
        "      SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], $f3=[*(1.0, $4)])\n" +
        "        SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", "b2", 2L, 2L, 2.0, 2L});
    result.add(new Object[]{"a1", "b1", 1L, 1L, 1.0, 1L});
    result.add(new Object[]{"a1", "b4", 4L, 4L, 4.0, 4L});
    result.add(new Object[]{"a1", "b3", 3L, 3L, 3.0, 3L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleAggregationsAliasesMultipleGroupBy() throws Exception {
    String sql = "select fielda as abc, fieldb as def, min(fieldc) as `min`, max(fieldc) as `max`, " +
        "cast(avg(1.0 * fieldc) as float) as `avg`, sum(fieldc) as `sum` from test group by fielda, fieldb";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(abc=[$0], def=[$1], min=[$2], max=[$3], avg=[CAST($4):FLOAT], sum=[$5])\n" +
        "    SolrAggregate(group=[{0, 1}], min=[MIN($2)], max=[MAX($2)], agg#2=[AVG($3)], sum=[SUM($2)])\n" +
        "      SolrProject(abc=[$0], def=[$5], fieldc=[$4], $f3=[*(1.0, $4)])\n" +
        "        SolrTableScan(table=[[" + getZkAddress() + ", " + getCollectionName() + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{"a2", "b2", 2L, 2L, 2.0, 2L});
    result.add(new Object[]{"a1", "b1", 1L, 1L, 1.0, 1L});
    result.add(new Object[]{"a1", "b4", 4L, 4L, 4.0, 4L});
    result.add(new Object[]{"a1", "b3", 3L, 3L, 3.0, 3L});

    checkQuery(sql, explainPlan, result);
  }

//  @Test
//  public void testAggregates() throws Exception {
//    try(Statement stmt = conn.createStatement()) {
//      String sql;
//
//      sql = "select fielda from test";
//      System.out.println(sql);
//      System.out.println(getExplainPlan(stmt, sql));
//      System.out.println();
//    }
//  }

  private String getExplainPlan(Statement stmt, String sql) throws SQLException {
    String explainSQL = "explain plan for " + sql;
    try (ResultSet rs = stmt.executeQuery(explainSQL)) {
      StringBuilder explainPlan = new StringBuilder();
      while (rs.next()) {
        explainPlan.append(rs.getString(1));
      }
      return explainPlan.toString();
    }
  }

  private List<Object[]> getResult(Statement stmt, String sql) throws SQLException {
    try (ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData rsMetaData = rs.getMetaData();
      List<Object[]> result = new ArrayList<>();
      while (rs.next()) {
        Object[] row = new Object[rsMetaData.getColumnCount()];
        for (int i = 0; i < rsMetaData.getColumnCount(); i++) {
          // Force _version_ to be null since can't check it
          if(VersionInfo.VERSION_FIELD.equals(rsMetaData.getColumnName(i+1))) {
            row[i] = null;
          } else {
            row[i] = rs.getObject(i+1);
          }
        }
        result.add(row);
      }
      return result;
    }
  }

  private void assertResultEquals(List<Object[]> expected, List<Object[]> actual) {
    assertEquals("Result is a different size", expected.size(), actual.size());
    for(int i = 0; i < expected.size(); i++) {
      Object[] expectedRow = expected.get(i);
      Object[] actualRow = actual.get(i);
      assertArrayEquals("Row " + i, expectedRow, actualRow);
    }
  }

  private void checkQuery(String sql, String explainPlan, List<Object[]> result) throws Exception {
    try (Statement stmt = getConnection().createStatement()) {
//      System.out.println(getExplainPlan(stmt, sql));
//      assertEquals(explainPlan, getExplainPlan(stmt, sql));
      assertResultEquals(result, getResult(stmt, sql));
    }
  }
  
}
