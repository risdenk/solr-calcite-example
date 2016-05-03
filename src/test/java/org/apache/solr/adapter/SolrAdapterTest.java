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
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SolrAdapterTest {
  private static final String CONFIG_NAME = "test";
  private static final String COLLECTION_NAME = "test";

  private static MiniSolrCloudCluster miniSolrCloudCluster;
  private static Connection conn;
  private static String zkAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    setupSolr();
    indexDocs();
    setupConnection();
  }

  private static void setupSolr() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").build();
    Path tempDirectory = Files.createTempDirectory(SolrAdapterTest.class.getSimpleName());
    tempDirectory.toFile().deleteOnExit();
    miniSolrCloudCluster = new MiniSolrCloudCluster(1, tempDirectory, jettyConfig);

    URL solr_conf = SolrAdapterTest.class.getClassLoader().getResource("solr_conf");
    assertNotNull(solr_conf);
    miniSolrCloudCluster.uploadConfigDir(Paths.get(solr_conf.toURI()).toFile(), CONFIG_NAME);

    miniSolrCloudCluster.createCollection(COLLECTION_NAME, 1, 1, CONFIG_NAME, Collections.emptyMap());

    zkAddress = miniSolrCloudCluster.getZkServer().getZkAddress();
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

  private static void setupConnection() throws Exception {
    String driverClass = CalciteSolrDriver.class.getCanonicalName();
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    Properties properties = new Properties();
    properties.setProperty("lex", Lex.MYSQL.toString());
    properties.setProperty("zk", zkAddress);
    conn = DriverManager.getConnection("jdbc:calcitesolr:", properties);
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

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereEqual() throws Exception {
    String sql = "select * from test where fielda = 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereNotEqual() throws Exception {
    String sql = "select * from test where fielda <> 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<>(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereLessThan() throws Exception {
    String sql = "select * from test where fielda < 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereLessThanEqual() throws Exception {
    String sql = "select * from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[<=($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereGreaterThan() throws Exception {
    String sql = "select * from test where fielda > 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectStarWhereGreaterThanEqual() throws Exception {
    String sql = "select * from test where fielda >= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrFilter(condition=[>=($0, 'a1')])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
    result.add(new Object[] {"a1", null, 1L, "3", 3L, "b3", null});
    result.add(new Object[] {"a2", null, 1L, "2", 2L, "b2", "d1"});
    result.add(new Object[] {"a1", null, 1L, "1", 1L, "b1", "d1"});

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Field equality is not supported")
  @Test
  public void testSelectStarWhereFieldEqual() throws Exception {
    String sql = "select * from test where fielda = fieldb";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", null, 2L, "5", 0L, "b2", "d2"});
    result.add(new Object[] {"a1", null, 0L, "4", 4L, "b4", "d2"});
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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldOrderByLimit() throws Exception {
    String sql = "select fielda from test order by fielda limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldWhereLessThanEqual() throws Exception {
    String sql = "select fielda from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrFilter(condition=[<=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});
    result.add(new Object[] {"a1"});

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Field equality is not supported")
  @Test
  public void testSelectSingleFieldWhereFieldEqual() throws Exception {
    String sql = "select fielda from test where fielda = fieldb";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1"});
    result.add(new Object[] {"a2"});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectSingleFieldAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc from test order by abc asc limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsOrderByLimit() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test order by fielda limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'a1')])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereLessThan() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda < 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[<($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereLessThanEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda <= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[<=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsWhereGreaterThanEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda >= 'a1'";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(fielda=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrFilter(condition=[>=($0, 'a1')])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Field equality is not supported")
  @Test
  public void testSelectMultipleFieldsWhereFieldEqual() throws Exception {
    String sql = "select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = fieldb";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a2", "b2", 0L, "d2", 2L});
    result.add(new Object[] {"a1", "b4", 4L, "d2", 0L});
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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});
    result.add(new Object[] {"a2", "b2", 2L, "d1", 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectMultipleFieldsAliasOrderByLimit() throws Exception {
    String sql = "select fielda as abc, fieldb, fieldc, fieldd_s, fielde_i from test order by abc limit 2";

    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrProject(abc=[$0], fieldb=[$5], fieldc=[$4], fieldd_s=[$6], fielde_i=[$2])\n" +
        "    SolrSort(sort0=[$0], dir0=[ASC], fetch=[2])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {"a1", "b1", 1L, "d1", 1L});
    result.add(new Object[] {"a1", "b3", 3L, null, 1L});

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testSelectCountStar() throws Exception {
    String sql = "select count(*) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
        "    SolrProject(DUMMY=[0])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

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
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();
    result.add(new Object[] {5L});

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Can't currently handle count(FIELD) queries")
  @Test
  public void testSelectCountField() throws Exception {
    String sql = "select count(fielda) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Broken because CountMetric returns a double instead of a long")
  @Test
  public void testSelectSingleFieldCountStarGroupBySingleField() throws Exception {
    String sql = "select fielda, count(*) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Broken because CountMetric returns a double instead of a long")
  @Test
  public void testSelectSingleFieldCountOneGroupBySingleField() throws Exception {
    String sql = "select fielda, count(1) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Can't currently handle count(FIELD) queries")
  @Test
  public void testSelectSingleFieldCountFieldGroupBySingleField() throws Exception {
    String sql = "select fielda, count(fielda) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Can't currently handle count(FIELD) queries")
  @Test
  public void testSelectSingleFieldCountDifferentFieldGroupBySingleField() throws Exception {
    String sql = "select fielda, count(fieldb) from test group by fielda";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
        "    SolrProject(fielda=[$0])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore("Broken because CountMetric returns a double instead of a long")
  @Test
  public void testSelectSingleFieldCountStarGroupByMultipleFields() throws Exception {
    String sql = "select fielda, fieldb, count(*) from test group by fielda, fieldb";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{0, 5}], EXPR$2=[COUNT()])\n" +
        "    SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Ignore
  @Test
  public void testSelectSingleFieldAggregates() throws Exception {
    String sql = "select min(fieldc) from test";
    String explainPlan = "SolrToEnumerableConverter\n" +
        "  SolrAggregate(group=[{}], EXPR$0=[MIN($0)])\n" +
        "    SolrProject(fieldc=[$4])\n" +
        "      SolrTableScan(table=[[" + zkAddress + ", " + COLLECTION_NAME + "]])\n";

    List<Object[]> result = new ArrayList<>();

    checkQuery(sql, explainPlan, result);
  }

  @Test
  public void testAggregates() throws Exception {
    try(Statement stmt = conn.createStatement()) {
      String sql;

      sql = "select count(distinct fielda) from test";
      System.out.println(sql);
      System.out.println(getExplainPlan(stmt, sql));
      System.out.println();

      sql = "select sum(distinct fieldc) from test";
      System.out.println(sql);
      System.out.println(getExplainPlan(stmt, sql));
      System.out.println();

      sql = "select fielda, fieldb, min(fieldc), max(fieldc), avg(fieldc), sum(fieldc) from test group by fielda, fieldb";
      System.out.println(sql);
      System.out.println(getExplainPlan(stmt, sql));
      System.out.println();

      sql = "select fielda as abc, fieldb as def, min(fieldc) as `min`, max(fieldc) as `max`, avg(fieldc) as `avg`, sum(fieldc) as `sum` from test group by fielda, fieldb";
      System.out.println(sql);
      System.out.println(getExplainPlan(stmt, sql));
      System.out.println();
    }
  }

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
          if("_version_".equals(rsMetaData.getColumnName(i+1))) {
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

  private void assertResultEquals(List<Object[]> expected, List<Object[]> actual) throws Exception {
    assertEquals("Result is a different size", expected.size(), actual.size());
    for(int i = 0; i < expected.size(); i++) {
      Object[] expectedRow = expected.get(i);
      Object[] actualRow = actual.get(i);
      assertArrayEquals("Row " + i, expectedRow, actualRow);
    }
  }

  private void checkQuery(String sql, String explainPlan, List<Object[]> result) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      assertEquals(explainPlan, getExplainPlan(stmt, sql));
      assertResultEquals(result, getResult(stmt, sql));
    }
  }
}
