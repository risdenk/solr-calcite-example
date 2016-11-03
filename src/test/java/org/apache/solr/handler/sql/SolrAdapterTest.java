package org.apache.solr.handler.sql;

import org.apache.calcite.config.Lex;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.VersionInfo;
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

public class SolrAdapterTest extends TestBase {
  private static final String CONFIG_NAME = "test";
  private static final String COLLECTION_NAME = "test";

  private static Path tempLogDirectory;
  private static MiniSolrCloudCluster miniSolrCloudCluster;
  private static Connection conn;
  private static String zkAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tempLogDirectory = Files.createTempDirectory(SolrAdapterTest.class.getCanonicalName());

    System.setProperty("solr.log.dir", tempLogDirectory.toString());

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
    miniSolrCloudCluster.uploadConfigSet(Paths.get(solr_conf.toURI()), CONFIG_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
        .process(miniSolrCloudCluster.getSolrClient());

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
    String driverClass = CalciteSolrDriver.class.getName();
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    String url = CalciteSolrDriver.CONNECT_STRING_PREFIX;
    Properties properties = new Properties();
    properties.setProperty("lex", Lex.MYSQL.toString());
    properties.setProperty("zk", zkAddress);
    conn = DriverManager.getConnection(url, properties);
    System.out.println(url);
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

    Files.deleteIfExists(tempLogDirectory);
  }

  @Override
  String getZkAddress() {
    return zkAddress;
  }

  @Override
  String getCollectionName() {
    return COLLECTION_NAME;
  }

  @Override
  Connection getConnection() {
    return conn;
  }
}
