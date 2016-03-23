package org.apache.solr.main;

import org.apache.solr.adapter.SolrSchemaFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestCalcite {
  public static void main(String[] args) {
    String zk = args[0];

    List<String> sqlQueries = new ArrayList<>();
    sqlQueries.add("select * from test");
    sqlQueries.add("select * from test limit 2");
    sqlQueries.add("select * from test where fielda = 'a1'");
    sqlQueries.add("select * from test where fielda = '(a1 a2)'");
    sqlQueries.add("select fielda from test");
    sqlQueries.add("select fielda from test limit 2");
    sqlQueries.add("select fielda from test where fielda = 'a1'");
    sqlQueries.add("select fielda as abc from test where fielda = 'a1' AND fieldb = '(b1)'");
    sqlQueries.add("select fielda from test where fielda = '(a1 a2)'");
    sqlQueries.add("select fielda, fieldb, fieldc, fieldd_s, fielde_i from test");
    sqlQueries.add("select fielda, fieldb, fieldc, fieldd_s, fielde_i from test limit 2");
    sqlQueries.add("select fielda, fieldb, fieldc, fieldd_s, fielde_i from test where fielda = 'a1'");
    sqlQueries.add("select id, fielde_i, fieldd_s from test order by fielde_i desc");
    sqlQueries.add("select id as abc, fielde_i as def, fieldd_s as ghi from test order by fielde_i desc");
    sqlQueries.add("select fielda, fieldb, min(fieldc), max(fieldc), avg(fieldc), sum(fieldc) from test group by fielda, fieldb");
    sqlQueries.add("select fielda as abc, fieldb as def, min(fieldc) as `min`, max(fieldc) as `max`, avg(fieldc) as `avg`, sum(fieldc) as `sum` from test group by fielda, fieldb");

    Properties info = new Properties();
    info.setProperty("model",
        "inline:{\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"defaultSchema\": \"" + zk + "\",\n" +
            "  \"schemas\": [\n" +
            "    {\n" +
            "      \"name\": \"" + zk + "\",\n" +
            "      \"type\": \"custom\",\n" +
            "      \"factory\": \"" + SolrSchemaFactory.class.getName() + "\",\n" +
            "      \"operand\": {\n" +
            "        \"zk\": \"" + zk + "\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}");
    info.setProperty("lex", "MYSQL");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      try (Statement stmt = conn.createStatement()) {
        for (String sql : sqlQueries) {
          printExplain(stmt, sql);
          printResult(stmt, sql);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.exit(0);
  }

  private static void printResult(Statement stmt, String sql) throws SQLException {
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
        System.out.println();
      }
    }
  }

  private static void printExplain(Statement stmt, String sql) throws SQLException {
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
        System.out.println();
      }
    }
  }
}
