package org.apache.solr.main;

import org.apache.calcite.config.Lex;
import org.apache.solr.adapter.CalciteSolrDriver;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class TestCalcite {
  public static void main(String[] args) throws Exception {
    String zk = args[0];
    String sql = args[1];

    Properties info = new Properties();
    info.setProperty("lex", Lex.MYSQL.toString());
    info.setProperty("zk", zk);

    String driverClass = CalciteSolrDriver.class.getCanonicalName();
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    try (Connection conn = DriverManager.getConnection("jdbc:calcitesolr:", info)) {
      try (Statement stmt = conn.createStatement()) {
        printExplain(stmt, sql);
        printResult(stmt, sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.exit(0);
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
      }
      System.out.println();
    }
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
      }
      System.out.println();
    }
  }
}
