package org.apache.solr.main;

import org.apache.solr.adapter.SolrSchemaFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by risdenk on 3/21/16.
 */
public class TestCalcite {
    public static void main(String[] args) {
        String zk = args[0];
        String sql = args[1];

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

        try(Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
            try(Statement stmt = conn.createStatement()) {
                try(ResultSet rs = stmt.executeQuery(sql)) {
                    ResultSetMetaData rsMetaData = rs.getMetaData();
                    while(rs.next()) {
                        for(int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                            List<Object> outputList = new ArrayList<>();
                            outputList.add(rsMetaData.getColumnName(i));
                            outputList.add(rsMetaData.getColumnTypeName(i));
                            outputList.add(rs.getString(i));
                            System.out.println(outputList);
                        }
                        System.out.println();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
