package net.bitnine.agens.hive.jdbctest;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

////////////////////////////////////////////////////
//
//	Cypher Test
//	- must be ready Livy Server before using AgensHiveStorageHandler
//

public class JdbcCypherTester implements JdbcTester {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    
    public JdbcCypherTester(
            String jdbcUrl,
            String jdbcUser,
            String jdbcPassword
    ) throws Exception {

        // Stream.of(jdbcUrl, jdbcUser, jdbcPassword).filter(Objects::nonNull).count()
        if( jdbcUrl == null || jdbcUser == null || jdbcPassword == null ) 
            throw new IllegalArgumentException("For cypher test, you need right jdbcUrl, jdbcUser, jdbcPassword");
        
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        
        // check if exists hive driver
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException("For cypher test, you need HIVE JDBC driver!", e.getCause());
        }
    }

    public void run() throws Exception {

        // replace "hive" here with the name of the user the queries should run as
        Connection conn = DriverManager.getConnection(
                "jdbc:hive2://tonyne.iptime.org:20000/default",        // jdbc url
                "bgmin",                                            // user
                "");                                                // password
        Statement stmt = conn.createStatement();

        /////////////////////////////

        // drop table if exists
        String tableName = "modern_test1";
        stmt.execute("drop table if exists " + tableName);

        // show tables
        String sql1 = "show tables in default";
        System.out.println("\n** Running: " + sql1);
        System.out.println("==>");
        ResultSet res1 = stmt.executeQuery(sql1);
        while (res1.next()) {
            System.out.println(res1.getString(1));
        }

        /////////////////////////////

/*
CREATE external TABLE modern_test1
STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'avro.schema.url'='hdfs://minmac:9000/user/agens/default.avsc',
'agens.spark.datasource'='modern',
'agens.spark.query'='match (a:person)-[b]-(c:person) return distinct a.id_, a.name, a.age, a.country, b.label, c.name'
);
 */
        // create table
        String sql2 = "create external table " + tableName + "\n" +
                "STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'" + "\n" +
                "TBLPROPERTIES(" + "\n" +
                "'avro.schema.url'='hdfs://minmac:9000/user/agens/default.avsc'," + "\n" +
                "'agens.spark.datasource'='modern'," + "\n" +
                "'agens.spark.query'='match (a:person)-[b]-(c:person) return distinct a.id_, a.name, a.age, a.country, b.label, c.name'" + "\n" +
                ")";
        System.out.println("\n** Running: \n" + sql2);
        System.out.println("==>");
        stmt.execute(sql2);

        // check if created or not table
        String sql3 = "show tables '" + tableName + "'";
        ResultSet res3 = stmt.executeQuery(sql3);
        if (res3.next()) {
            System.out.println(res3.getString(1));
        }

        // describe table
        String sql4 = "describe " + tableName;
        System.out.println("\n** Running: " + sql4);
        System.out.println("==>");
        ResultSet res4 = stmt.executeQuery(sql4);
        while (res4.next()) {
            System.out.println(res4.getString(1) + "\t" + res4.getString(2));
        }

        /////////////////////////////

        // select * query
        String sql5 = "select * from " + tableName;
        System.out.println("\n** Running: " + sql5);
        ResultSet res5 = stmt.executeQuery(sql5);

        ResultSetMetaData metadata = res5.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<String> colNames = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            colNames.add(metadata.getColumnName(i));
        }
        System.out.println("==> cols: " + colNames.stream().collect(Collectors.joining(", ")));
        System.out.println("--------------------------------------------------------------");
        while (res5.next()) {
            String row = "";
            for (int i = 1; i <= columnCount; i++) {
                row += res5.getString(i) + "\t";
            }
            System.out.println(row);
        }

        /////////////////////////////

        System.out.println("\n");
        if (!stmt.isClosed()) stmt.close();
        if (!conn.isClosed()) conn.close();
    }

    //////////////////////////////////////////////////////////

    private void selectAll() throws Exception {

    }

    private void listTables(String dbName) throws Exception {

    }

    private void descTable(String tableName) throws Exception {

    }

}
