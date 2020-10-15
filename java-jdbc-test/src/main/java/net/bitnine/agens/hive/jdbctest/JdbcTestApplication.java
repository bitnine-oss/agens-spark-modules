package net.bitnine.agens.hive.jdbctest;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
public class JdbcTestApplication implements ApplicationRunner {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String optionCypher = "cypher";    // using AgensHiveStorageHandler with Livy

	private String option = "sql";            // (default) using EsStorageHandler and Agens UDFs

	public static void main(String[] args) throws Exception {
		SpringApplication.run(JdbcTestApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		// Parsing Arguments in Commandline
		// https://codeboje.de/spring-boot-commandline-app-args/
		if (args.getNonOptionArgs().size() > 0) {
			if (args.getNonOptionArgs().get(0).equalsIgnoreCase(optionCypher))
				this.option = optionCypher;
		}
		// else: option = "sql"

		System.out.println(String.format("Hello, JdbcTestApplication (%s)", this.option));
		System.out.println("==>\n");

		/////////////////////////////

		// check if exists hive driver
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		/////////////////////////////

		if (this.option.equals(optionCypher)) cypherTest();
		else sqlTest();
	}

	////////////////////////////////////////////////////

	////////////////////////////////////////////////////
	//
	//	Cypher Test
	//	- must be ready Livy Server before using AgensHiveStorageHandler
	//

	private void cypherTest() throws SQLException {

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

	////////////////////////////////////////////////////

	////////////////////////////////////////////////////
	//
	//	SQL Test
	//	- must be load elasticsearch-hadoop-hive jar on hive before using EsStorageHandler
	//

	private void sqlTest() throws SQLException {

		// replace "hive" here with the name of the user the queries should run as
		Connection conn = DriverManager.getConnection(
				"jdbc:hive2://tonyne.iptime.org:20000/default",        // jdbc url
				"bgmin",                                            // user
				"");                                                // password
		Statement stmt = conn.createStatement();

		/////////////////////////////

		// drop table if exists
		String tableName = "modern_test2";
		stmt.execute("drop table if exists " + tableName);

/*
drop table if exists `modern_test2`;

CREATE EXTERNAL TABLE if not exists `modern_test2` (
  `timestamp` TIMESTAMP,    -- default: current_timestamp()
  `datasource` STRING,
  `deleted` CHAR(1),        -- UPPERCASE: Y or N
  `id` STRING,
  `label` STRING,
  `properties` ARRAY<STRUCT<`key`:STRING,`type`:STRING,`value`:STRING>>
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.resource'='agensvertex2',
  'es.nodes'='minmac:29200',            -- <IP>:<Port>
  'es.nodes.wan.only'='true',           -- if true, just use one node (performance down)
  'es.net.http.auth.user'='elastic',
  'es.net.http.auth.pass'='bitnine',
  'es.index.auto.create'='false',       -- prevent modification of index
  'es.write.operation'='upsert',        -- if exists same `ID`, then overwrite
  'es.query'='?q=datasource:modern AND deleted:N',  -- multiple: '%20AND%20'
  'es.read.field.as.array.include'='properties',    -- https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-field-info
  'es.mapping.id'='id'
  -- ,'es.mapping.names'='timestamp:timestamp, datasource:datasource, deleted:deleted, label:label, id:id, properties:properties'
);

 */
		// es-connection info.
		String indexName = "agensvertex2";
		String esUrl = "minmac:29200";
		String esUser = "elastic";
		String esPassword = "bitnine";
		String datasource = "modern";

		// create table
		String sql1 = "create external table if not exists `" + tableName + "` (\n" +
				"`timestamp` TIMESTAMP,"+"\n"+
				"`datasource` STRING,"+"\n"+
				"`deleted` CHAR(1),"+"\n"+
				"`id` STRING,"+"\n"+
				"`label` STRING,"+"\n"+
				"`properties` ARRAY<STRUCT<`key`:STRING,`type`:STRING,`value`:STRING>>"+"\n"+
				") STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'"+"\n"+
				"TBLPROPERTIES("+"\n"+
				"'es.resource'='"+indexName+"',"+"\n"+
				"'es.nodes'='"+esUrl+"',"+"\n"+
				"'es.nodes.wan.only'='true',"+"\n"+
				"'es.net.http.auth.user'='"+esUser+"',"+"\n"+
				"'es.net.http.auth.pass'='"+esPassword+"',"+"\n"+
				"'es.index.auto.create'='false',"+"\n"+
				"'es.write.operation'='upsert',"+"\n"+
				"'es.query'='?q=datasource:"+datasource+" AND deleted:N',"+"\n"+
				"'es.read.field.as.array.include'='properties',"+"\n"+
				"'es.mapping.id'='id'"+"\n"+
				")";

		System.out.println("\n** Running: \n" + sql1);
		System.out.println("==>");
		stmt.execute(sql1);

		// check if created or not table
		String sql2 = "show tables '" + tableName + "'";
		ResultSet res2 = stmt.executeQuery(sql2);
		if (res2.next()) {
			System.out.println(res2.getString(1));
		}

		// describe table
		String sql3 = "describe " + tableName;
		System.out.println("\n** Running: " + sql3);
		System.out.println("==>");
		ResultSet res3 = stmt.executeQuery(sql3);
		while (res3.next()) {
			System.out.println(res3.getString(1) + "\t" + res3.getString(2));
		}

		/////////////////////////////

		// select count(*) as cnt
		String sql4 = "select count(*) as cnt from " + tableName;
		System.out.println("\n** Running: " + sql4);
		ResultSet res4 = stmt.executeQuery(sql4);
		if( res4.next() ){
			System.out.println(res4.getLong(1));
		}

		/////////////////////////////

		// select *
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

/*
insert OVERWRITE TABLE `modern_test2`
select current_timestamp(), 'N', 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Agens'))

insert OVERWRITE TABLE `modern_test2`
select current_timestamp(), 'N', 'modern_102', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Graph'))
 */
		// insert overwrite into
		String sql6_1 = "insert OVERWRITE TABLE `" + tableName + "`"+"\n"+
				"select current_timestamp(), 'N', 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Agens'))";
		System.out.println("\n** Running: \n" + sql6_1);
		int res6_1 = stmt.executeUpdate(sql6_1);
		System.out.println("==> "+res6_1);

		String sql6_2 = "insert OVERWRITE TABLE `" + tableName + "`"+"\n"+
				"select current_timestamp(), 'N', 'modern_102', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Graph'))";
		System.out.println("\n** Running: \n" + sql6_2);
		int res6_2 = stmt.executeUpdate(sql6_2);
		System.out.println("==> "+res6_2);

		/////////////////////////////

		// select *
		String sql7 = "select * from " + tableName;
		System.out.println("\n** Running: " + sql7);
		ResultSet res7 = stmt.executeQuery(sql7);
		// columnCount is same (previous select query)
		while (res7.next()) {
			String row = "";
			for (int i = 1; i <= columnCount; i++) {
				row += res7.getString(i) + "\t";
			}
			System.out.println(row);
		}

		/////////////////////////////

/*
update modern_test2 set properties = array(struct<>,..) where id = 'modern_101'
==>
insert OVERWRITE TABLE `modern_test2`
select current_timestamp(), 'N', 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Thomas'),named_struct('key','age','type','java.lang.Integer','value','44'))
 */
		// insert overwrite into
		String sql8 = "insert OVERWRITE TABLE `" + tableName + "`"+"\n"+
				"select current_timestamp(), 'N', 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Bitnine'),named_struct('key','age','type','java.lang.Integer','value','44'))";
		System.out.println("\n** Running: \n" + sql8);
		int res8 = stmt.executeUpdate(sql8);
		System.out.println("==> "+res8);

		/////////////////////////////

		// select *
		String sql9 = "select * from " + tableName;
		System.out.println("\n** Running: " + sql9);
		ResultSet res9 = stmt.executeQuery(sql9);
		// columnCount is same (previous select query)
		while (res9.next()) {
			String row = "";
			for (int i = 1; i <= columnCount; i++) {
				row += res9.getString(i) + "\t";
			}
			System.out.println(row);
		}

		/////////////////////////////

/*
delete from modern_test2 where id = 'modern_102'
==>
insert OVERWRITE TABLE `modern_test2`
select `timestamp`, 'Y' as deleted, id, datasource, label, properties from `modern_test2` where id in ('modern_102')
 */
		// insert overwrite into
		String sql10 = "insert OVERWRITE TABLE `" + tableName + "`"+"\n"+
				"select `timestamp`, 'Y' as deleted, id, datasource, label, properties from `" + tableName + "` where id in ('modern_102')";
		System.out.println("\n** Running: \n" + sql10);
		int res10 = stmt.executeUpdate(sql10);
		System.out.println("==> "+res10);

		/////////////////////////////

		// select *
		String sql11 = "select * from " + tableName;
		System.out.println("\n** Running: " + sql11);
		ResultSet res11 = stmt.executeQuery(sql11);
		// columnCount is same (previous select query)
		while (res11.next()) {
			String row = "";
			for (int i = 1; i <= columnCount; i++) {
				row += res11.getString(i) + "\t";
			}
			System.out.println(row);
		}

		/////////////////////////////

		System.out.println("\n");
		if (!stmt.isClosed()) stmt.close();
		if (!conn.isClosed()) conn.close();
	}

}