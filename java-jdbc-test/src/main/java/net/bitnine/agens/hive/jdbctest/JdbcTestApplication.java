package net.bitnine.agens.hive.jdbctest;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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

		JdbcTester tester = null;
		try{
			if( this.option.equals(optionCypher) ){
				tester = new JdbcCypherTester(
						"jdbc:hive2://tonyne.iptime.org:20000/default",        // jdbc url
						"bgmin",                                            // user
						""
				);
			}
			else{
				tester = new JdbcSqlTester(
						"jdbc:hive2://tonyne.iptime.org:20000/default",        // jdbc url
						"bgmin",                                            // user
						""
				);
			}
		}
		catch( IllegalArgumentException|ClassNotFoundException e ){
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

		/////////////////////////////

		//	**NOTE
		//	올바른 테스트를 위해 ES 의 index 를 재생성 해야 함 ==> resources/scripts 실행

		if( tester != null ) tester.run();
	}

}