package net.bitnine.agens.livy.app;

import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.RunCypherJob;
import net.bitnine.agens.livy.util.AgensLivyJobException;

import java.util.HashMap;
import java.util.Map;

public class RunCypherApp {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: RunCypherApp <livyUrl> <datasource> <name> <cypher>");
            System.exit(-1);
        }

        // options: livyUri, datasource, name, query
        Map<String,String> parameters = new HashMap<>();
        parameters.put("agens.spark.livy", args[0]);
        parameters.put("agens.spark.datasource", args[1]);
        parameters.put("agens.spark.name", args[2]);
        parameters.put("agens.spark.query", args[3]);

        RunCypherJob job = new RunCypherJob();
        try{
            String schemaJson = job.run(
                    parameters.get("agens.spark.livy"),
                    parameters.get("agens.spark.datasource"),
                    parameters.get("agens.spark.name"),
                    parameters.get("agens.spark.query")
            );
            // for DEBUG
            System.out.println("RunCypherApp ==>\n" + schemaJson);
        }
        catch (AgensLivyJobException ex){
            System.err.println("Error: AgensLivyJobException\n"+ex.getMessage());
            System.exit(-1);
        }
    }

}
/*
## requirement!!
## ==> 하나 이상의 jar 포함시, linux는 ':', windows는 ';'으로 구분

[spark-defaults.conf]
spark.driver.extraClassPath     /Users/bgmin/Servers/extraJars/agens-spark-connector-1.0-dev.jar
spark.executor.extraClassPath   /Users/bgmin/Servers/extraJars/agens-spark-connector-1.0-dev.jar

============================

java -cp target/agens-livy-test-1.0-dev.jar \
net.bitnine.agens.livy.app.RunCypherApp http://minmac:8998 modern \
test_query02 "match (a:person) return a.id_, a.name, a.age, a.country"
==>

 */
