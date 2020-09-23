package net.bitnine.agens.livy.app;

import net.bitnine.agens.livy.RunAvroWriteJob;
import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.util.AgensLivyJobException;

import java.util.HashMap;
import java.util.Map;

public class RunAvroWriteApp {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RunAvroWriteApp <livyUrl> <name>");
            System.exit(-1);
        }

        // options: livyUri, datasource, name, query
        Map<String,String> parameters = new HashMap<>();
        parameters.put("agens.spark.livy", args[0]);
        parameters.put("agens.spark.name", args[1]);

        RunAvroWriteJob job = new RunAvroWriteJob();
        try{
            String schemaJson = job.run(
                    parameters.get("agens.spark.livy"),
                    parameters.get("agens.spark.name")
            );
            // for DEBUG
            System.out.println("RunAvroWriteApp ==>\n" + schemaJson);
        }
        catch (AgensLivyJobException ex){
            System.err.println("Error: AgensLivyJobException\n"+ex.getMessage());
            System.exit(-1);
        }
    }
}

/*
java -cp target/agens-livy-test-1.0-dev.jar \
net.bitnine.agens.livy.app.RunAvroWriteApp http://minmac:8998 person
-------------------
RunAvroWriteApp ==>
{
  "type" : "record",
  "name" : "avro_person",
  "namespace" : "net.bitnine.agens.hive",
  "fields" : [ {
    "name" : "firstname",
    "type" : [ "string", "null" ]
  }, {
    "name" : "middlename",
    "type" : [ "string", "null" ]
  }, {
    "name" : "lastname",
    "type" : [ "string", "null" ]
  }, {
    "name" : "dob_year",
    "type" : "int"
  }, {
    "name" : "dob_month",
    "type" : "int"
  }, {
    "name" : "gender",
    "type" : [ "string", "null" ]
  }, {
    "name" : "salary",
    "type" : "int"
  } ]
}
 */
