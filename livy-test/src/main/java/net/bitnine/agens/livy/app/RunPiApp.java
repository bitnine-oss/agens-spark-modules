package net.bitnine.agens.livy.app;

import java.util.HashMap;
import java.util.Map;

import net.bitnine.agens.livy.RunAvroWriteJob;
import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.util.AgensLivyJobException;

public class RunPiApp {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RunPiApp <livyUrl> <slices>");
            System.exit(-1);
        }

        // options: livyUri, datasource, name, query
        Map<String,String> parameters = new HashMap<>();
        parameters.put("agens.spark.livy", args[0]);
        parameters.put("agens.spark.value", args[1]);

        RunAvroWriteJob job = new RunAvroWriteJob();
        try{
            String schemaJson = job.run(
                    parameters.get("agens.spark.livy"),
                    parameters.get("agens.spark.value")
            );
            // for DEBUG
            System.out.println("RunPiApp ==>\n" + schemaJson);
        }
        catch (AgensLivyJobException ex){
            System.err.println("Error: AgensLivyJobException\n"+ex.getMessage());
            System.exit(-1);
        }
    }
}

/*
java -cp target/agens-livy-test-1.0-dev.jar net.bitnine.agens.livytest.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */