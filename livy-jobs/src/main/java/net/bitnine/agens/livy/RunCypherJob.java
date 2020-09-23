package net.bitnine.agens.livy;

import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

import net.bitnine.agens.livy.job.CypherJob;
import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.util.AgensLivyJobException;
import org.apache.livy.*;

public class RunCypherJob {

    public static String run(
            String livyUrl,         // ex) http://minmac:8998
            String datasource,      // ex) modern
            String name,            // person
            String query            // match (a:person) return a.id_, a.name, a.age, a.country
    ) throws AgensLivyJobException {
        // parameter: agens.spark.livy
        URI livyUri = AgensLivyHelper.convertURI(livyUrl);
        if( livyUri == null )
            throw new AgensLivyJobException("[Error] Wrong livy URI: "+livyUrl);

        // connect to livy server with livyUri
        LivyClient client;
        try {
            client = new LivyClientBuilder()
                    .setURI(livyUri)
                    // .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    // **NOTE: 간혹 Http ConnectionTimeout 발생하는 경우가 있다
                    .setConf("livy.rsc.server.connect.timeout","360s")
                    .setConf("livy.rsc.client.connect.timeout","120s")
                    .build();
        }
        catch (Exception ex){
            throw new AgensLivyJobException("[Fail] livyClient connect: "+ex.getMessage(), ex.getCause());
        }

        // result = schema of saved avro data
        String schemaJson = null;
        try {
            // parameters(3): agens.query.datasource, agens.query.name, agens.query.query
            schemaJson = client.submit(new CypherJob(datasource, name, query)).get();
        } catch (Exception ex){
            throw new AgensLivyJobException("[Fail] livyClient.submit: "+ex.getMessage(), ex.getCause());
        } finally {
            client.stop(true);
        }

        return schemaJson;
    }

    public static void main( String[] args ) throws AgensLivyJobException {
        System.out.println("net.bitnine.agens.livy.RunAvroWriteJob: " + Arrays.stream(args).collect(Collectors.joining()));
        if (args.length != 4) {
            System.err.println("Usage: RunCypherJob <livyUrl> <datasource> <name> <query>");
            System.exit(-1);
        }
        System.out.println("");

        System.out.println("RunCypherJob.. ");
        String result = run(args[0], args[1], args[2], args[3]);
        System.out.println("result ==>\n"+result);
    }
}

/*
java -cp target/agens-livy-jobs-1.0-dev.jar net.bitnine.agens.livy.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */