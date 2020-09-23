package net.bitnine.agens.livy;

import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.job.AvroWriteJob;
import net.bitnine.agens.livy.util.AgensLivyJobException;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RunAvroWriteJob {

    public static String run(
            String livyUrl,         // ex) http://minmac:8998
            String name             // person
    ) throws AgensLivyJobException {
        // parameter: agens.query.livy
        URI livyUri = AgensLivyHelper.convertURI(livyUrl);
        if( livyUri == null )
            throw new AgensLivyJobException("Wrong livy URI: "+livyUrl);

        // connect to livy server with livyUri
        LivyClient client;
        try {
            client = new LivyClientBuilder()
                    .setURI(livyUri)
                    // .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    .setConf("livy.rsc.server.connect.timeout","360s")
                    .setConf("livy.rsc.client.connect.timeout","120s")
                    .build();
        }
        catch (Exception ex){
            throw new AgensLivyJobException("Fail: livyClient connect", ex.getCause());
        }

        // result = schema of saved avro data
        String schemaJson = null;
        try {
            // parameters(3): agens.query.datasource, agens.query.name, agens.query.query
            schemaJson = client.submit(new AvroWriteJob(name)).get();
        } catch (Exception ex){
            throw new AgensLivyJobException("Fail: livyClient.submit", ex.getCause());
        } finally {
            client.stop(true);
        }

        return schemaJson;
    }

    public static void main( String[] args ) throws AgensLivyJobException {
        System.out.println("net.bitnine.agens.livy.RunAvroWriteJob: " + Arrays.stream(args).collect(Collectors.joining()));
        if (args.length != 2) {
            System.err.println("Usage: RunAvroWriteJob <livyUrl> <name>");
            System.exit(-1);
        }
        System.out.println("");

        String result = run(args[0], args[1]);
        System.out.println("result ==>\n"+result);
    }

}

/*
java -cp target/agens-livy-test-1.0-dev.jar \
net.bitnine.agens.livy.app.RunAvroWriteApp http://minmac:8998 person
==>
Uploading livy-jobs jar to the SparkContext...

 */