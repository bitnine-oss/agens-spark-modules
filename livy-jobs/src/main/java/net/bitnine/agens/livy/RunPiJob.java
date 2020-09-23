package net.bitnine.agens.livy;

import net.bitnine.agens.livy.util.AgensLivyHelper;
import net.bitnine.agens.livy.job.PiJob;
import net.bitnine.agens.livy.util.AgensLivyJobException;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.spark.launcher.SparkLauncher;

import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RunPiJob {

    public static Double run(
            String livyUrl,         // ex) http://minmac:8998
            String intVal           // ex) "2"
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
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, "1G")
                    .setConf("livy.rsc.server.connect.timeout","360s")
                    .setConf("livy.rsc.client.connect.timeout","120s")
                    .build();
        }
        catch (Exception ex){
            throw new AgensLivyJobException("Fail: livyClient connect", ex.getCause());
        }

        // result = schema of saved avro data
        Double result = 0.0d;
        try {
            // parameters: slices
            Integer slices = Integer.parseInt(intVal);
            result = client.submit(new PiJob(slices)).get();
        } catch (Exception ex){
            throw new AgensLivyJobException("Fail: livyClient.submit", ex.getCause());
        } finally {
            client.stop(true);
        }

        return result;
    }

    public static void main( String[] args ) throws AgensLivyJobException {
        System.out.println("net.bitnine.agens.livy.RunPiJob: " + Arrays.stream(args).collect(Collectors.joining()));
        if (args.length != 2) {
            System.err.println("Usage: RunAvroWriteJob <livyUrl> <slices>");
            System.exit(-1);
        }
        System.out.println("");

        Double result = run(args[0], args[1]);
        System.out.println("result ==>\n"+result);
    }
}

/*
java -cp target/agens-livy-jobs-1.0-dev.jar net.bitnine.agens.livy.PiApp http://minmac:8998 2
==>
Uploading livy-example jar to the SparkContext...
Pi is roughly 3.14074
 */