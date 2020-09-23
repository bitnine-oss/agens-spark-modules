package net.bitnine.agens.livy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

public final class AgensLivyHelper {

    public static URI convertURI(String param){
        try{
            return new URI(param);
        }
        catch (URISyntaxException ex){
            return null;
        }
    }

    public static boolean existsHdfsFile(String hdfsUrl){
        try{
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path(hdfsUrl);
            return fileSystem.exists(path);
        }
        catch (Exception ex){
            return false;
        }
    }

    // **NOTE: same function of AgensHelper.savePath
    public static String savePath(String tempPath, String name){
        return String.format("%s/%s.avro", tempPath, name);
    }
}
