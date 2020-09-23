package net.bitnine.agens.spark;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Map;


public final class AgensJavaHelper {

    public String hello(String msg){
        if(msg == null) return null;
        return String.format("Hello, '%s' - AgensSparkConnector", msg);
    }

    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<A, B>>conforms()
        );
    }

}
