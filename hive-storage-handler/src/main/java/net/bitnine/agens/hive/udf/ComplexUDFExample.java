package net.bitnine.agens.hive.udf;

// https://blog.matthewrathbone.com/2013/08/10/guide-to-writing-hive-udfs.html

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.List;

public class ComplexUDFExample extends GenericUDF {

    ListObjectInspector listOI;
    StringObjectInspector elementOI;

    @Override
    public String getDisplayString(String[] arg0) {
        return "arrayContainsUDF()"; // this should probably be better
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
        }

        // 1. Check we received the right object types.
        ObjectInspector a = arguments[0];
        ObjectInspector b = arguments[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }

        this.listOI = (ListObjectInspector) a;
        this.elementOI = (StringObjectInspector) b;

        // 2. Check that the list contains strings
        if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of strings");
        }

        // the return type of our function is a boolean, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // ==> FAILED:
        // ClassCastException org.apache.hadoop.io.Text cannot be cast to java.lang.String
        // Object : org.apache.hadoop.io.Text

        // get the list and string from the deferred objects using the object inspectors
        List<Object> list = (List<Object>) this.listOI.getList(arguments[0].get());
        String arg = elementOI.getPrimitiveJavaObject(arguments[1].get());

        // check for nulls
        if (list == null || arg == null) {
            return null;
        }

//        System.err.println("evaluate:0) compare "+arg+" to "+ list +" ==> ");
//        System.err.println("evaluate:0) type "+arg.getClass().getCanonicalName());
//        System.err.println("evaluate:0) type "+list.get(0).getClass().getCanonicalName());

        // see if our list contains the value we need
        for(Object s: list) {
//            System.err.println("evaluate:1) compare "+arg+" to "+ s.toString() +" = "+arg.equals(s.toString()));
            if( arg.equals(s.toString()) ) return new Boolean(true);
        }

        return new Boolean(false);
    }

}

/*
** Usage:

ADD JAR hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar;

CREATE TEMPORARY FUNCTION arrayContainsUDF AS 'net.bitnine.agens.hive.udf.ComplexUDFExample';

select arrayContainsUDF(array('a', 'b', 'c'), 'b') as test;

 */