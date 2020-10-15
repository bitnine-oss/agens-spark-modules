package net.bitnine.agens.hive.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * http://www.congiu.com/structured-data-in-hive-a-generic-udf-to-sort-arrays-of-structs/
 * @author rcongiu
 */
@Description(name = "array_struct_sort",
        value = "_FUNC_(array(struct1,struct2,...), string myfield) - "
                + "returns the passed array struct, ordered by the given field  " ,
        extended = "Example:\n"
                + "  > SELECT _FUNC_(str, 'myfield') FROM src LIMIT 1;\n"
                + " 'b' ")
public class ArrayStructSortUDF extends GenericUDF {

    protected ObjectInspector[] argumentOIs;

    ListObjectInspector loi;
    StructObjectInspector elOi;

    // cache comparators for performance
    Map<String,Comparator> comparatorCache = new HashMap<String,Comparator>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
        // all common initialization
        argumentOIs = ois;

        // clear comparator cache from previous invokations
        comparatorCache.clear();

        return checkAndReadObjectInspectors(ois);
    }

    /**
     * Utility method to check that an object inspector is of the correct type,
     * and returns its element object inspector
     */
    protected ListObjectInspector checkAndReadObjectInspectors(ObjectInspector[] ois)
            throws UDFArgumentTypeException, UDFArgumentException {

        // check number of arguments. We only accept two,
        // the list of struct to sort and the name of the struct field
        // to sort by
        if(ois.length != 2 ) {
            throw new UDFArgumentException("2 arguments needed, found " + ois.length );
        }

        // first argument must be a list/array
        if (! ois[0].getCategory().equals(LIST)) {
            throw new UDFArgumentTypeException(0, "Argument 1"
                    + " of function " + this.getClass().getCanonicalName() + " must be " + Constants.LIST_TYPE_NAME
                    + ", but " + ois[0].getTypeName()
                    + " was found.");
        }

        // a list/array is read by a LIST object inspector
        loi = (ListObjectInspector) ois[0];

        // a list has an element type associated to it
        // elements must be structs for this UDF
        if( loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) {
            throw new UDFArgumentTypeException(0, "Argument 1"
                    + " of function " +  this.getClass().getCanonicalName() + " must be an array of structs " +
                    " but is an array of " + loi.getListElementObjectInspector().getCategory().name());
        }

        // store the object inspector for the elements
        elOi = (StructObjectInspector)loi.getListElementObjectInspector();

        // returns the same object inspector
        return  loi;
    }

    // to sort a list , we must supply our comparator
    public class StructFieldComparator implements Comparator {

        StructField field;

        public StructFieldComparator(String fieldName) {
            field = elOi.getStructFieldRef(fieldName);
        }

        public int compare(Object o1, Object o2) {

            // ok..so both not null
            Object f1 = elOi.getStructFieldData(o1, field);
            Object f2 = elOi.getStructFieldData(o2, field);

            // compare using hive's utility functions
            return ObjectInspectorUtils.compare(f1, field.getFieldObjectInspector(),
                    f2, field.getFieldObjectInspector());
        }
    }

    // factory method for cached comparators
    Comparator getComparator(String field) {

        if(!comparatorCache.containsKey(field)) {
            comparatorCache.put(field, new StructFieldComparator(field));
        }

        return comparatorCache.get(field);
    }

    @Override
    public Object evaluate(DeferredObject[] dos) throws HiveException {
        // get list
        if(dos==null || dos.length != 2) {
            throw new HiveException("received " + (dos == null? "null" :
                    Integer.toString(dos.length) + " elements instead of 2"));
        }

        // each object is supposed to be a struct
        // we make a shallow copy of the list. We don't want to sort
        // the list in place since the object could be used elsewhere in the
        // hive query
        ArrayList al = new ArrayList(loi.getList(dos[0].get()));

        // sort with our comparator, then return
        // note that we could get a different field to sort by for every
        // invocation
        String fieldName = dos[1].get().toString();
        Collections.sort(al, getComparator( fieldName ) );

        return al;
    }

    @Override
    public String getDisplayString(String[] children) {
        return  (children == null? null : this.getClass().getCanonicalName() + "(" + children[0] + "," + children[1] + ")");
    }

}

/*
** Usage:

ADD JAR hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar;

CREATE TEMPORARY FUNCTION array_struct_sort AS 'net.bitnine.agens.hive.udf.ArrayStructSortUDF';

select array_struct_sort(array('a', 'b', 'c'), 'b') as test;

 */
