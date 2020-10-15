package net.bitnine.agens.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

/**
 * Created by ljw on 2019-12-17
 * Description:
 * https://programmer.group/udf-writing-for-struct-complex-data-type-and-genericudf-writing.html
 */
@SuppressWarnings("Duplicates")
public class CompareMaxStructUDF extends GenericUDF {

    StructObjectInspector soi1;
    StructObjectInspector soi2;

    /**
     * Avoid frequent Struct objects
     */
    private List<? extends StructField> allStructFieldRefs;

    //1. This method is called only once and before the evaluate() method.The parameter accepted by this method is an arguments array.This method checks to accept the correct parameter type and number.
    //2. Definition of output type
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        String error = "";

        //Verify that the number of parameters is correct
        if (arguments.length != 2) {
            throw new UDFArgumentException("Two parameters are required");
        }

        //Determine whether the parameter type is correct - struct
        ObjectInspector.Category arg1 = arguments[0].getCategory();
        ObjectInspector.Category arg2 = arguments[1].getCategory();

        if (!(arg1.equals(ObjectInspector.Category.STRUCT))) {
            error += arguments[0].getClass().getSimpleName();
            throw new UDFArgumentTypeException(0, "\"array\" expected at function STRUCT_CONTAINS, but \"" +
                    arg1.name() + "\" " + "is found" + "\n" + error);
        }

        if (!(arg2.equals(ObjectInspector.Category.STRUCT))) {
            error += arguments[1].getClass().getSimpleName();
            throw new UDFArgumentTypeException(0, "\"array\" expected at function STRUCT_CONTAINS, but \""
                    + arg2.name() + "\" " + "is found" + "\n" + error);
        }

        //Output structure definition
        ArrayList<String> structFieldNames = new ArrayList();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList();
        soi1 = (StructObjectInspector) arguments[0];
        soi2 = (StructObjectInspector) arguments[1];

        StructObjectInspector toValid = null;
        if (soi1 == null)
            toValid = soi2;
        else toValid = soi1;

        //Setting the return type
        allStructFieldRefs = toValid.getAllStructFieldRefs();
        for (StructField structField : allStructFieldRefs) {
            structFieldNames.add(structField.getFieldName());
            structFieldObjectInspectors.add(structField.getFieldObjectInspector());
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                structFieldNames, structFieldObjectInspectors
        );
    }

    // This method is similar to the evaluate() method of UDF.
    // It handles the real parameters and returns the final result.
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        // Convert the struct type from hive to com.aliyun.odps.data.Struct.
        // If there are errors, debug to see what the data from deferredObjects looks like
        // Then re-encapsulate it yourself!!!

        ArrayList list1 = (ArrayList) deferredObjects[0].get();
        ArrayList list2 = (ArrayList) deferredObjects[1].get();

        int len = list1.size();

        ArrayList fieldNames = new ArrayList<>();
        ArrayList fieldValues = new ArrayList<>();

        for (int i = 0; i < len ; i++) {
            if (!list1.get(i).equals(list2.get(i))) {
                fieldNames.add(allStructFieldRefs.get(i).getFieldName());
                fieldValues.add(list2.get(i));
            }
        }

        if (fieldValues.size() == 0) return null;

        return fieldValues;
    }

    // This method is used to print out prompts when an implementation of GenericUDF fails.
    // The hint is the string you returned at the end of the implementation.
    @Override
    public String getDisplayString(String[] strings) {
        return "Usage:" + this.getClass().getName() + "(" + strings[0] + ")";
    }

}