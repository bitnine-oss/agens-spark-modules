package net.bitnine.agens.hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

// https://stackoverflow.com/questions/15694555/a-genericudf-function-to-extract-a-field-from-an-array-of-structs

@Description(
        name = "agens_property_get",
        value = "_FUNC_( array< struct<productcategory:string> > ) - Collect all property field values inside an array of struct(s), and return the results in an array<string>",
        extended = "Example:\n SELECT _FUNC_(array_of_structs_with_properties_field)"
)
public class GetPropertyUDF extends GenericUDF {

    private ArrayList ret;

    private ListObjectInspector listOI;
    private StructObjectInspector structOI;
    private ObjectInspector prodCatOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        if (args.length != 1) {
            throw new UDFArgumentLengthException("The function extract_product_category() requires exactly one argument.");
        }

        if (args[0].getCategory() != Category.LIST) {
            throw new UDFArgumentTypeException(0, "Type array<struct> is expected to be the argument for extract_product_category but " + args[0].getTypeName() + " is found instead");
        }

        listOI = ((ListObjectInspector) args[0]);
        structOI = ((StructObjectInspector) listOI.getListElementObjectInspector());

        // key, type, value
        if (structOI.getAllStructFieldRefs().size() != 3) {
            throw new UDFArgumentTypeException(0, "Incorrect number of fields in the struct, should be three");
        }

        StructField productCategoryField = structOI.getStructFieldRef("key");
        // If not, throw exception
        if (productCategoryField == null) {
            throw new UDFArgumentTypeException(0, "NO \"key\" field in input structure");
        }

        //Are they of the correct types?
        //We store these object inspectors for use in the evaluate() method
        prodCatOI = productCategoryField.getFieldObjectInspector();

        //First are they primitives
        if (prodCatOI.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "productCategory field must be of string type");
        }

        //Are they of the correct primitives?
        if (((PrimitiveObjectInspector)prodCatOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "productCategory field must be of string type");
        }

        ret = new ArrayList();

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public ArrayList evaluate(DeferredObject[] arguments) throws HiveException {

        ret.clear();

        if (arguments.length != 1) {
            return null;
        }

        if (arguments[0].get() == null) {
            return null;
        }

        int numElements = listOI.getListLength(arguments[0].get());

        for (int i = 0; i < numElements; i++) {
            LazyString prodCatDataObject = (LazyString) (structOI.getStructFieldData(listOI.getListElement(arguments[0].get(), i), structOI.getStructFieldRef("productCategory")));
            Text productCategoryValue = ((StringObjectInspector) prodCatOI).getPrimitiveWritableObject(prodCatDataObject);
            ret.add(productCategoryValue);
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] strings) {

        assert( strings.length > 0 );

        StringBuilder sb = new StringBuilder();
        sb.append("agens_property_get(");
        sb.append(strings[0]);
        sb.append(")");

        return sb.toString();
    }
}