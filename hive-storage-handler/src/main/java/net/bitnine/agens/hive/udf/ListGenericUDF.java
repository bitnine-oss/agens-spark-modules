package net.bitnine.agens.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//////////////////////////////////////////////////////
//
//  출처: https://118k.tistory.com/460
//

//  GenericUDF 를 사용해야 하는 이유
//
//  1. It can accept arguments of complex types, and return complex types.//
//  2. It can accept variable length of arguments.
//  3. It can accept an infinite number of function signature
//      - for example, it's easy to write a GenericUDF that accepts array, array> and so on (arbitrary levels of nesting).
//  4. It can do short-circuit evaluations using DeferedObject.
//
//

@Description(
        // List 의 문자열의 길이를 모두 더하여 출력
        name="sumListStringLength",
        value="_FUNC_(value) - Returns value that sum list string length.",
        extended="Example:\n  > SELECT _FUNC_(Array<String>) FROM table LIMIT 1;"
)
public class ListGenericUDF extends GenericUDF {

    ListObjectInspector listOi;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // initialize 함수에서는 다음과 같은 역활을 진행
        // - 입력받은 파라미터에 대한 검증
        // - 반환하는 파라미터에 대한 검증
        // - 함수에 입력받는 파라미터 개수 확인

        if(arguments.length != 1)
            throw new UDFArgumentLengthException("function argument need 1.");

        // 파라미터의 타입 확인
        ObjectInspector inspector = arguments[0];
        if( !(inspector instanceof ListObjectInspector) )
            throw new UDFArgumentException("function argument need List");

        listOi = (ListObjectInspector) inspector;

        // 입력받는 리스트내 엘리먼트의 객체 타입 확인
        if( !(listOi.getListElementObjectInspector() instanceof StringObjectInspector) )
            throw new UDFArgumentException("array argument need ");

        // 반환은 문자열의 수이므로 int 객체 반환
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // arguments의 객체를 형변환
        List<Text> list = (List<Text>) listOi.getList(arguments[0].get());

        if(list == null)
            return null;

        int sum = 0;
        for(Text txt : list) {
            sum += txt.getLength();
        }

        return new IntWritable(sum);
    }

    @Override
    public String getDisplayString(String[] children) {

        StringBuffer buffer = new StringBuffer();
        buffer.append("sumListStringLength(Array<String>), ");

        for(String child : children)
            buffer.append(child).append(",");

        return buffer.toString();
    }

}

/*
<사용방법>

ADD JAR hdfs://minmac:9000/user/agens/lib/agens-hive-storage-handler-1.0-dev.jar;

CREATE TEMPORARY FUNCTION sumListStringLength AS 'net.bitnine.agens.hive.udf.ListGenericUDF';

select sumListStringLength(array('1','22','333','4444')) as test;

 */