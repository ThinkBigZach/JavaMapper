package com.serde;

import org.apache.hadoop.hive.serde2.*;

import java.sql.Timestamp;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Map;
        import java.util.Properties;
        import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
        import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
        import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
        import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
        import org.apache.commons.logging.Log;
        import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
        import org.apache.hadoop.io.Text;
  import javax.print.attribute.standard.DateTimeAtCompleted;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hive.serde.Constants;
        import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * Properties:
 * ignore.malformed.json = true/false : malformed json will be ignored
 *         instead of throwing an exception
 *
 * @author rcongiu
 */
public class SymlinkSerde2 extends AbstractSerDe {

    int deserializedDataSize;
    int serializedDataSize;
    private List<Object> row = new ArrayList<Object>();
    ObjectInspector rowOI;
    public void initialize(Configuration configuration, Properties tbl) throws SerDeException {


          // Get column names and sort order
        final List<String> columnNames = Arrays.asList(tbl.getProperty(Constants.LIST_COLUMNS).split(","));
        final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(Constants.LIST_COLUMN_TYPES));
        final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(1);

        for (int i=0; i< 1; i++) {
            columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        TypeInfo rowTypeInfo =
                (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        rowOI =
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    public Writable serialize(Object o, ObjectInspector objInspector) throws SerDeException {
        String w = o.toString();
        w = w.replaceAll("\n", "").replaceAll("\r", "");
        Text t = new Text(w);
        serializedDataSize = t.getBytes().length;
        return t;
    }

    public Object deserialize(Writable writable) throws SerDeException {
//        Text rowText = (Text) writable;
        Text writa = (Text)writable;
//         deserializedDataSize = rowText.getBytes().length;
//        Object jObj = rowText.toString().replaceAll("\n", "").replaceAll("\r", "");
        row.clear();

//        Object s = writable.

        String s = writa.toString().replaceAll("\n", "").replaceAll("\r", "");

        row.add(s);
        return row;

    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}
