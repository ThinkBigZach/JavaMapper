package com.chs.symlink;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

//import au.com.bytecode.opencsv.CSVReader;
//import au.com.bytecode.opencsv.CSVWriter;


/**
 * CSVSerde uses opencsv (http://opencsv.sourceforge.net/) to serialize/deserialize columns as CSV.
 *
 * @author Larry Ogrodnek <ogrodnek@gmail.com>
 */
public final class SymlinkSerde implements SerDe {

    private ObjectInspector inspector;
    private String[] outputFields;
    private int numCols;
    private List<String> row;
    private char separatorChar;
    private char quoteChar;
    private char escapeChar;


    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
        final List<String> columnNames = Arrays.asList(tbl.getProperty(Constants.LIST_COLUMNS).split(","));
        final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(Constants.LIST_COLUMN_TYPES));

        numCols = columnNames.size();

        final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);

        for (int i=0; i< numCols; i++) {
            columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
        this.outputFields = new String[numCols];
        row = new ArrayList<String>(numCols);

        for (int i=0; i< numCols; i++) {
            row.add(null);
        }

    }

    private final char getProperty(final Properties tbl, final String property, final char def) {
        final String val = tbl.getProperty(property);

        if (val != null) {
            return val.charAt(0);
        }

        return def;
    }

    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();
        final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(0));
        final ObjectInspector fieldOI = outputFieldRefs.get(0).getFieldObjectInspector();
        final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
        String s = fieldStringOI.getPrimitiveWritableObject(field).toString();


//        // Get all data out.
//        for (int c = 0; c < numCols; c++) {
//            final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
//            final ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();
//
//            // The data must be of type String
//            final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
//
//            // Convert the field to Java class String, because objects of String type
//            // can be stored in String, Text, or some other classes.
//            outputFields[c] = fieldStringOI.getPrimitiveWritableObject(field).toString();
//        }

        final StringWriter writer = new StringWriter();
        writer.write(s);
            String rep = s.replaceAll("\n", "");
            rep = rep.replaceAll("\r", "");
            return new Text(rep);

    }

    public Object deserialize(final Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        return rowText.toString();
    }




    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}
