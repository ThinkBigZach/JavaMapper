package com.udf.input;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;




public class ReplaceChars extends UDF {
    private Text result = new Text();
    public Text evaluate(String str2) {
        String rep = str2.replaceAll("\n", "");
        rep = rep.replaceAll("\r", "");
        rep = rep.replaceAll("\036", "");
        result.set(rep);
        return result;
    }

}
