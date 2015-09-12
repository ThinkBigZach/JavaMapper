package com.symlink.input;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

/**
 * Created by zw186016 on 9/4/15.
 */
public class NewInputFormat extends TextInputFormat implements JobConfigurable{
    private CompressionCodecFactory compressionCodecs = null;
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit)genericSplit;
        LineRecordReader m = new LineRecordReader(job, fileSplit, "\036\n".getBytes(Charsets.UTF_8));
        return m;
    }
}
