package com.chs.symlink;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zw186016 on 9/8/15.
 */
public class SymlinkTextInputSplit extends FileSplit {
    private final FileSplit split;

    public SymlinkTextInputSplit() {
        super((Path)null, 0, 0, (String[])null);
        split = new FileSplit((Path)null, 0, 0, (String[])null);
    }

    public SymlinkTextInputSplit(Path symlinkPath, FileSplit split) throws IOException {
        super(symlinkPath, 0, 0, split.getLocations());
        this.split = split;
    }

    /**
     * Gets the target split, i.e. the split of target data.
     */
    public FileSplit getTargetSplit() {
        return split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        split.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        split.readFields(in);
    }
}