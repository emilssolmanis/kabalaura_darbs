package com.esmupliks.pig.load;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

public class LoadMyTracksCSV extends LoadFunc {
    @Override
    public InputFormat getInputFormat() {
        return null;
    }

    @Override
    public void setLocation(String location, Job job) {
    }

    @Override
    public Tuple getNext() {
        return null;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
    }
}
