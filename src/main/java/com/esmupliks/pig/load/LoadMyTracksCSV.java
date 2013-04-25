package com.esmupliks.pig.load;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LoadMyTracksCSV extends LoadFunc {
    private final TupleFactory tupleFactory = TupleFactory.getInstance();
    private RecordReader reader;
    private Path path;

    @Override
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }
            Text lineText = (Text) reader.getCurrentValue();
            String line = lineText.toString();
            String[] parts = line.split(",");

            Tuple t = tupleFactory.newTuple();
// valid records are of the form
// "Seg","Pnt","Lat (deg)","Lon (deg)","Alt (m)","Bear (deg)","Acc (m)","Speed (m/s)","Time"
// "1",  "1",  "56.943113","24.12330", "35.0",   "0.0",       "48",     "0",          "blabla"
// "1",  "1",  "56.954902","24.10074", "33.0",   "0.0",       "50",     "0",          "blabla"
            if (parts.length == 12 || parts.length == 13) {
                // strip quotes
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = parts[i].substring(1, parts[i].length() - 1);
                }
                try {
                    Long seg = Long.parseLong(parts[0]);
                    Long pnt = Long.parseLong(parts[1]);
                    Double lat = Double.parseDouble(parts[2]);
                    Double lon = Double.parseDouble(parts[3]);
                    Double alt = Double.parseDouble(parts[4]);
                    Double bearing = Double.parseDouble(parts[5]);
                    Double accuracy = Double.parseDouble(parts[6]);
                    Double speed = Double.parseDouble(parts[7]);
                    String time = parts[8];
                    t.append(seg);
                    t.append(pnt);
                    t.append(lat);
                    t.append(lon);
                    t.append(alt);
                    t.append(bearing);
                    t.append(accuracy);
                    t.append(speed);
                    t.append(time);
                    t.append(path.toString());
                    return t;
                } catch (NumberFormatException ex) {
                    return t;
                }
            } else {
                return t;
            }
        } catch (InterruptedException ex) {
            return null;
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.path = ((FileSplit) split.getWrappedSplit()).getPath();
        this.reader = reader;
    }
}
