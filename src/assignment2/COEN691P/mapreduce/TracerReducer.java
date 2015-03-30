package assignment2.COEN691P.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TracerReducer 
    extends MapReduceBase 
    implements Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    ArrayList<Double> listTraces = new ArrayList<Double>();

    public void reduce(
            Text key, 
            Iterator<DoubleWritable> values, 
            OutputCollector<Text, DoubleWritable> output, 
            Reporter reporter) 
            throws IOException
    {
        double sum = 0;
        while (values.hasNext()) 
        {
            sum += values.next().get();
            listTraces.add(values.next().get());
        }
        
        Collections.sort(listTraces);
        int size = listTraces.size();

        output.collect(new Text("min"), new DoubleWritable(listTraces.get(0)));
        output.collect(new Text("max"), new DoubleWritable(listTraces.get(size-1)));
        output.collect(new Text("avg"), new DoubleWritable(sum/size));
    }

}
