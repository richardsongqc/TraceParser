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

    @Override
    public void reduce(Text arg0, Iterator<DoubleWritable> arg1,
            OutputCollector<Text, DoubleWritable> arg2, Reporter arg3)
            throws IOException
    {
        Double sum = (double) 0;
        while (arg1.hasNext()) 
        {
            Double item = arg1.next().get();
            sum += item;
            listTraces.add(item);
        }
        
        Collections.sort(listTraces);
        int size = listTraces.size();
        
        System.out.println("size of listTrace = " + size);
        
        arg2.collect(new Text("min"), new DoubleWritable(listTraces.get(0)));
        arg2.collect(new Text("max"), new DoubleWritable(listTraces.get(size-1)));
        arg2.collect(new Text("avg"), new DoubleWritable(sum/size));
        
    }

}
