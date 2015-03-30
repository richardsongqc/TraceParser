package assignment2.COEN691P.mapreduce;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TracerMapper 
    extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, DoubleWritable>
{
    
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws java.io.IOException
    {
        String[] line = value.toString().split(" ");
        
        //Long Time = Long.parseLong(line[0]);
        //Long JobID = Long.parseLong(line[1]);
        //Long TaskID = Long.parseLong(line[2]);
        //int JobType = Integer.parseInt(line[3]);
        //Double NrmlTaskCores = Double.parseDouble(line[4]);
        Double NrmlTaskMem = Double.parseDouble(line[5]);
        output.collect( new Text("value"), new DoubleWritable(NrmlTaskMem));
    }
    
    //        protected void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output)
    //        throws java.io.IOException, InterruptedException
    //{
    //    String[] line = value.toString().split(" ");
    //    
    //    Long Time = Long.parseLong(line[0]);
    //    Long JobID = Long.parseLong(line[1]);
    //    Long TaskID = Long.parseLong(line[2]);
    //    int JobType = Integer.parseInt(line[3]);
    //    Double NrmlTaskCores = Double.parseDouble(line[4]);
    //    Double NrmlTaskMem = Double.parseDouble(line[5]);
    //    
    //    context.write(JobID, tempWritable);
    //}

}
