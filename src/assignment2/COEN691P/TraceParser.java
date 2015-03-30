package assignment2.COEN691P;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.util.*;

import assignment2.COEN691P.mapreduce.TracerMapper;
import assignment2.COEN691P.mapreduce.TracerReducer;

public class TraceParser
{

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {
        if (args.length != 2)
        {
            System.err.println("Usage: TraceParser <options> <input path> <output path>");
            System.err.println("\t<usages>:");              
            System.err.println("\t\t-cores\t\t CPU usage");
            System.err.println("\t\t-memory\t\tMemory usage");
            System.err.println("\t<input path>:");
            System.err.println("\t<output path>:");
            return;
        }
        
        //Job job = new Job();
        //job.setJarByClass(TraceParser.class);
        //job.setJobName("Trace Parser");
        //
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //
        //job.setMapperClass(TracerMapper.class);
        //job.setReducerClass(TracerReducer.class);
        //
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        //
        //job.waitForCompletion(true);
        
        JobConf conf = new JobConf(TraceParser.class);
        conf.setJobName("TraceParser");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(TracerMapper.class);
        //conf.setCombinerClass(Combine.class);
        conf.setReducerClass(TracerReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf); // blocking call
        
        
    }
}
