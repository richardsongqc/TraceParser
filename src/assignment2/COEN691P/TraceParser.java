package assignment2.COEN691P;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import assignment2.COEN691P.mapreduce.TracerMapper;
import assignment2.COEN691P.mapreduce.TracerReducer;

public class TraceParser
{

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {
        if (args.length != 2)
        {
            System.err
                    .println("Usage: TraceParser <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(TraceParser.class);
        job.setJobName("Trace Parser");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TracerMapper.class);
        job.setReducerClass(TracerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
