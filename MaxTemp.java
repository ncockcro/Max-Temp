package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemp {

  public static class MyMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private IntWritable temp = new IntWritable(); // Dont need to set to 1 because your not parsing a constant
    private Text month = new Text();
    private Text temp2 = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer it = new StringTokenizer(value.toString());

	month.set(it.nextToken());
	temp2.set(it.nextToken());
	 temp = new IntWritable(Integer.parseInt(temp2.toString()));


	context.write(month,temp);

    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int maxTemp = -500;
      for (IntWritable val : values) {
	if(val.get() > maxTemp) {
		maxTemp = val.get();
	}
      }
      result.set(maxTemp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "max month");
    job.setJarByClass(MaxTemp.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


