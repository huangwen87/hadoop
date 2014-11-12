package com.algorithm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 数据去重
 * @author darwen
 * 2013-12-4  下午2:41:57
 */
public class Dedup{
	
	
	public static class Mapclass extends Mapper<Object, Text, Text, Text>{
		
		private Text line = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = new Text(value.toString().trim());
			context.write(line, new Text(""));
			
		}
	}
	
	
	
	public static class Reduceclass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "10.15.107.64:9001");
        String[] ioArgs= args;
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Data Deduplication <in> <out>");
            System.exit(2);
        }
        
        Job job = new Job(conf, "Data Deduplication");
        job.setJarByClass(Dedup.class);
        
        //设置Map、Combine和Reduce处理类
        job.setMapperClass(Mapclass.class);
        job.setCombinerClass(Reduceclass.class);
        job.setReducerClass(Reduceclass.class);
        
	    //设置输出类型
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //设置输入和输出目录
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
