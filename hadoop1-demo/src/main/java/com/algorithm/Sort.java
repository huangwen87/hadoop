package com.algorithm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 数据排序   利用reduce默认的排序
 * @author darwen
 * 2013-12-4  下午3:29:10
 */
public class Sort {
	
	
	public static class MapClass extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		private IntWritable data = new IntWritable();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
		
	}
	
	public static class ReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

	    private static IntWritable linenum = new IntWritable(1);
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			for(IntWritable val : values){   //一个key 可能存在多个值
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get() + 1);
			}
			
		}
		
	}
	
	
	


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "10.15.107.64:9001");
        String[] ioArgs= args;
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Data Sort <in> <out>");
            System.exit(2);
        }
        
        Job job = new Job(conf, "Data Sort");
        job.setJarByClass(Sort.class);
        
        //设置Map、Combine和Reduce处理类
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        
	    //设置输出类型
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    //设置输入和输出目录
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
