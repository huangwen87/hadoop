package com.algorithm;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * ƽ��ɼ�
 * @author darwen
 * 2013-12-4  ����4:46:50
 */
public class AvgScore {
	
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			System.out.println("line: "+ line);
			StringTokenizer data = new StringTokenizer(line, "\n");			
			while(data.hasMoreTokens()){
				StringTokenizer tokenline = new StringTokenizer(data.nextToken());
				String name = tokenline.nextToken();
				String score = tokenline.nextToken();
				context.write(new Text(name), new IntWritable(Integer.parseInt(score)));				
			}
					
		}
		
	}
	
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			int count = 0;
			for(IntWritable val : values){
				sum += val.get();
				count++;
			}
			double avgScore = (double)sum/count;
			context.write(key, new DoubleWritable(avgScore));
		}
		
	}
	
	


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
//        conf.set("mapred.job.tracker", "10.15.107.64:9001");
//        String[] ioArgs= args;
//        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: AvgScore <in> <out>");
//            System.exit(2);
//        }
        
        Job job = new Job(conf, "AvgScore");
        job.setJarByClass(AvgScore.class);
        
        //����Map��Combine��Reduce������
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        
	    //����reduce�������
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    //����map�������
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    
	    
	    //���ý��������ݼ��ָ��С��ݿ�
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    //������������Ŀ¼
//	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    FileInputFormat.addInputPath(job, new Path("hdfs://10.15.107.74/user/hadoop/avgScore_in/"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/avgScore_out/"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);     
	}

}
