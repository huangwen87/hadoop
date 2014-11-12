package com.algorithm;

import java.io.IOException;
import java.util.StringTokenizer;

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

import com.util.MultipleOutputFormat;

/**
 * 单词统计源码
 * @author darwen
 * 2013-12-4  下午2:41:36
 */
public class WordCount {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
		        throws IOException, InterruptedException {
		    String line = value.toString();
		    StringTokenizer itr = new StringTokenizer(line);
		    while (itr.hasMoreTokens()) {
		        word.set(itr.nextToken().toLowerCase());
		        context.write(word, one);
             }
	   }
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values,
       Context context) throws IOException, InterruptedException {
	   int sum = 0;
	   for (IntWritable val : values) {
	        sum += val.get();
	   }
	    context.write(key, new IntWritable(sum));
      }
    }
	
    public static class AlphabetOutputFormat extends MultipleOutputFormat<Text, IntWritable> {  
        @Override  
        protected String generateFileNameForKeyValue(Text key, IntWritable value, Configuration conf) {  
            char c = key.toString().toLowerCase().charAt(0);  
            if (c >= 'a' && c <= 'z') {  
                return c + ".txt";  
            }  
            return "other.txt";  
        }  
    } 
	
	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "10.15.107.74:9001");
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(AlphabetOutputFormat.class);//设置输出格式 
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
