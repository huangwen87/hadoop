package com.algorithm;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.util.AllFileSplitInputFormat;

/**
 * ����һ�����⣺��һ���ļ�����̫���³���hdfs���С  ��������Combiner�׶�ͳ��
 * һ���ļ��� ĳ�����ʳ��ֵĴ�����������������������һ���ļ���hdfs�����Ʒ�Ϊ���map������
 * ��reduce�׶��ֲ����ڽ��л���
 * 
 * 
 * ��������
 * @author darwen
 * 2013-12-6  ����10:24:39
 */
public class InvertedIndex {


	
	public static class MapClass extends Mapper<Object, Text, Text, Text>{
		
		private FileSplit file;
		private Text keyinfo = new Text();
		private Text valueinfo = new Text();


		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				
			file = (FileSplit)context.getInputSplit();
			//file.getPath().toString()---> hdfs://10.15.107.64/user/hadoop/index_in/file*.txt
			int splitIndex = file.getPath().toString().indexOf("file");
			StringTokenizer token = new StringTokenizer(value.toString());
			while(token.hasMoreTokens()){
				keyinfo.set(token.nextToken()+":"+file.getPath().toString().substring(splitIndex));
				valueinfo.set("1");
				context.write(keyinfo, valueinfo);
			}
		}
		
	}
	
	
	public static class Combiner extends Reducer<Text, Text, Text, Text>{
		
		private Text info = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
               
			int sum = 0;
			for(Text value : values){
				sum += Integer.parseInt(value.toString());
			}
			
			int splitIndex = key.toString().indexOf(":");
			info.set(key.toString().substring(splitIndex+1)+":"+sum);
			key.set(key.toString().substring(0, splitIndex));
			context.write(key, info);
			
		}
		
	}
	
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		
		private Text info = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String buffer = new String();			
			for(Text value : values){
				buffer += value.toString()+";";
			}
			info.set(buffer);
			context.write(key, info);
			
		}
		
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "10.15.107.63:9001");
	    conf.set("mapred.map.tasks", "15");
        Job job = new Job(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        // ����Map��combiner��Reduce������
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(ReduceClass.class);
        
        //�����޸�---�ļ����ֿ�
        job.setInputFormatClass(AllFileSplitInputFormat.class);
        
        
        // �����������
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ������������Ŀ¼
        String inPath = "hdfs://10.15.107.63:8020/user/hadoop/index_in/";
        String outPath = "/user/hadoop/index_out/";
        
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
