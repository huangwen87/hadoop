package com.gw.mymain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gw.common.mapred.MultiOutputFormat;
import com.gw.doubleleven.LogParseMapper;

public class DoubleElevebLogParse {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
       
		System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.2.0");
		
		
        Job job = Job.getInstance(conf, "DoubleElevebLogParse");
        job.setJarByClass(DoubleElevebLogParse.class);
        job.setMapperClass(LogParseMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);
        
        String inPath = "hdfs://10.15.43.7:9000/test";
        String outPath = "/myout/";
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
