package com.hbase.mixexample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *  把hdfs中的数据根据某种正则表达式抽取相关数据并导入到Hbase
 *  通过在main函数TableOutputFormat.OUTPUT_TABLE中指定输出表
 *  在setup函数中 对family、qualifier定义，直接写入bytes数据
 *  来源：http://blog.csdn.net/wisgood/article/details/17576387
 *  @author darwen
 *  2013-12-30  上午10:09:30
 */
public class MapHdfsDataToHbase {

	
	public enum Counters{
		LINES
	}
	
	
	public static class MapperClass extends 
	               Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>{
		
		private byte[] family = null;
		private byte[] qualifier = null;
		
		
        
		/** 
		 * 描述：获取hbase 某个具体表的某列所有qualifier(qualifier存在的情况)  
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
				family    = Bytes.toBytes("addColumn");
			    qualifier = Bytes.toBytes("realtime");

		}

		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
		    try {		    	
		    	String[] strArr = value.toString().split("\t");
		    	Put put = new Put(Bytes.toBytes(key + ""));
		    	put.add(family, qualifier, Bytes.toBytes(strArr[strArr.length-1])); //最后一个时间
		    	ImmutableBytesWritable keyinfo = new 
		    			 ImmutableBytesWritable(Bytes.toBytes(strArr[1]));
		    	System.out.println("keyinfo: " + keyinfo);
		    	context.write(keyinfo, put);
		    	context.getCounter(Counters.LINES).increment(1);

			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		
		Job job = new Job(conf, "MapHdfsDataToHbase");
		job.setJarByClass(MapHdfsDataToHbase.class);
		job.setMapperClass(MapperClass.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "cf");
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);
		
		String path = "hdfs://linux:8020/user/hadoop/hdfs2hbase_in/";
		FileInputFormat.addInputPath(job, new Path(path));
		System.exit(job.waitForCompletion(true)?0:1);

		
	}

}
