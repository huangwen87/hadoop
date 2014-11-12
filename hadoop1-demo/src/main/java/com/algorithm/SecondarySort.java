package com.algorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 自定数据类型   对数据进行二次排序
 * 首先按第一key排序，如果第一key相同桉第二key排序
 * @author darwen
 * 2013-12-12  上午9:54:16
 */
public class SecondarySort {
	
	//自己定义的key类应该实现WritableComparable接口  
	public static class IntPair implements WritableComparable<IntPair>{
		
		private int first;
		private int second;
		
		public int getFirst() {
			return first;
		}
		public void set(int first, int second) {
			this.first = first;
			this.second = second;
		}
		public int getSecond() {
			return second;
		}
		
		
		@Override
		public int compareTo(IntPair o) {
			if(this.first != o.first){
				return this.first < o.first ? -1:1;
			}else if(this.second != o.second){
				return this.second < o.second ? -1:1;
			}else{
			    return 0;
			}
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readInt();
			this.second = in.readInt();
		}
		
		@Override
		public int hashCode() {
			return this.first * 157 + this.second;
		}
		
		@Override
		public boolean equals(Object right) {
			if(right == null){
				return false;
			}else if(right instanceof IntPair){
				IntPair r = (IntPair)right;
				return r.first == this.first && r.second == this.second;
			}else{
				return false;
			}
		}
		
	}
	
	//分区
    public static class PartitionBar extends Partitioner<IntPair, IntWritable>{

		@Override
		public int getPartition(IntPair key, IntWritable value, int numPartitions) {
			return Math.abs(key.first * 127) % numPartitions;
		}
    	
    }
    
    
    /**
     * 自定义分组，只要key的first相同就分到一组中去
     */
    public static class GroupingComparator extends WritableComparator{
    	
    	protected GroupingComparator(){
    		super(IntPair.class, true);
    	}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair p1 = (IntPair) a;
			IntPair p2 = (IntPair) b;
			int l = p1.getFirst();
			int r = p2.getFirst();
			return l == r ? 0:(l < r ? -1:1);
		}
		
    	
    }
    
    
    public static class MapClass extends Mapper<Object, Text, IntPair, IntWritable>{
    	private  final IntPair keyinfo = new IntPair();
    	private  final IntWritable valueinfo = new IntWritable();
    	
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer token = new StringTokenizer(value.toString());
			int first = 0;
			int second = 0;
			if(token.hasMoreTokens()){
				first = Integer.parseInt(token.nextToken());
				if(token.hasMoreTokens()){
					second = Integer.parseInt(token.nextToken());
				}
				keyinfo.set(first, second);
				valueinfo.set(second);
				context.write(keyinfo, valueinfo);
			}
			
		}
    	
    }
    
    public static class ReduceClass extends Reducer<IntPair, IntWritable, Text, IntWritable>{
    	
    	private static final Text left = new Text();
    	private static final Text SEPARATOR =
    			new Text("---------------------------");

		@Override
		protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(SEPARATOR, null);
		    left.set(String.valueOf(key.getFirst()));
			for(IntWritable iw : values){
				context.write(left, iw);
			}
			
		}
    	
    }
    

	public static void main(String[] args) throws IOException, 
	                           InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "10.15.107.64:9001");
        String[] ioArgs= args;
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: SecondarySort <in> <out>");
            System.exit(2);
        }
        
        Job job = new Job(conf, "SecondarySort");
        job.setJarByClass(SecondarySort.class);
        
        //设置Map和Reduce处理类
        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(PartitionBar.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(ReduceClass.class);
        
	    //设置输出类型
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    
	    
	    //设置输入和输出目录
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
