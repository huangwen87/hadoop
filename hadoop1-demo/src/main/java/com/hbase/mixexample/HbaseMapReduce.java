package com.hbase.mixexample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

/**
 *  找到具有相同兴趣的人，我们简单定义为如果author之间article的tag相同，
 *  则认为两者有相同兴趣，将分析结果保存到HBase。除了上面介绍的blog表外，
 *  我们新增一张表tag_friend，RowKey为tag，Value为authors,
 *  
 *  hbase表结构：
 *  表名blog        列簇：article、author
 *  
 *  表名tag_friend  列簇 tag、person  
 *  
 *  @author darwen
 *  2013-12-26  下午1:59:53
 */
public class HbaseMapReduce {
   

	public static class Mapper extends 
	                  TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>{

		public Mapper(){
			
		}
		
		@Override
		protected void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException, InterruptedException {
			ImmutableBytesWritable value = null;
			String[] tags = null;
			for(KeyValue kv : values.list()){
				System.out.println("=======");
				System.out.println("Family:" + Bytes.toString(kv.getFamily()));
				System.out.println("Qualifier:" + Bytes.toString(kv.getQualifier()));
				System.out.println("value:" + Bytes.toString(kv.getValue()));
				if("author".equals(Bytes.toString(kv.getFamily()))
						&& "nickname".equals(Bytes.toString(kv.getQualifier()))){
					value = new ImmutableBytesWritable(kv.getValue());
				}
				if("article".equals(Bytes.toString(kv.getFamily()))
						&& "tags".equals(Bytes.toString(kv.getQualifier()))){
					tags = Bytes.toString(kv.getValue()).split(",");
				}
			}
			
			for(int i = 0; i < tags.length; i++){
					ImmutableBytesWritable key = new ImmutableBytesWritable(
							Bytes.toBytes(tags[i].toLowerCase()));
					try {
						System.out.println("key: " + key + " ==  value:" + value);
						context.write(key, value);
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
			}
		}
		
	}
	
	
	
	public static class Reducer extends 
	                 TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>{

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			
			String friends = "";
			for(ImmutableBytesWritable val : values){
				friends += (friends.length() > 0 ? "," : "") + Bytes.toString(val.get());
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("person"), Bytes.toBytes("nicknames"), Bytes.toBytes(friends));
			context.write(key, put);
			
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
         Configuration conf = new Configuration();
         conf = HBaseConfiguration.create(conf);
         Job job = new Job(conf, "HbaseMapReduce");
         job.setJarByClass(HbaseMapReduce.class);
         
         Scan scan = new Scan();
         scan.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
         scan.addColumn(Bytes.toBytes("article"), Bytes.toBytes("tags"));
         
         TableMapReduceUtil.initTableMapperJob("blog", scan, Mapper.class, 
        		 ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
         
         TableMapReduceUtil.initTableReducerJob("tag_friend", Reducer.class, job);
         System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
