package com.algorithm.mybloomfilter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BloomFilterMR {

	public static class MapClass extends Mapper<Text, Text, Text, BloomFilter<String>>{

		  //自己写得方法
        BloomFilter<String> bf = new BloomFilter<String>();  //注意add()&contains()
		
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
	         bf.add(key.toString());
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("testkey"), bf);
		}
	}
	
	
	public static class ReducerClass extends Reducer<Text, BloomFilter<String>, Text, Text>{

		BloomFilter<String> bf = new BloomFilter<String>();
		
		@Override
		protected void reduce(Text key, Iterable<BloomFilter<String>> values, Context context)
				throws IOException, InterruptedException {
			while(values.iterator().hasNext()){
				bf.union((BloomFilter<String>)values.iterator().next());
			}
		}
		
	}
	
	
}
