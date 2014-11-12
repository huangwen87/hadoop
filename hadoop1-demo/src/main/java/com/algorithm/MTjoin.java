package com.algorithm;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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
 * 多表关联
 * @author darwen
 * 2013-12-5  下午3:37:54
 */
public class MTjoin {
	
	 public static int time = 0;
	
	public static class MapClass extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String relationType = new String();
			//两个文件，首行不处理
			if(line.contains("factoryname") ||
					line.contains("addressname")){
				return;
			}
			
			//每行的数据处理
			StringTokenizer token = new StringTokenizer(line);
			String mapkey = new String();
			String mapvalue = new String();
			int flag = 0;
			while(token.hasMoreTokens()){
				String record = token.nextToken();
				if(record.charAt(0) >='0' && record.charAt(0) <= '9'){
					mapkey = record;
					if(flag > 0){
						relationType = "1";
					}else{
						relationType = "2";
					}
					continue;  //不执行下去了
				}
				//工厂的名字也会分裂
				mapvalue += record + " ";
				flag++;
			}
			context.write(new Text(mapkey), new Text(relationType+"+"+mapvalue));
		}	
	}
	
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			// 输出表头
            if (0 == time) {
                context.write(new Text("factoryname"), new Text("addressname"));
                time++;
            }
            
			int fatorynum = 0;
			String[] fatory = new String[10];
			int addressnum = 0;
			String[] address = new String[10];
			
			Iterator<Text> itor = values.iterator();
			while(itor.hasNext()){
				String line = itor.next().toString();
				int len = line.length();
				int flag = 2;
				if(len == 0){
					continue;
				}				
				char relationType = line.charAt(0);
				
				if(relationType == '1'){
					fatory[fatorynum++] = line.substring(flag); 
				}else if(relationType == '2'){
					address[addressnum++] = line.substring(flag);
				}	
			}
			
			if(fatorynum != 0 && addressnum!=0){
				for(int i = 0; i < fatorynum; i++){
					for(int j = 0; j < addressnum; j++){
						context.write(new Text(fatory[i]), new Text(address[j]));
					}
				}
			}		
		}		
	}
	

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "10.15.107.64:9001");
        String[] ioArgs = args;
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Multiple Table Join <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Multiple Table Join");
        job.setJarByClass(MTjoin.class);
        // 设置Map和Reduce处理类
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
