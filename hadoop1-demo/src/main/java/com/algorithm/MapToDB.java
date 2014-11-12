package com.algorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** map完数据  到mysql中去
 * 
 * @author Darwen
 *
 */
public class MapToDB extends Configured implements Tool{

    public static class EventsDBWritable implements Writable, DBWritable{

        String uname;
        String upassword;
        
        public EventsDBWritable(){
        	
        }
    	
		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.uname);
			statement.setString(2, this.upassword);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
            this.uname = resultSet.getString(1);
            this.upassword = resultSet.getString(2);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, this.uname);
			Text.writeString(out, this.upassword);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
            this.uname = Text.readString(in);
            this.upassword = Text.readString(in);
		}

		@Override
		public String toString() {
			return "EventsDBWritable [uname=" + uname + ", upassword="
					+ upassword + "]";
		}
    	
    }

    
    public static class Mapclass extends MapReduceBase 
         implements Mapper<LongWritable,Text,EventsDBWritable,Text>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<EventsDBWritable, Text> output, Reporter reporter)
				throws IOException {
			String[] splits = value.toString().split(" ");
			EventsDBWritable r = new EventsDBWritable();
			r.uname = splits[0];
			r.upassword = splits[1];
			output.collect(r, new Text(""));			
		}

    	
    }
 
    
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, MapToDB.class);
		job.set("mapred.job.tracker", "10.15.107.64:9001");
		Path in = new Path(args[0]);
		FileInputFormat.setInputPaths(job, in);
		
		job.setJobName("MapToDB");
		job.setMapperClass(Mapclass.class);
		
		job.setOutputKeyClass(EventsDBWritable.class);  
	    job.setOutputValueClass(Text.class);  
 
	        
		job.setOutputFormat(DBOutputFormat.class);
		
		DBOutputFormat.setOutput(job, "user", "uname", "upass");
		
		DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver", "jdbc:mysql://10.15.107.75:3306/test",
				"root", "111111");
		
		
		job.setNumReduceTasks(0);

		JobClient.runJob(job);

		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new MapToDB(), args);
       System.exit(res);
	}

}
