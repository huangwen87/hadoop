package com.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * 根据文件分块（而不是hdfs分块）
 * @author darwen
 * 2013-12-6  下午2:21:50
 */
public class AllFileSplitInputFormat extends FileInputFormat<LongWritable, Text>{
	
	private static final double SPLIT_SLOP = 1.1;   // 10% slop
	
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	
	private static final Log LOG = LogFactory.getLog(AllFileSplitInputFormat.class);
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LineRecordReader();
	}

	
	/**
	 * 一个文件为其自身的分块
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}


	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		 long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		    long maxSize = getMaxSplitSize(job);

		    // generate splits
		    List<InputSplit> splits = new ArrayList<InputSplit>();
		    List<FileStatus>files = listStatus(job);
		    for (FileStatus file: files) {
		      Path path = file.getPath();
		      FileSystem fs = path.getFileSystem(job.getConfiguration());
		      long length = file.getLen();
		      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
		      if ((length != 0) && isSplitable(job, path)) { 
		        long blockSize = file.getBlockSize();
		        long splitSize = computeSplitSize(blockSize, minSize, maxSize);
                
		        LOG.info("文件名：" + file.getPath().toString().
		        		  substring(file.getPath().toString().indexOf("file")));
		        LOG.info("blockSize: " + blockSize);
		        LOG.info("*****************");
		        LOG.info("minSize: " + minSize);
		        LOG.info("getFormatMinSplitSize(): " + getFormatMinSplitSize());
		        LOG.info("getMinSplitSize(job): "    + getMinSplitSize(job));
		        LOG.info("*****************");
		        LOG.info("maxSize: " + maxSize);
		        LOG.info("*****************");
		        LOG.info("splitSize: " + splitSize);
		        
		        LOG.info("mapred.map.tasks: " + job.getConfiguration().get("mapred.map.tasks"));
		        LOG.info("===================================");
		        LOG.info("===================================");
		        
		        
		        long bytesRemaining = length;
		        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
		          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
		          splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
		                                   blkLocations[blkIndex].getHosts()));
		          bytesRemaining -= splitSize;
		        }
		        
		        if (bytesRemaining != 0) {
		          splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
		                     blkLocations[blkLocations.length-1].getHosts()));
		        }
		      } else if (length != 0) {
		        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
		      } else { 
		        //Create empty hosts array for zero length files
		        splits.add(new FileSplit(path, 0, length, new String[0]));
		      }
		    }
		    
		    // Save the number of input files in the job-conf
		    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		    LOG.info("Total # of splits: " + splits.size());
		    return splits;
	}
	
	

}
