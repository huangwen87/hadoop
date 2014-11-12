package com.gw.common;

import java.lang.String;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import com.gw.common.hdfs.HdfsUtil;
import com.gw.common.FileMerger;

public class DataCollect {
	
	public static void main(String[] args)throws Exception {
		String dates = args[0];
		String spath = args[1];
		String destpath = args[2];
		HdfsUtil fu = HdfsUtil.getInstance();
		
		if (!fu.fileExists(destpath)) {
			fu.mkdir(destpath);
		}
	
		String regex = dates;
		FileMerger.mergeFile(spath,regex,destpath);

		//		fu.writeFile(destpath + "/_SUCCESS", "" ,"UTF-8");
		
	}

}
