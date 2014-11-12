package com.gw.common;

import java.util.*;
import java.io.IOException;
import java.lang.String;
import java.util.regex.*;

import com.gw.common.hdfs.HdfsUtil;

public class FileMerger {
	
	public static void mergeFile (String sourcepath,String regex,String destpath) throws IOException {
		
		HdfsUtil fu = HdfsUtil.getInstance();
		
		if (!fu.fileExists(destpath)) {
			fu.mkdir(destpath);
		}	

		List<String>fl = fu.getFileList(sourcepath,3,regex,0);
		if (fl == null) {
			throw new IOException("符合要求的源文件不存在!");
		}

		Map<String,Set<String> > filemap = new HashMap<String,Set<String> >();
		Pattern pattern = Pattern.compile(regex);
		for (String fn : fl) {
			Matcher matcher = pattern.matcher(fn);
			if(!matcher.find() || matcher.groupCount() < 1)
				throw new IOException("非法的文件名正则，必须包含合并的新文件名group!");
			String nfn = matcher.group(1);
			Set<String> fset = filemap.get(nfn);
			if (fset == null) {
				fset = new TreeSet<String>();
				filemap.put(nfn,fset);
			}
			fset.add(fn);
		}
		for (String fn : filemap.keySet()) {
			List<String> filel = new ArrayList<String>();
			Set<String> fset = filemap.get(fn);
			for (String str : fset) {
				String fullname = sourcepath + "/" + str;
				filel.add(fullname);
			}
			fn = destpath + "/" + fn;
			if (fu.fileExists(fn)) {
				fu.deleteHadoopFile(fn);
			}
			fu.combine(filel,fn,"UTF-8");
		}


	}

}
