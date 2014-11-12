package com.gw.common;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;

public class ZipUtil {
    static final int BUFFER = 8192;

    public void compress(String srcPathName,String pathName) {
		File file = new File(srcPathName);
		if (!file.exists())
			throw new RuntimeException(srcPathName + "不存在！");
		try {
			File zipFile = new File(pathName);
			FileInputStream fis = new FileInputStream(file);
			FileOutputStream fileOutputStream = new FileOutputStream(zipFile);
			compress(fis, file.getName(),fileOutputStream);
			fileOutputStream.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    public void compress(InputStream finput,String name,OutputStream foutput) {

		try{
			CheckedOutputStream cos = new CheckedOutputStream(foutput,new CRC32());
			ZipOutputStream out = new ZipOutputStream(cos);
			String basedir = "";
			compressFile(finput, out, name);
			out.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
	  private void compressDirectory(File dir, ZipOutputStream out, String basedir) {
	  if (!dir.exists())
	  return;

	  File[] files = dir.listFiles();
	  for (int i = 0; i < files.length; i++) {
	  compress(files[i], out, basedir + dir.getName() + "/");
	  }
	  }
	*/
    /** 压缩一个文件 */
    private void compressFile(InputStream file, ZipOutputStream out, String name) {
		try {
			BufferedInputStream bis = new BufferedInputStream(file);
			ZipEntry entry = new ZipEntry(name);
			out.putNextEntry(entry);
			int count;
			byte data[] = new byte[BUFFER];
			while ((count = bis.read(data, 0, BUFFER)) != -1) {
				out.write(data, 0, count);
			}
			bis.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}  
