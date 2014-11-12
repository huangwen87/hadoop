package com.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 *  hdfs文件增删查改操作
 *  @author darwen
 *  2013-12-20  下午5:36:01
 */
public class HDFSCommon {
	
	private Configuration conf;  
    private FileSystem fs;  
    
    public HDFSCommon() throws IOException{  
        conf=new Configuration();  
        fs=FileSystem.get(conf);  
    }  
	
    /** 
     * 上传文件， 
     * @param localFile 本地路径 
     * @param hdfsPath 格式为hdfs://ip:port/destination 
     * @throws IOException 
     */  
    public void upFile(String localFile,String hdfsPath) throws IOException{  
        InputStream in=new BufferedInputStream(new FileInputStream(localFile));  
        OutputStream out=fs.create(new Path(hdfsPath));  
        IOUtils.copyBytes(in, out, conf);  
    }
    
    /** 
     * 附加文件 
     * @param localFile 
     * @param hdfsPath 
     * @throws IOException 
     */  
    public void appendFile(String localFile,String hdfsPath) throws IOException{  
        InputStream in=new FileInputStream(localFile);  
        OutputStream out=fs.append(new Path(hdfsPath));  
        IOUtils.copyBytes(in, out, conf);  
    }  
    
    /** 
     * 下载文件 
     * @param hdfsPath 
     * @param localPath 
     * @throws IOException 
     */  
    public void downFile(String hdfsPath, String localPath) throws IOException{  
        InputStream in=fs.open(new Path(hdfsPath));  
        OutputStream out=new FileOutputStream(localPath);  
        IOUtils.copyBytes(in, out, conf);  
    } 
    
    /** 
     * 删除文件或目录 
     * @param hdfsPath 
     * @throws IOException 
     */  
    public void delFile(String hdfsPath) throws IOException{  
        fs.delete(new Path(hdfsPath), true);  
    }  
}
