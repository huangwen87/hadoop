package com.gw.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class Ftp {

    private  FTPClient ftp;
    /**
	 *
	 * @param path 上传到ftp服务器哪个路径下
	 * @param addr 地址
	 * @param port 端口号
	 * @param username 用户名
	 * @param password 密码
	 * @return
	 * @throws Exception
	 */
    public  boolean connect(String addr,int port,String username,String password) throws Exception {
		boolean result = false;
		ftp = new FTPClient();
		int reply;
		ftp.connect(addr,port);
		ftp.login(username,password);
		ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
		reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			return result;
		}
		//		ftp.changeWorkingDirectory(path);
		result = true;
		return result;
	}
    /**
	 *
	 * @param file 上传的文件或文件夹
	 * @throws Exception
	 */
    public void upload(File file,String path) throws Exception{
		ftp.changeWorkingDirectory(path);

		if(file.isDirectory()){
			ftp.makeDirectory(file.getName());
			ftp.changeWorkingDirectory(file.getName());
			String[] files = file.list();
			for (int i = 0; i < files.length; i++) {
				File file1 = new File(file.getPath()+"\\"+files[i] );
				if(file1.isDirectory()){
					upload(file1,path);
					ftp.changeToParentDirectory();
				}else{
					File file2 = new File(file.getPath()+"\\"+files[i]);
					FileInputStream input = new FileInputStream(file2);
					ftp.storeFile(file2.getName(), input);
					input.close();
				}
			}
		}else{
			File file2 = new File(file.getPath());
			FileInputStream input = new FileInputStream(file2);
			ftp.storeFile(file2.getName(), input);
			input.close();
		}
	}
    public void uploadFile(InputStream file,String name,String path) throws Exception{
		ftp.changeWorkingDirectory(path);
		ftp.storeFile(name, file);
	}

	public static void main(String[] args) throws Exception{
		Ftp t = new Ftp();
		t.connect("10.15.107.69", 21, "crm", "111111");
		File file = new File("/home/gumeng/workspace/BehaviorAnalysis/trunk/test.txt");
		t.upload(file,"/crm_ftp/UBA/");
	}
}    
