package kevin.zhang;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.util.HDFSCommon;
import com.util.PropertiesInit;

public class Ictclas 
{
	
	static private Logger log = Logger.getLogger(Ictclas.class.getName());
	
	private String ictclas_dir;
	private String userdict;
	private NLPIR nlpir;
	
	public Ictclas(){
		
		PropertiesInit property = new PropertiesInit("plugins/conf/config.properties");
		
		/*-------------------------下载hdfs文件到本地---start--------------*/
		try {
			HDFSCommon hdfsCommon = new HDFSCommon();
			String hdfsPath = property.getPro().getProperty("mydictHDFSPath");
			String localPath = property.getPro().getProperty("mydictLinuxPath");
			hdfsCommon.downFile(hdfsPath, localPath);
			userdict = localPath;
		} catch (IOException e) {
			e.printStackTrace();
		}
		/*-------------------------下载hdfs文件到本地----end---------------*/
		
		if(System.getProperty("os.name").contains("Windows")){
			ictclas_dir = property.getPro().getProperty("mydataWindowPath");
			userdict = property.getPro().getProperty("mydictWindowPath");
		}else{
			ictclas_dir = property.getPro().getProperty("mydataLinuxPath");
			userdict = property.getPro().getProperty("mydictLinuxPath");
		}

		nlpir = new NLPIR();
		init(ictclas_dir);
		loadDic(userdict);
	}
	
	public Ictclas(String name){
		init(name);
	}

	/**
     * 分词系统初始
     * @param path: 分词相关文件的路
     * @return 是否初始化成
     */
	public boolean init(String path) {
		try {
			if (NLPIR.NLPIR_Init(path.getBytes(), 0) == false) {
				log.info("NLPIR_Init Fail!");
				System.out.println("NLPIR_Init Fail!");
				return false;
			}
			else {
				log.info("NLPIR_Init Succeed!" + "\n");
				System.out.println("NLPIR_Init Succeed!" + "\n");
			}
		}
		catch (Exception e) {
			log.error(e.getMessage(), e);
			e.printStackTrace();
		}
		nlpir.NLPIR_SetPOSmap(1);
		return true;
	}
	
	/**
     * 加载用户词典
     * @param sPath: 词典文件的路径和文件
     */
	public void loadDic(String sPath) {
		long begin = System.currentTimeMillis();
		System.out.println("dic_path: " + sPath);
		int n = nlpir.NLPIR_ImportUserDict(sPath.getBytes());
		long end = System.currentTimeMillis();
		log.info("import words: " + n + "  timeuse: " + (end - begin) + "\n");
	}
	
	/**
     * 对字符串进行分词
     * @param input: 待分词的字符
     * @return 分词后的字符
     */
	public String clasSentence(String input) {
		String output = "";
		try {		
			byte nativeBytes[] = nlpir.NLPIR_ParagraphProcess(input.getBytes("GB2312"), 1);
			output = new String(nativeBytes, "GB2312");
		}
		catch (Exception ex) {
			log.error(ex.getMessage(), ex);
			ex.printStackTrace();
		}
		return output;
	}
	
	public void exit() {
		NLPIR.NLPIR_Exit();
	}
}
