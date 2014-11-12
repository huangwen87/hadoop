package com.hbase.hbase.performancetest;

/**
 * desc： 存储过程结果的实体类
 * @author darwen
 * 2014-4-9 上午8:59:05
 */
public class Model {
	
	private long newsid;
	private int ID;
	private String publishdate;
	private String newstype;
	private String title;
	private String text;
	
	
	public long getNewsid() {
		return newsid;
	}
	public void setNewsid(long newsid) {
		this.newsid = newsid;
	}
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	public String getPublishdate() {
		return publishdate;
	}
	public void setPublishdate(String publishdate) {
		this.publishdate = publishdate;
	}
	public String getNewstype() {
		return newstype;
	}
	public void setNewstype(String newstype) {
		this.newstype = newstype;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	
	
	@Override
	public String toString() {
		return "Model [newsid=" + newsid + ", ID=" + ID + ", publishdate="
				+ publishdate + ", newstype=" + newstype + ", title=" + title
				+ ", text=" + text + "]";
	}
	 
}
