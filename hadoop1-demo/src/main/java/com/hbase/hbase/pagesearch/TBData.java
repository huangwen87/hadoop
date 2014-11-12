package com.hbase.hbase.pagesearch;

import java.util.List;
import java.util.Map;

/**
 * desc： 配置页面的相关参数
 * @author darwen
 * 2014-4-10 下午2:09:52
 */
public class TBData {
	
	private Integer currentPage;
	private Integer pageSize;
	private Integer totalCount;
	private Integer totalPage;
	private List<Map<String, String>> resultList;
	
	
	public Integer getCurrentPage() {
		return currentPage;
	}
	public void setCurrentPage(Integer currentPage) {
		this.currentPage = currentPage;
	}
	public Integer getPageSize() {
		return pageSize;
	}
	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}
	public Integer getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(Integer totalCount) {
		this.totalCount = totalCount;
	}
	public Integer getTotalPage() {
		return totalPage;
	}
	public void setTotalPage(Integer totalPage) {
		this.totalPage = totalPage;
	}
	public List<Map<String, String>> getResultList() {
		return resultList;
	}
	public void setResultList(List<Map<String, String>> resultList) {
		this.resultList = resultList;
	}
	
	@Override
	public String toString() {
		return "TBData [currentPage=" + currentPage + ", pageSize=" + pageSize
				+ ", totalCount=" + totalCount + ", totalPage=" + totalPage
				+ ", resultList=" + resultList + "]";
	}

}
