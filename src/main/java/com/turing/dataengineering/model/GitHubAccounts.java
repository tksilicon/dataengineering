package com.turing.dataengineering.model;

import java.util.List;

import org.springframework.stereotype.Component;

@Component
public class GitHubAccounts {
	
	List<String> listOfUrls;

	public GitHubAccounts() {
		super();
	}

	public GitHubAccounts(List<String> listOfUrls) {
		super();
		this.listOfUrls = listOfUrls;
	}
	

	/**
	 * 
	 * @return
	 */
	public List<String> getListOfUrls() {
		return listOfUrls;
	}

	/**
	 * 
	 * @param listOfUrls
	 */
	public void setListOfUrls(List<String> listOfUrls) {
		this.listOfUrls = listOfUrls;
	}
	
	

}
