package com.turing.dataengineering.service;

import java.util.List;

/**
 * The Interface JGitService.
 * 
 * @author thankgodukachukwu
 *
 */
public interface JGitService {

	public void cloneGit(String  path, String dir);
	
	public List<String> searchRepository(String dir);
			
}
