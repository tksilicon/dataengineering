package com.turing.dataengineering.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The Class JGitServiceImpl.
 *
 * @author thankgodukachukwu
 */
/**
 * @author thankgodukachukwu
 *
 */
@Service("jgitServiceImpl")
public class JGitServiceImpl implements JGitService {
	private static final Logger logger = LoggerFactory.getLogger(JGitServiceImpl.class);
	private String fileNameToSearch;
	private List<String> result = null; 
	private static final String TEMPWORKDIR = "src/main/resources/tempwork/";

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.turing.dataengineering.service.JGitService#cloneGit(java.lang.String)
	 */
	@Override
	public void cloneGit(String repoUrl, String dir) {

		try {

			Git.cloneRepository().setURI(repoUrl).setBranchesToClone(Arrays.asList("refs/heads/master")).setDirectory(new File(TEMPWORKDIR + dir)).call();
			
			

		} catch (GitAPIException e) {

		}

	}

	/*
	 * (non-Javadoc)
	 * List of URLS found is reinitialized here for each repository search
	 * 
	 * @see
	 * com.turing.dataengineering.service.JGitService#searchRepository(java.lang.
	 * String)
	 */

	@Override
	public List<String> searchRepository(String dir) {
		
		result = new ArrayList<>();

		searchDirectory(new File(TEMPWORKDIR + dir), ".py");

		return result;

	}

	/**
	 * 
	 * @return
	 */

	public String getFileNameToSearch() {
		return fileNameToSearch;
	}

	/**
	 * 
	 * @param fileNameToSearch
	 */
	public void setFileNameToSearch(String fileNameToSearch) {
		this.fileNameToSearch = fileNameToSearch;
	}

	/**
	 * 
	 * @param directory
	 * @param fileNameToSearch
	 */
	public void searchDirectory(File directory, String fileNameToSearch) {

		setFileNameToSearch(fileNameToSearch);

		if (directory.isDirectory()) {
			search(directory);
		}

	}

	/**
	 * 
	 * 
	 * @param file
	 */

	private void search(File file) {

		if (file.isDirectory()) {

			if (file.canRead()) {
				for (File temp : file.listFiles()) {
					if (temp.isDirectory()) {
						search(temp);
					} else {
						if (temp.getName().toLowerCase().endsWith(getFileNameToSearch())) {
							result.add(temp.getAbsoluteFile().toString());
						}

					}
				}

			} else {

			}
		}

	}

}
