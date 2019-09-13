package com.turing.dataengineering.kafta.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.turing.dataengineering.model.DataEnginnerModel;
import com.turing.dataengineering.model.GitHubAccounts;
import com.turing.dataengineering.service.JGitService;

/**
 * Consumer service that processes github accounts and listens to Producer in
 * dataengineeringine module
 * 
 * @author thankgodukachukwu
 *
 */
@Service
public class Consumer {

	private final Logger logger = LoggerFactory.getLogger(Consumer.class);

	private static final String COMMENTS = "\"\"\"";
	private static final String IMPORTCOM = "import";
	private static final String SRCDATASET = "src/main/resources/dataset/";
	private static final String JSONT = ".json";
	private static final String TEMPWORKDIR = "src/main/resources/tempwork";
	private static final String USERDIR = "user.dir";

	JGitService jgit;

	ObjectMapper objectMapper;

	@Autowired
	public Consumer(JGitService jgit) {
		this.jgit = jgit;

	}

	/**
	 * 
	 * @param message the github URLS sent in batches for processing. Each message
	 *                is converted to URLS by objectmapper
	 * @throws IOException
	 */

	@KafkaListener(topics = "szghaz9h-users", groupId = "szghaz9h-consumers")
	public void consume(String message) throws IOException {
		logger.info("Consuming message");

		/**
		 * 
		 * Delete the temporary working directory where the repositories are cloned if
		 * it exist. And create an objectmapper to convert messages to json string
		 * 
		 * GitHubAccounts is used to deserialize the string to a GitHubAccount object
		 * for processing
		 * 
		 * and fetch the URLS sent in this message
		 */
		objectMapper = new ObjectMapper();
		GitHubAccounts git = objectMapper.readValue(message, GitHubAccounts.class);
		List<String> records = git.getListOfUrls();

		/**
		 * 
		 * Iterate through the list of URLs to clone them with jgit Clone repositories
		 * directories if they don't exist
		 */

		Iterator<String> crunchifyIterator = records.iterator();

		while (crunchifyIterator.hasNext()) {
			String str = crunchifyIterator.next();

			String[] checkFileExists = str.split("/");

			Path p4 = FileSystems.getDefault()
					.getPath(SRCDATASET + checkFileExists[checkFileExists.length - 1] + JSONT);

			if (System.getProperty("os.name").equals("Linux")) {

				Optional<String> directoryChecking = Optional.of(System.getProperty(USERDIR)
						+ "/src/main/resources/tempwork/" + checkFileExists[checkFileExists.length - 1] + "/");

				if (!Files.exists(p4) && !Files.isDirectory(Paths.get(directoryChecking.get()))) {
					logger.info("json doesnt exists/repository doesnt exists");
					/**
					 * json and directory doesnt exist, clone it first and check if processing has
					 * been done else process
					 * 
					 */

					jgit.cloneGit(str, checkFileExists[checkFileExists.length - 1]);

					Map<String, DataEnginnerModel> map = getAnalysis(
							jgit.searchRepository(checkFileExists[checkFileExists.length - 1]), str,
							checkFileExists[checkFileExists.length - 1]);

					String firstKey = map.keySet().stream().findFirst().get();
					DataEnginnerModel body = map.get(firstKey);

					FileUtils.touch(new File(SRCDATASET + firstKey + JSONT));
					File file2 = new File(SRCDATASET + firstKey + JSONT);

					try (FileWriter writer = new FileWriter(file2);) {
						writer.write(objectMapper.writeValueAsString(body));

					} catch (Exception e) {
						logger.info(e.getMessage());
					}

				} else if (!Files.exists(p4) && Files.isDirectory(Paths.get(directoryChecking.get()))) {

					logger.info("json doesnt exists/repository exists");

					/**
					 * 
					 * json doesnt exist but directory exist so we check if processing has been done
					 * else process
					 * 
					 */

					Map<String, DataEnginnerModel> map = getAnalysis(
							jgit.searchRepository(checkFileExists[checkFileExists.length - 1]), str,
							checkFileExists[checkFileExists.length - 1]);

					String firstKey = map.keySet().stream().findFirst().get();
					DataEnginnerModel body = map.get(firstKey);

					FileUtils.touch(new File(SRCDATASET + firstKey + JSONT));
					File file2 = new File(SRCDATASET + firstKey + JSONT);

					try (FileWriter writer = new FileWriter(file2);) {
						writer.write(objectMapper.writeValueAsString(body));

					} catch (Exception e) {

					}

				} else if (Files.exists(p4)) {
					// do nothing
					logger.info(
							"json exists/we dont care about repository/if you want to force another analysis. Use the force/update analysis feature");
				}

			}
		}

		/**
		 * Clean up When we are done processing messages the tempwork directory is
		 * deleted to avoid keep heavy files in the file system
		 * 
		 */

		FileUtils.deleteDirectory(new File(TEMPWORKDIR));

	}

	/**
	 * These class variables are used to run the analysis. They are reinitialized
	 * for every call
	 **/
	Integer linesOfCodeRepository;
	List<Integer> nextFactorOverall;
	List<Integer> averageParameters;
	Set<String> packages;
	List<Integer> averageVariables;

	FileSystemResource pyFile;
	BufferedReader reader2;

	String line2;
	Integer lines;

	/**
	 * Using deque instead of stack because it is synchonizable
	 */
	Deque<String> stack;
	Integer nestFactor;

	/**
	 * 
	 * The application logic to find lines in a program file, the nesting factor,
	 * average parameters, eliminate comments, find libraries and other logic is
	 * handled here.
	 *
	 * 
	 * @param filesFound the files found per repository
	 * @param repo       the repository url
	 * @param repoName   the short name of the repo which correspond to its
	 *                   directory on the file system
	 * @return
	 * @throws IOException
	 */
	private Map<String, DataEnginnerModel> getAnalysis(List<String> filesFound, String repo, String repoName)
			throws IOException {

		logger.info("Analysis logic running");

		Iterator<String> iterator = filesFound.iterator();

		Map<String, DataEnginnerModel> mapToRuturn = new HashMap<>();

		linesOfCodeRepository = 0;
		nextFactorOverall = new ArrayList<>();
		averageParameters = new ArrayList<>();
		packages = new LinkedHashSet<>();
		averageVariables = new ArrayList<>();

		String str = null;

		while (iterator.hasNext()) {

			str = iterator.next();

			pyFile = new FileSystemResource(str);

			reader2 = new BufferedReader(new InputStreamReader(pyFile.getInputStream()));

			lines = 0;

			/**
			 * Using deque instead of stack because it is synchonizable
			 */
			stack = new ArrayDeque<>();
			int index = 0;
			nestFactor = 0;

			while ((line2 = reader2.readLine()) != null) {

				findParameters(line2);
				findCount(line2);
				findComments(line2);

				/**
				 * The nesting factor is tracked here with index of first for should be less
				 * than the next indexes of nesting for.
				 */

				if (line2.trim().startsWith("for") && !line2.startsWith("#") && stack.isEmpty()) {

					if (index == 0) {
						index = line2.indexOf("for");
						nestFactor++;
					} else if (index != 0 && line2.indexOf("for") > index) {
						nestFactor++;

					}

				}

			}

			reader2.close();

			nextFactorOverall.add(nestFactor);
			linesOfCodeRepository += lines;

		}

		/*
		 * 
		 * The data generated is packaged in DataEnginnerModel and returned.
		 */

		StringBuilder build = new StringBuilder("[");

		String strLib = StringUtils.join(packages, ",");
		build.append(strLib);
		build.append("]");

		DataEnginnerModel dataEnginnerModel = new DataEnginnerModel();
		dataEnginnerModel.setRepository_url(repo);
		dataEnginnerModel.setNumberOfLines(linesOfCodeRepository);
		dataEnginnerModel.setLibraries(build.toString());
		dataEnginnerModel.setNestingFactor(nextFactorOverall.stream().mapToInt(val -> val).average().orElse(0.0));
		dataEnginnerModel.setAverageParameters(averageParameters.stream().mapToInt(val -> val).average().orElse(0.0));
		dataEnginnerModel.setAverageVariables(averageVariables.stream().mapToInt(val -> val).average().orElse(0.0));

		mapToRuturn.put(repoName, dataEnginnerModel);

		return mapToRuturn;
	}

	/**
	 * 
	 * Variables are assigned with equal too though some assigments are references
	 * but a heuristics of taking all equal to sign is a minimal solution
	 * 
	 * @param lines2 the current line
	 */

	private void findCount(String lines2) {

		Pattern p = Pattern.compile("(?U)(\\w+)\\W" + "=" + "\\W+(\\w+)");

		Matcher m = p.matcher(line2);
		int count = 0;
		while (m.find()) {
			count++;
		}
		averageVariables.add(count);

	}

	/**
	 * Method parameters can only appear in lines that have function which starts
	 * with "def"
	 * 
	 * @param line2 the current line
	 */

	private void findParameters(String line2) {

		if (line2.startsWith("def")) {

			String methodParametersStr = StringUtils.substringBetween(line2, "(", ")");

			if (methodParametersStr != null && !methodParametersStr.isEmpty()) {
				String[] methodParameters = methodParametersStr.split(",");
				averageParameters.add(methodParameters.length);
			}

		}
	}

	/**
	 * 
	 * Eliminate all lines that begin with comments. Comments come in three forms
	 * 
	 * A stack in this case deque is used to track comments. Once a comment is
	 * opened, it is popped on the stack and and every next line, the stack is
	 * checked, if it not empty, it then a comment. Once a closing comment is seen,
	 * the begining comment is polled out of the stack signalling that we can now
	 * continue with reading codes
	 * 
	 * @param line2 the current line
	 * 
	 */
	private void findComments(String line2) {

		if (line2.startsWith(COMMENTS) && stack.isEmpty()) {
			stack.push(COMMENTS);
		} else if (!line2.startsWith(COMMENTS) && !stack.isEmpty()) {
			// do nothing
		} else if (line2.startsWith(COMMENTS) && !stack.isEmpty()) {
			stack.pop();

		} else if (!line2.startsWith(COMMENTS) && stack.isEmpty()) {

			/**
			 * 
			 * All imports come in two form, they start with from or import the next few
			 * lines tracks them
			 */
			if (line2.trim().startsWith(IMPORTCOM)) {

				String[] packaging = line2.split(" ");

				if (packaging[1].trim().length() > 0) {
					packages.add(packaging[1]);
				}

			}

			if (line2.trim().startsWith("from")) {

				String[] packaging = line2.split(" ");

				if (packaging[1].trim().length() > 0) {
					packages.add(packaging[1]);
				}
			}

			if (!line2.isEmpty() && !line2.startsWith("#")) {

				lines++;

			}
		}

	}

}
