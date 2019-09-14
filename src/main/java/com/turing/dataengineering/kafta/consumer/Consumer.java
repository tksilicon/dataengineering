package com.turing.dataengineering.kafta.consumer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.turing.dataengineering.model.DataEnginnerModel;
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
	private static final String DATASET = System.getProperty("user.dir")+ "/dataset/";
	private static final String JSONT = ".json";
	private static final String TEMPWORKDIR = "src/main/resources/tempwork";
	private static final String GROUPID = "group_id";
	private static final String TOPIC = "users";

	Map<String, DataEnginnerModel> results = new LinkedHashMap<>();

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

	@KafkaListener(topics = TOPIC, groupId = GROUPID)
	public void consume(String message) throws IOException {
		logger.info("Consuming message");
		logger.info(message);

		String[] checkFileExists = message.split("/");

		if (!results.containsKey(checkFileExists[checkFileExists.length - 1])) {

			jgit.cloneGit(message, checkFileExists[checkFileExists.length - 1]);

			DataEnginnerModel map = getAnalysis(jgit.searchRepository(checkFileExists[checkFileExists.length - 1]),
					message);
			results.put(checkFileExists[checkFileExists.length - 1], map);

			objectMapper = new ObjectMapper();
			File fileT = new File(DATASET + "resultsTemp" + JSONT);
			FileUtils.writeStringToFile(fileT, objectMapper.writeValueAsString(map) + ",\n", StandardCharsets.UTF_8,
					true);
			
			
			

			/**
			 * Clean up When we are done processing a messages the tempwork directory is
			 * deleted to avoid keep heavy files in the file system
			 * 
			 */

			FileUtils.deleteDirectory(new File(TEMPWORKDIR));
		}

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
	private DataEnginnerModel getAnalysis(List<String> filesFound, String repo) throws IOException {

		logger.info("Analysis logic running");

		Iterator<String> iterator = filesFound.iterator();

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
					} else if (line2.indexOf("for") > index) {
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

		return dataEnginnerModel;

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
