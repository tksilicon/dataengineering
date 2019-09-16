package com.turing.dataengineering.model;




import lombok.Data;

@Data
public class DataEnginnerModel {
	
	
	String repository_url;
	Integer numberOfLines;
	String libraries;
	Double nestingFactor;
	float codeDuplication;
	Double averageParameters;
	Double averageVariables;
	
	
	public DataEnginnerModel() {
		super();
	}
	

	
	
}
