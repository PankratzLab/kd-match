package org.pankratzlab.kdmatch;

import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Stream;

import org.pankratzlab.internal.gwas.FactorLoadings;

public class Sample {
	/**
	 * @param iD
	 * @param dim
	 */
	public Sample(String iD, double[] dim) {
		super();
		ID = iD;
		this.dim = dim;
	}

	public Sample(String iD, double[] dim, int status, String group) {
		super();
		ID = iD;
		this.dim = dim;
		this.status = status;
		this.group = group;
	}
	
	public static Sample parseSample(String[] sampleLine, int idCol, int[] numericColumnsToUseForClustering,
			int[] factorColumnsToAssignGroup, FactorLoadings factorLoadings) {
		String group = "";
		String id = sampleLine[idCol];
		int status = Integer.parseInt(sampleLine[idCol+1]);
		double[] dim = new double[numericColumnsToUseForClustering.length];
		for (int i = 0; i < dim.length; i++) {
			//TODO improve: This isn't great - I think it requires the factors input argument from user to be in file column order
			dim[i] = Double.parseDouble(sampleLine[numericColumnsToUseForClustering[i]]) * factorLoadings.getDoubleLoadings().get(i);
		}
		for (int i = 0; i < factorColumnsToAssignGroup.length; i++) {
			group += (sampleLine[factorColumnsToAssignGroup[i]] + "_");
		}
		return new Sample(id, dim, status, group);
	}

	public boolean isValidCaseOrControl() {
		return (this.isCase() || this.isControl());
	}

	public boolean isCase() {
		return status == 1;
	}

	public boolean isControl() {
		return status == 0;
	}

	public String getGroup() {
		return group;
	}

	/**
	 * @return the iD
	 */
	String getID() {
		return ID;
	}

	String ID;
	// data for this sample (e.g holds PC1-10)
	double[] dim;
	// case (1) control (0) status
	int status;
	// stores group for forced matching
	String group;

	String getOutput() {
		StringJoiner j = new StringJoiner("\t");
		j.add(ID);
		for (int i = 0; i < dim.length; i++) {
			j.add(Double.toString(dim[i]));
		}
		return j.toString();
	}
}
