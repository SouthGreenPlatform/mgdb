/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 <CIRAD>
 *     
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about
 * GNU Affero General Public License V3.
 *******************************************************************************/
package fr.cirad.mgdb.model.mongo.subtypes;

import org.springframework.data.mongodb.core.mapping.Field;

// TODO: Auto-generated Javadoc
/**
 * The Class GenotypingSample.
 */
public class GenotypingSample
{
	
	/** The Constant FIELDNAME_INDIVIDUAL. */
	public final static String FIELDNAME_INDIVIDUAL = "in";
	
	/** The Constant FIELDNAME_PROBLEM. */
	public final static String FIELDNAME_PROBLEM = "pb";
	
	/** The individual. */
	@Field(FIELDNAME_INDIVIDUAL)
	private String individual;
	
	/** The problem. */
	@Field(FIELDNAME_PROBLEM)
	private String problem;

	/**
	 * Instantiates a new genotyping sample.
	 *
	 * @param individual the individual
	 */
	public GenotypingSample(String individual) {
		super();
		this.individual = individual.intern();
	}
	
	/**
	 * Gets the individual.
	 *
	 * @return the individual
	 */
	public String getIndividual() {
		return individual;
	}
	
	/**
	 * Sets the individual.
	 *
	 * @param individual the new individual
	 */
	public void setIndividual(String individual) {
		this.individual = individual.intern();
	}
	
	/**
	 * Gets the problem.
	 *
	 * @return the problem
	 */
	public String getProblem() {
		return problem;
	}
	
	/**
	 * Checks if is problematic.
	 *
	 * @return true, if is problematic
	 */
	public boolean isProblematic() {
		return problem != null;
	}

	/**
	 * Sets the problem.
	 *
	 * @param problem the new problem
	 */
	public void setProblem(String problem) {
		this.problem = problem;
	}
}
