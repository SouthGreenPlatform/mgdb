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
 * See <http://www.gnu.org/licenses/gpl-3.0.html> for details about
 * GNU Affero General Public License V3.
 *******************************************************************************/
package fr.cirad.mgdb.model.mongo.subtypes;

import java.util.HashMap;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class MappingStats.
 */
@Document
@TypeAlias("MS")
public class MappingStats
{

	/** The Constant RUNNAME. */
	public final static String RUNNAME = "rn";

	/** The Constant SAMPLE_INFO. */
	public final static String SAMPLE_INFO = "sp";

	/** The Constant MAPPING_LENGTH. */
	public final static String MAPPING_LENGTH = "ml";

	/** The Constant SAMPLE_INFO_FIELDNAME_RPKM. */
	public final static String SAMPLE_INFO_FIELDNAME_RPKM = "rp";

	/** The run name. */
	@Field(RUNNAME)
	private String runName;

	/** The mapping length. */
	@Field(MAPPING_LENGTH)
	private Long mappingLength;

	/** The sample info. */
	@Field(SAMPLE_INFO)
	private HashMap<String/*sample*/, HashMap<String/*field*/, Comparable/*value*/>> sampleInfo = new HashMap<String, HashMap<String, Comparable>>();

	/**
	 * Instantiates a new mapping stats.
	 *
	 * @param runName the run name
	 * @param sampleInfo the sample info
	 */
	public MappingStats(String runName, HashMap<String, HashMap<String, Comparable>> sampleInfo) {
		super();
		this.runName = runName;
		this.sampleInfo = sampleInfo;
	}

	/**
	 * Gets the run name.
	 *
	 * @return the run name
	 */
	public String getRunName() {
		return runName;
	}

	/**
	 * Sets the run name.
	 *
	 * @param runName the new run name
	 */
	public void setRunName(String runName) {
		this.runName = runName;
	}

	/**
	 * Gets the mapping length.
	 *
	 * @return the mapping length
	 */
	public Long getMappingLength() {
		return mappingLength;
	}

	/**
	 * Sets the mapping length.
	 *
	 * @param mappingLength the new mapping length
	 */
	public void setMappingLength(Long mappingLength) {
		this.mappingLength = mappingLength;
	}

	/**
	 * Gets the sample info.
	 *
	 * @return the sample info
	 */
	public HashMap<String, HashMap<String, Comparable>> getSampleInfo() {
		return sampleInfo;
	}

	/**
	 * Sets the sample info.
	 *
	 * @param sampleInfo the sample info
	 */
	public void setSampleInfo(HashMap<String, HashMap<String, Comparable>> sampleInfo) {
		this.sampleInfo = sampleInfo;
	}
}

