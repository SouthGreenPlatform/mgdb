/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 <South Green>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See <http://www.gnu.org/licenses/gpl-3.0.html> for details about
 * GNU General Public License V3.
 *******************************************************************************/
package fr.cirad.mgdb.model.mongo.subtypes;

import java.util.HashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

//@Document(collection = "sampleGenotypes")
/**
 * The Class SampleGenotype.
 */
//@TypeAlias("G")
public class SampleGenotype
{

	/** The Constant FIELDNAME_GENOTYPECODE. */
	public final static String FIELDNAME_GENOTYPECODE = "gt";

	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";

//	static public class SampleGenotypeId
//	{
//		public final static String FIELDNAME_PROJECT_ID = "pi";
//		public final static String FIELDNAME_RUNNAME = "rn";
//		public final static String FIELDNAME_VARIANT_ID = "vi";
//		public final static String FIELDNAME_SAMPLE_INDEX = "si";
//
//		@Field(FIELDNAME_PROJECT_ID)
//		private int projectId;
//
//		@Field(FIELDNAME_RUNNAME)
//		private String runName;
//
//		@Field(FIELDNAME_VARIANT_ID)
//		private Comparable variantId;
//
//		@Field(FIELDNAME_SAMPLE_INDEX)
//		private int sampleIndex;
//
//		public SampleGenotypeId(int projectId, String runName, Comparable variantId, int sampleIndex) {
//			this.projectId = projectId;
//			this.runName = runName.intern();
//			this.variantId = variantId;
//			this.sampleIndex = sampleIndex;
//		}
//
//		public SampleGenotypeId() {
//		}
//
//		public int getProjectId() {
//			return projectId;
//		}
//
//		public String getRunName() {
//			return runName;
//		}
//
//		public Comparable getVariantId() {
//			return variantId;
//		}
//	}
//
//	@Id
//	private SampleGenotypeId id;

	/** The code. */
@Field(FIELDNAME_GENOTYPECODE)
	private String code;

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Comparable> additionalInfo = null;

	/**
	 * Instantiates a new sample genotype.
	 *
	 * @param code the code
	 */
	public SampleGenotype(String code) {
		super();
		this.code = code.intern();
	}

//	public SampleGenotype(SampleGenotypeId sampleGenotypeId) {
//		super();
//		this.id = sampleGenotypeId;
//	}

	/**
 * Gets the code.
 *
 * @return the code
 */
public String getCode() {
		return code;
	}

	/**
	 * Sets the code.
	 *
	 * @param code the new code
	 */
	public void setCode(String code) {
		this.code = code.intern();
	}

	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Comparable> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new HashMap<String, Comparable>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(HashMap<String, Comparable> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
}
