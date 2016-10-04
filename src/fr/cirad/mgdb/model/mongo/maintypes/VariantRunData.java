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
package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.HashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;

// TODO: Auto-generated Javadoc
/**
 * The Class VariantRunData.
 */
@Document(collection = "variantRunData")
@TypeAlias("R")
@CompoundIndexes({
    @CompoundIndex(def = "{'_id." + VariantRunData.VariantRunDataId.FIELDNAME_VARIANT_ID + "': 1, '_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID + "': 1}")
})
public class VariantRunData
{
	
	/** The Constant FIELDNAME_SAMPLEGENOTYPES. */
	public final static String FIELDNAME_SAMPLEGENOTYPES = "sp";
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME = "EFF_nm";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE = "EFF_ge";

	/**
	 * The Class VariantRunDataId.
	 */
	static public class VariantRunDataId
	{
		
		/** The Constant FIELDNAME_PROJECT_ID. */
		public final static String FIELDNAME_PROJECT_ID = "pi";
		
		/** The Constant FIELDNAME_RUNNAME. */
		public final static String FIELDNAME_RUNNAME = "rn";
		
		/** The Constant FIELDNAME_VARIANT_ID. */
		public final static String FIELDNAME_VARIANT_ID = "vi";

		/** The project id. */
		@Field(FIELDNAME_PROJECT_ID)
		private int projectId;

		/** The run name. */
		@Field(FIELDNAME_RUNNAME)
		private String runName;

		/** The variant id. */
		@Field(FIELDNAME_VARIANT_ID)
		private Comparable variantId;

		/**
		 * Instantiates a new variant run data id.
		 *
		 * @param projectId the project id
		 * @param runName the run name
		 * @param variantId the variant id
		 */
		public VariantRunDataId(int projectId, String runName, Comparable variantId) {
			this.projectId = projectId;
			this.runName = runName.intern();
			this.variantId = variantId;
		}

		/**
		 * Gets the project id.
		 *
		 * @return the project id
		 */
		public int getProjectId() {
			return projectId;
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
		 * Gets the variant id.
		 *
		 * @return the variant id
		 */
		public Comparable getVariantId() {
			return variantId;
		}
	}

	/** The id. */
	@Id
	private VariantRunDataId id;

	/** The sample genotypes. */
	@Field(FIELDNAME_SAMPLEGENOTYPES)
	private HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<Integer, SampleGenotype>();

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Object> additionalInfo = null;

	/**
	 * Instantiates a new variant run data.
	 */
	public VariantRunData() {
	}

	/**
	 * Instantiates a new variant run data.
	 *
	 * @param id the id
	 */
	public VariantRunData(VariantRunDataId id) {
		this.id = id;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public VariantRunDataId getId() {
		return id;
	}

	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(VariantRunDataId id) {
		this.id = id;
	}

	/**
	 * Gets the run name.
	 *
	 * @return the run name
	 */
	public String getRunName() {
		return getId().getRunName();
	}

//	public TreeSet<Integer> getSortedSampleSet() {
//		TreeSet<Integer> result = new TreeSet<Integer>(/*new AlphaNumericStringComparator()*/);
//		for (Integer sampleIndex : sampleGenotypes.keySet())
//			result.add(sampleIndex);
//		return result;
//	}

	/**
 * Gets the sample genotypes.
 *
 * @return the sample genotypes
 */
public HashMap<Integer, SampleGenotype> getSampleGenotypes() {
		return sampleGenotypes;
	}

	/**
	 * Sets the sample genotypes.
	 *
	 * @param genotypes the genotypes
	 */
	public void setSampleGenotypes(HashMap<Integer, SampleGenotype> genotypes) {
		this.sampleGenotypes = genotypes;
	}

	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Object> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new HashMap<>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(HashMap<String, Object> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
        @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof Individual))
			return false;
		
		return getId().getProjectId() == ((VariantRunData)o).getId().getProjectId() && getId().getRunName().equals(((VariantRunData)o).getId().getRunName()) && getId().getVariantId() == ((VariantRunData)o).getId().getVariantId();
	}
}
