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
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.MappingStats;

// TODO: Auto-generated Javadoc
/**
 * The Class SequenceStats.
 */
@Document(collection = "contigStats")
@TypeAlias("CS")
public class SequenceStats
{
	
	/** The Constant FIELDNAME_SEQUENCE_LENGTH. */
	public final static String FIELDNAME_SEQUENCE_LENGTH = "cl";
	
	/** The Constant FIELDNAME_P4E_METHOD. */
	public final static String FIELDNAME_P4E_METHOD = "pm";
	
	/** The Constant FIELDNAME_CDS_START. */
	public final static String FIELDNAME_CDS_START = "cs";
	
	/** The Constant FIELDNAME_CDS_END. */
	public final static String FIELDNAME_CDS_END = "ce";
	
	/** The Constant FIELDNAME_FRAME. */
	public final static String FIELDNAME_FRAME = "fr";
	
	/** The Constant FIELDNAME_AVERAGE_RPKM. */
	public final static String FIELDNAME_AVERAGE_RPKM = "ar";
	
	/** The Constant FIELDNAME_PROJECT. */
	public final static String FIELDNAME_PROJECT = "pj";
	
	/** The id. */
	@Id
	private String id;
	
	/** The sequence length. */
	@Field(FIELDNAME_SEQUENCE_LENGTH)
	private long sequenceLength;
	
	/** The p4e method. */
	@Field(FIELDNAME_P4E_METHOD)
	private String p4eMethod;
	
	/** The cds start. */
	@Field(FIELDNAME_CDS_START)
	private Long cdsStart;
	
	/** The cds end. */
	@Field(FIELDNAME_CDS_END)
	private Long cdsEnd;
	
	/** The frame. */
	@Field(FIELDNAME_FRAME)
	private Byte frame;
	
	/** The average rpkm. */
	@Field(FIELDNAME_AVERAGE_RPKM)
	private Float averageRpkm;

	/** The project data. */
	@Field(FIELDNAME_PROJECT)
	private HashMap<String, List<MappingStats>> projectData = new HashMap<String, List<MappingStats>>();

	/**
	 * Instantiates a new sequence stats.
	 *
	 * @param id the id
	 * @param sequenceLength the sequence length
	 */
	public SequenceStats(String id, long sequenceLength) {
		super();
		this.id = id;
		this.sequenceLength = sequenceLength;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * Gets the sequence length.
	 *
	 * @return the sequence length
	 */
	public long getSequenceLength() {
		return sequenceLength;
	}

	/**
	 * Sets the sequence length.
	 *
	 * @param sequenceLength the new sequence length
	 */
	public void setSequenceLength(long sequenceLength) {
		this.sequenceLength = sequenceLength;
	}

	/**
	 * Gets the p4e method.
	 *
	 * @return the p4e method
	 */
	public String getP4eMethod() {
		return p4eMethod;
	}

	/**
	 * Sets the p4e method.
	 *
	 * @param p4eMethod the new p4e method
	 */
	public void setP4eMethod(String p4eMethod) {
		this.p4eMethod = p4eMethod;
	}

	/**
	 * Gets the cds start.
	 *
	 * @return the cds start
	 */
	public Long getCdsStart() {
		return cdsStart;
	}

	/**
	 * Sets the cds start.
	 *
	 * @param cdsStart the new cds start
	 */
	public void setCdsStart(Long cdsStart) {
		this.cdsStart = cdsStart;
	}

	/**
	 * Gets the cds end.
	 *
	 * @return the cds end
	 */
	public Long getCdsEnd() {
		return cdsEnd;
	}

	/**
	 * Sets the cds end.
	 *
	 * @param cdsEnd the new cds end
	 */
	public void setCdsEnd(Long cdsEnd) {
		this.cdsEnd = cdsEnd;
	}

	/**
	 * Gets the frame.
	 *
	 * @return the frame
	 */
	public Byte getFrame() {
		return frame;
	}

	/**
	 * Sets the frame.
	 *
	 * @param frame the new frame
	 */
	public void setFrame(Byte frame) {
		this.frame = frame;
	}

	/**
	 * Gets the average rpkm.
	 *
	 * @return the average rpkm
	 */
	public Float getAverageRpkm() {
		return averageRpkm;
	}

	/**
	 * Sets the average rpkm.
	 *
	 * @param averageRpkm the new average rpkm
	 */
	public void setAverageRpkm(Float averageRpkm) {
		this.averageRpkm = averageRpkm;
	}

	/**
	 * Gets the project data.
	 *
	 * @return the project data
	 */
	public HashMap<String, List<MappingStats>> getProjectData() {
		return projectData;
	}

	/**
	 * Sets the project data.
	 *
	 * @param projectData the project data
	 */
	public void setProjectData(HashMap<String, List<MappingStats>> projectData) {
		this.projectData = projectData;
	}	
	
	/**
	 * Calculate average rpkm.
	 *
	 * @param fUpdateField whether or not to update field
	 * @return the float
	 */
	public float calculateAverageRpkm(boolean fUpdateField)
	{
		float rpkmSum = 0;
		int sampleCount = 0;
		for (String prj : getProjectData().keySet())
		{
			for (MappingStats mp : getProjectData().get(prj))
				for (String sample : mp.getSampleInfo().keySet())
				{
					rpkmSum += Float.parseFloat((String) mp.getSampleInfo().get(sample).get(MappingStats.SAMPLE_INFO_FIELDNAME_RPKM));
					sampleCount++;
				}
		}
		float avgRpkm = rpkmSum/sampleCount;
		if (fUpdateField)
			setAverageRpkm(avgRpkm);
		return avgRpkm;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Sequence [id=" + id + ", sequenceLength=" + sequenceLength + "]";
	}
}