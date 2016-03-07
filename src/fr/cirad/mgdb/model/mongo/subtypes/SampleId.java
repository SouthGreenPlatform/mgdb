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

// TODO: Auto-generated Javadoc
/**
 * The Class SampleId.
 */
public class SampleId /*implements java.io.Serializable, IGenotypingSampleIndex*/ {

	/** The project. */
	private Integer project;
	
	/** The sample index. */
	private Integer sampleIndex;

	/**
	 * Instantiates a new sample id.
	 */
	private SampleId() {
	}

	/**
	 * Instantiates a new sample id.
	 *
	 * @param project the project
	 * @param sampleIndex the sample index
	 */
	public SampleId(Integer project, Integer sampleIndex) {
		this.project = project;
		this.sampleIndex = sampleIndex;
	}

	/**
	 * Gets the project.
	 *
	 * @return the project
	 */
	public Integer getProject() {
		return this.project;
	}

	/**
	 * Sets the project.
	 *
	 * @param project the new project
	 */
	public void setProject(Integer project) {
		this.project = project;
	}

	/**
	 * Gets the sample index.
	 *
	 * @return the sample index
	 */
	public Integer getSampleIndex() {
		return this.sampleIndex;
	}

	/**
	 * Sets the sample index.
	 *
	 * @param sampleIndex the new sample index
	 */
	public void setSampleIndex(Integer sampleIndex) {
		this.sampleIndex = sampleIndex;
	}

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof SampleId))
			return false;
		
		boolean f1 = ((SampleId)o).getSampleIndex() == getSampleIndex() || (getSampleIndex() != null && getSampleIndex().equals(((SampleId)o).getSampleIndex()));
		boolean f2 = ((SampleId)o).getProject() == getProject() || (getProject() != null && getProject().equals(((SampleId)o).getProject()));
		return f2 && f1;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return getProject() + "¤" + getSampleIndex();
	}
}
