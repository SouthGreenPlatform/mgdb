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
package fr.cirad.mgdb.model.mongo.maintypes;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class Sequence.
 */
@Document(collection = "contigSeqs")
@TypeAlias("CSQ")
public class Sequence
{

	/** The Constant FIELDNAME_SEQUENCE. */
	public final static String FIELDNAME_SEQUENCE = "sq";

	/** The id. */
	@Id
	private String id;

	/** The sequence. */
	@Field(FIELDNAME_SEQUENCE)
	private String sequence;

	/**
	 * Instantiates a new sequence.
	 *
	 * @param id the id
	 * @param sequence the sequence
	 */
	public Sequence(String id, /*String reference,*/ String sequence) {
		super();
		this.id = id;
		this.sequence = sequence;
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
	 * Gets the sequence.
	 *
	 * @return the sequence
	 */
	public String getSequence() {
		return sequence;
	}

	/**
	 * Sets the sequence.
	 *
	 * @param sequence the new sequence
	 */
	public void setSequence(String sequence) {
		this.sequence = sequence;
	}
}
