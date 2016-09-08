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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

// TODO: Auto-generated Javadoc
/**
 * The Class PopulationGroup.
 */
@Document(collection = "populationGroups")
@TypeAlias("PG")
public class PopulationGroup
{
	
	/** The Constant FIELDNAME_NAME. */
	public final static String FIELDNAME_NAME = "nm";

	/** The id. */
	@Id
	private String id;

	/** The name. */
	@Field(FIELDNAME_NAME)
	private String name;	

	/**
	 * Instantiates a new population group.
	 *
	 * @param id the id
	 */
	public PopulationGroup(String id) {
		this.id = id;
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	
//	public void setId(String id) {
//		this.id = id;
//	}

	/**
 * Gets the name.
 *
 * @return the name
 */
public String getName() {
		return name;
	}

	/**
	 * Sets the name.
	 *
	 * @param name the new name
	 */
	public void setName(String name) {
		this.name = name;
	}
}
