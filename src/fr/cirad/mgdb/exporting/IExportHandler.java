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
/*
 *
 */
package fr.cirad.mgdb.exporting;

import java.util.List;

/**
 * The Interface IExportHandler.
 */
public interface IExportHandler
{

	/** The Constant nMaxChunkSizeInMb. */
	static final int nMaxChunkSizeInMb = 3;

	/** The Constant LINE_SEPARATOR. */
	static final String LINE_SEPARATOR = "\n";

	/**
	 * Gets the export format name.
	 *
	 * @return the export format name
	 */
	public String getExportFormatName();

	/**
	 * Gets the export format description.
	 *
	 * @return the export format description
	 */
	public String getExportFormatDescription();

	/**
	 * Gets the step list.
	 *
	 * @return the step list
	 */
	public List<String> getStepList();

	/**
	 * Gets the supported variant types.
	 *
	 * @return the supported variant types
	 */
	public List<String> getSupportedVariantTypes();
}
