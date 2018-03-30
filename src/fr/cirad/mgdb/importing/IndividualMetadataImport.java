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
package fr.cirad.mgdb.importing;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BulkWriteResult;

import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

// TODO: Auto-generated Javadoc
/**
 * The Class IndividualMetadataImport.
 */
public class IndividualMetadataImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(IndividualMetadataImport.class);

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3)
			throw new Exception("You must pass 3 or 4 parameters as arguments: DATASOURCE name, metadata file path (TSV format with header on first line), label of column containing individual names (matching those in the DB), and optionally a CSV list of column labels for fields to import (all will be imported if no such parameter is supplied).");
		insertIndividualMetadata(args[0], new File(args[1]).toURI().toURL(), args[2], args.length > 3 ? args[2] : null);
	}

	public static void insertIndividualMetadata(String sModule, URL metadataFileURL, String individualColName, String csvFieldListToImport) throws Exception
	{
		List<String> fieldsToImport = csvFieldListToImport != null ? Arrays.asList(csvFieldListToImport.toLowerCase().split(",")) : null;
		Scanner scanner = new Scanner(metadataFileURL.openStream());

		GenericXmlApplicationContext ctx = null;
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		if (mongoTemplate == null) { // we are probably being invoked offline
			ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

			MongoTemplateManager.initialize(ctx);
			mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
				throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
		}

		try
		{
			HashMap<Integer, String> columnLabels = new HashMap<Integer, String>();
			int idColumn = -1;
			String sLine = null;
			BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Individual.class);
			List<String> passedIndList = new ArrayList<>();
			while (scanner.hasNextLine()) {
				sLine = scanner.nextLine().trim();
				if (sLine.length() == 0)
					continue;

				List<String> cells = Helper.split(sLine, "\t");

				if (columnLabels.isEmpty()) { // it's the header line
					for (int i=0; i<cells.size(); i++)
					{
						String cell = cells.get(i);
						if (cell.equalsIgnoreCase(individualColName))
							idColumn = i;
						else if (fieldsToImport == null || fieldsToImport.contains(cell.toLowerCase()))
							columnLabels.put(i, cell);
					}
					if (idColumn == -1)
						throw new Exception("Unable to find individual name column \"" + individualColName + "\" in file header!");
					if (columnLabels.size() == 0)
						throw new Exception("Unable to find any columns to import in file header!");

					continue;
				}

				// now deal with actual data rows
				HashMap<String, Comparable> additionalInfo = new HashMap<>();
				for (int col : columnLabels.keySet())
					if (col != idColumn)
						additionalInfo.put(columnLabels.get(col), cells.get(col));

				String individualId = cells.get(idColumn);
				passedIndList.add(individualId);
				bulkOperations.updateMulti(new Query(Criteria.where("_id").is(individualId)), new Update().set(Individual.SECTION_ADDITIONAL_INFO, additionalInfo));
			}
			
			// first check if any passed individuals are unknown
			Query verificationQuery = new Query(Criteria.where("_id").in(passedIndList));
			verificationQuery.fields().include("_id");
			List<String> foundIndList = mongoTemplate.find(verificationQuery, Individual.class).stream().map(ind -> ind.getId()).collect(Collectors.toList());
			if (foundIndList.size() < passedIndList.size())
				throw new Exception("The following individuals do not exist in the selected database: " + StringUtils.join(CollectionUtils.disjunction(passedIndList, foundIndList), ", "));
			
			BulkWriteResult bwr = bulkOperations.execute();
			LOG.info(bwr.getModifiedCount() + " individuals updated with metadata, out of " + bwr.getMatchedCount() + " matched documents");
		}
		finally
		{
			scanner.close();
			if (ctx != null)
				ctx.close();
		}
	}
}
