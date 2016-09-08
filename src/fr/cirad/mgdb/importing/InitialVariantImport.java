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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class VariantSynonymImport.
 */
public class InitialVariantImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(InitialVariantImport.class);
	
	/** The Constant twoDecimalNF. */
	static private final NumberFormat twoDecimalNF = NumberFormat.getInstance();

	static
	{
		twoDecimalNF.setMaximumFractionDigits(2);
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
		insertVariantsAndSynonyms(args);
	}

	/**
	 * Insert reference positions.
	 *
	 * @param args the args
	 * @throws Exception the exception
	 */
	public static void insertVariantsAndSynonyms(String[] args) throws Exception
	{
		if (args.length < 2)
			throw new Exception("You must pass 2 parameters as arguments: DATASOURCE name, exhaustive variant list TSV file. This TSV file is expected to be formatted as follows: widde_id, chr:pos, colon-separated list of containing chips, zero or more colon-separated lists of synonyms (their type being defined in the header)");

		File chipInfoFile = new File(args[1]);
		if (!chipInfoFile.exists() || chipInfoFile.isDirectory())
			throw new Exception("Data file does not exist: " + chipInfoFile.getAbsolutePath());
		
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(args[0]);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(args[0]);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + args[0] + "' is not supported!");
			}

			if (mongoTemplate.count(null, VariantData.class) > 0)
				throw new Exception("There are already some variants in this database!");
			
			long before = System.currentTimeMillis();

			BufferedReader in = new BufferedReader(new FileReader(chipInfoFile));
			try
			{
				String sLine = in.readLine();	// read header
				if (sLine != null)
					sLine = sLine.trim();
				List<String> header = splitByComaSpaceOrTab(sLine);
				sLine = in.readLine();
				if (sLine != null)
					sLine = sLine.trim();
				
				long count = 0;
				int nNumberOfVariantsToSaveAtOnce = 20000;
				ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
				
				do
				{
					if (sLine.length() > 0)
					{
						List<String> cells = Helper.split(sLine, "\t");
						VariantData variant = new VariantData(cells.get(0));
						variant.setType(cells.get(1));
						String[] seqAndPos = cells.get(2).split(":");
						if (!seqAndPos[0].equals("0"))
							variant.setReferencePosition(new ReferencePosition(seqAndPos[0], Long.parseLong(seqAndPos[1])));
						if (cells.size() == 3)
							continue;
						
						String chipList = cells.get(3);
						if (chipList.length() > 0)
						{
							List<String> analysisMethods = new ArrayList<String>();
								for (String chip : chipList.split(";"))
									analysisMethods.add(chip);
										variant.setAnalysisMethods(analysisMethods);
						}

						for (int i=3; i<header.size(); i++)
						{
							String syns = cells.get(i);
							if (syns.length() > 0)
							{
								TreeSet<Comparable> synSet = new TreeSet<Comparable>();
								for (String syn : syns.split(";"))
									synSet.add(syn);
								variant.getSynonyms().put(header.get(i), synSet);
							}
						}
						unsavedVariants.add(variant);
												
						if (count % nNumberOfVariantsToSaveAtOnce == 0)
						{
							mongoTemplate.insert(unsavedVariants, VariantData.class);
							unsavedVariants.clear();
							if (count > 0)
							{
								String info = count + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
								LOG.debug(info);
							}
						}
						
						count++;
					}
					sLine = in.readLine();
					if (sLine != null)
						sLine = sLine.trim();
				}
				while (sLine != null);
				
				if (unsavedVariants.size() > 0)
				{
					mongoTemplate.insert(unsavedVariants, VariantData.class);
					unsavedVariants.clear();
				}
				LOG.info("InitialVariantImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
			}
			finally
			{
				in.close();			
			}
		}
		finally
		{
			if (ctx != null)
				ctx.close();
		}
	}

	/**
	 * Split by coma space or tab.
	 *
	 * @param s the s
	 * @return the list
	 */
	private static List<String> splitByComaSpaceOrTab(String s)
	{
		return MgdbDao.split(s, s.contains(",") ? "," : (s.contains(" ") ? " " : "\t"));
	}
}