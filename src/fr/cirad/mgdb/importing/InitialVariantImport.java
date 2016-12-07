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
//import java.io.FileWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
//import java.util.Scanner;
import java.util.TreeSet;

//import org.apache.commons.lang.StringUtils;
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
		/* Insert chip list from external file */
//		HashMap<String, HashSet<String>> illuminaIdToChipMap = new HashMap<String, HashSet<String>>();
//		Scanner sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/D/data/intertryp/SNPchimp_exhaustive_with_rs.tsv"));
//		sc.nextLine();
//		while (sc.hasNextLine())
//		{
//			String[] splittedLine = sc.nextLine().split("\t");
//			HashSet<String> chipsForId = illuminaIdToChipMap.get(splittedLine[4]);
//			if (chipsForId == null)
//			{
//				chipsForId = new HashSet<String>();
//				illuminaIdToChipMap.put(splittedLine[4], chipsForId);
//			}
//			chipsForId.add(splittedLine[0]);
//		}
//		
//		sc.close();
//		sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/D/data/intertryp/bos_ExhaustiveSnpList_StandardFormat.tsv"));
//		FileWriter fw = new FileWriter("/media/sempere/Seagate Expansion Drive/D/data/intertryp/bos_ExhaustiveSnpList_StandardFormat_wChips.tsv");
//		fw.write(sc.nextLine() + "\tchip\n");
//		while (sc.hasNextLine())
//		{
//			String sLine = sc.nextLine();
//			String[] splittedLine = sLine.split("\t");
//			HashSet<String> chipsForId = illuminaIdToChipMap.get(splittedLine[2].split(";")[0]);
//			fw.write(sLine + "\t" + StringUtils.join(chipsForId, ";") + "\n");
//		}
//		sc.close();
//		fw.close();
		
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
			throw new Exception("You must pass 2 parameters as arguments: DATASOURCE name, exhaustive variant list TSV file. This TSV file is expected to be formatted as follows: id, chr:pos, colon-separated list of containing chips, zero or more colon-separated lists of synonyms (their type being defined in the header)");

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
				List<String> fieldsExceptSynonyms = Arrays.asList(new String[] {"id", "type", "pos", "chip"}); 
				do
				{
					if (sLine.length() > 0)
					{
						List<String> cells = Helper.split(sLine, "\t");
						VariantData variant = new VariantData(cells.get(header.indexOf("id")));
						variant.setType(cells.get(header.indexOf("type")));
						String[] seqAndPos = cells.get(header.indexOf("pos")).split(":");
						if (!seqAndPos[0].equals("0"))
							variant.setReferencePosition(new ReferencePosition(seqAndPos[0], Long.parseLong(seqAndPos[1])));
						
						String chipList = cells.get(header.indexOf("chip"));
						if (chipList.length() > 0)
						{
							List<String> analysisMethods = new ArrayList<String>();
								for (String chip : chipList.split(";"))
									analysisMethods.add(chip);
										variant.setAnalysisMethods(analysisMethods);
						}

						for (int i=0; i<header.size(); i++)
						{
							if (fieldsExceptSynonyms.contains(header.get(i)))
								continue;

							String syns = cells.get(i);
							if (syns.length() > 0)
							{
								TreeSet<Comparable> synSet = new TreeSet<Comparable>();
								for (String syn : syns.split(";"))
									if (!syn.equals("."))
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