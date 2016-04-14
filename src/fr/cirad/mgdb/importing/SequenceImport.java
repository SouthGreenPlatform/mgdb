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
package fr.cirad.mgdb.importing;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.biojava3.core.sequence.DNASequence;
import org.biojava3.core.sequence.compound.DNACompoundSet;
import org.biojava3.core.sequence.io.DNASequenceCreator;
import org.biojava3.core.sequence.io.FastaReader;
import org.biojava3.core.sequence.io.GenericFastaHeaderParser;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.Sequence;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class SequenceImport.
 */
public class SequenceImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(SequenceImport.class);

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length < 3)
			throw new Exception("You must pass 3 parameters as arguments: DATASOURCE name, FASTA reference-file, 3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)!");

		String sModule = args[0];

		GenericXmlApplicationContext ctx = null;
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		if (mongoTemplate == null)
		{	// we are probably being invoked offline
			try
			{
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
			}
			catch (BeanDefinitionStoreException fnfe)
			{
				LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
				ctx = new GenericXmlApplicationContext("applicationContext.xml");
			}

			MongoTemplateManager.initialize(ctx);
			mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
				throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
		}

		String seqCollName = MongoTemplateManager.getMongoCollectionName(Sequence.class);

		if ("2".equals(args[2]))
		{	// empty project's sequence data before importing
			if (mongoTemplate.collectionExists(seqCollName))
			{
				mongoTemplate.dropCollection(seqCollName);
				LOG.info("Collection " + seqCollName + " dropped.");
			}
		}
		else if ("0".equals(args[2]))
		{
			// do nothing
		}
		else
			throw new Exception("3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)");

		FastaReader fastaReader = new FastaReader(
				new File(args[1]),
				new GenericFastaHeaderParser(),
				new DNASequenceCreator(DNACompoundSet.getDNACompoundSet()));
		LinkedHashMap<String, DNASequence> sequences = fastaReader.process();

		LOG.info("Importing fasta file");
		int rowIndex = 0;
		for (Entry<String, DNASequence> entry : sequences.entrySet())
		{
			String sequenceId = entry.getValue().getOriginalHeader();
			DNASequence sequence = sequences.get(sequenceId);
			if (sequence == null)
				LOG.error("Unable to find sequence '" + sequenceId + "' in file ");
			else
				mongoTemplate.save(new Sequence(sequenceId, sequence.getSequenceAsString()), seqCollName);

			rowIndex++;
			if (rowIndex%1000 == 0)
				System.out.print(rowIndex + " ");
		}
		System.out.println();
		LOG.info(sequences.size() + " records added to collection " + seqCollName);

		ctx.close();
	}

}
