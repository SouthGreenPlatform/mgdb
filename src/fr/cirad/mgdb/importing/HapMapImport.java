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

import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.VariantContext.Type;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapCodec;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapFeature;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.WriteResult;

import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

// TODO: Auto-generated Javadoc
/**
 * The Class HapMapImport.
 */
public class HapMapImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	private String m_processID;

	/**
	 * Instantiates a new hap map import.
	 */
	public HapMapImport()
	{
	}

	/**
	 * Instantiates a new hap map import.
	 *
	 * @param processID the process id
	 */
	public HapMapImport(String processID)
	{
		m_processID = processID;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, and HapMap file! An optional 6th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		File mainFile = new File(args[4]);
		if (!mainFile.exists() || mainFile.length() == 0)
			throw new Exception("File " + args[4] + " is missing or empty!");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new HapMapImport().importToMongo(args[0], args[1], args[2], args[3], args[4], mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param mainFilePath the main file path
	 * @param importMode the import mode
	 * @throws Exception the exception
	 */
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String mainFilePath, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		ProgressIndicator.registerProgressIndicator(progress);
		progress.setPercentageEnabled(false);		

		FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFilePath, new RawHapMapCodec(), false);
		GenericXmlApplicationContext ctx = null;
		try
		{
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

			if (m_processID == null)
				m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);

			if (importMode == 2) // drop database before importing
				mongoTemplate.getDb().dropDatabase();
			else if (project != null)
			{
				if (importMode == 1) // empty project data before importing
				{
					WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId())), DBVCFHeader.class);
					LOG.info(wr.getN() + " records removed from vcf_header");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantRunData.class);
					LOG.info(wr.getN() + " records removed from variantRunData");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
					project.getRuns().clear();
				}
				else // empty run data before importing
				{
                    WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId()).and("_id." + VcfHeaderId.FIELDNAME_RUN).is(sRun)), DBVCFHeader.class);
					LOG.info(wr.getN() + " records removed from vcf_header");
					List<Criteria> crits = new ArrayList<Criteria>();
					crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
					crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(sRun));
					crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
					wr = mongoTemplate.remove(new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))), VariantRunData.class);
					LOG.info(wr.getN() + " records removed from variantRunData");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);	// we are going to re-write it
				}
//				if (mongoTemplate.count(null, VariantRunData.class) == 0)
//					mongoTemplate.getDb().dropDatabase(); // if there is no genotyping data then any other data is irrelevant
			}

			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(2 /* Sequencing */);
				project.setTechnology(sTechnology);
			}

			long beforeReadingAllVariants = System.currentTimeMillis();
			// First build a list of all variants that exist in the database: finding them by ID is by far most efficient
			HashMap<String, Comparable> existingVariantIDs = new HashMap<String, Comparable>();
			Query query = new Query();
			query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE);
			Iterator<VariantData> variantIterator = mongoTemplate.find(query, VariantData.class).iterator();
			while (variantIterator.hasNext())
			{
				VariantData vd = variantIterator.next();
				ReferencePosition chrPos = vd.getReferencePosition();
				String variantDescForPos = vd.getType() + "::" + chrPos.getSequence() + "::" + chrPos.getStartSite();
				existingVariantIDs.put(variantDescForPos, vd.getId());
			}
			if (existingVariantIDs.size() > 0)
			{
				String info = existingVariantIDs.size() + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s";
				LOG.info(info);
				progress.addStep(info);
				progress.moveToNextStep();
			}
			
			if (!project.getVariantTypes().contains(Type.SNP.toString()))
				project.getVariantTypes().add(Type.SNP.toString());

			// loop over each variation
			long count = 0;
			int nNumberOfVariantsToSaveAtOnce = 1;
			ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
			ArrayList<VariantRunData> unsavedRuns = new ArrayList<VariantRunData>();
			HashMap<String /*individual*/, SampleId> previouslyCreatedSamples = new HashMap<String /*individual*/, SampleId>();
			Iterator<RawHapMapFeature> it = reader.iterator();
			progress.addStep("Processing variant lines");
			progress.moveToNextStep();
			while (it.hasNext())
			{
				RawHapMapFeature hmFeature = it.next();               
				String variantDescForPos = Type.SNP.toString() + "::" + hmFeature.getChr() + "::" + hmFeature.getStart();
				try
				{
					Comparable variantId = existingVariantIDs.get(variantDescForPos);
					VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
					if (variant == null)
						variant = new VariantData(hmFeature.getName() != null && hmFeature.getName().length() > 0 ? hmFeature.getName() : new ObjectId());
					unsavedVariants.add(variant);
					VariantRunData runToSave = addHapMapDataToVariant(mongoTemplate, variant, hmFeature, project, sRun, previouslyCreatedSamples);
					if (!unsavedRuns.contains(runToSave))
							unsavedRuns.add(runToSave);

					if (count == 0)
					{
						nNumberOfVariantsToSaveAtOnce = Math.max(1, 30000 / hmFeature.getSampleIDs().length);
						LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
					}
					if (count % nNumberOfVariantsToSaveAtOnce == 0)
					{
						if (existingVariantIDs.size() == 0)
						{	// we benefit from the fact that it's the first variant import into this database to use bulk insert which is meant to be faster
							mongoTemplate.insert(unsavedVariants, VariantData.class);
							mongoTemplate.insert(unsavedRuns, VariantRunData.class);
						}
						else
						{
							for (VariantData vd : unsavedVariants)
								mongoTemplate.save(vd);
							for (VariantRunData run : unsavedRuns)
								mongoTemplate.save(run);
						}
						unsavedVariants.clear();
						unsavedRuns.clear();

						progress.setCurrentStepProgress(count);
						if (count > 0)
						{
							String info = count + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
							LOG.debug(info);
						}
					}

					int ploidy = 2;	// the only one supported by HapMap format 
					if (project.getPloidyLevel() < ploidy)
						project.setPloidyLevel(ploidy);

					if (!project.getSequences().contains(hmFeature.getChr()))
						project.getSequences().add(hmFeature.getChr());

					int alleleCount = hmFeature.getAlleles().length;
					project.getAlleleCounts().add(alleleCount);	// it's a TreeSet so it will only be added if it's not already present

					count++;
				}
				catch (Exception e)
				{
					throw new Exception("Error occured importing variant number " + (count + 1) + " (" + variantDescForPos + ")", e);
				}
			}
			reader.close();

			if (existingVariantIDs.size() == 0)
			{	// we benefit from the fact that it's the first variant import into this database to use bulk insert which is meant to be faster
				mongoTemplate.insert(unsavedVariants, VariantData.class);
				mongoTemplate.insert(unsavedRuns, VariantRunData.class);
			}
			else
			{
				for (VariantData vd : unsavedVariants)
					mongoTemplate.save(vd);
				for (VariantRunData run : unsavedRuns)
					mongoTemplate.save(run);							
			}

			// save project data
			if (!project.getRuns().contains(sRun))
				project.getRuns().add(sRun);
			mongoTemplate.save(project);

			LOG.info("HapMapImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");

			progress.addStep("Preparing database for searches");
			progress.moveToNextStep();
			MgdbDao.prepareDatabaseForSearches(mongoTemplate);
			progress.markAsComplete();
		}
		finally
		{
			if (ctx != null)
				ctx.close();

			reader.close();
		}
	}

	/**
	 * Adds the hap map data to variant.
	 *
	 * @param mongoTemplate the mongo template
	 * @param variantToFeed the variant to feed
	 * @param hmFeature the hm feature
	 * @param project the project
	 * @param runName the run name
	 * @param usedSamples the used samples
	 * @return the variant run data
	 * @throws Exception the exception
	 */
	static private VariantRunData addHapMapDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, RawHapMapFeature hmFeature, GenotypingProject project, String runName, Map<String /*individual*/, SampleId> usedSamples) throws Exception
	{
		// mandatory fields
		if (variantToFeed.getType() == null)
			variantToFeed.setType(Type.SNP.toString());
		else if (!variantToFeed.getType().equals(Type.SNP.toString()))
			throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

		List<String> knownAlleleList = new ArrayList<String>();
		if (variantToFeed.getKnownAlleleList().size() > 0)
			knownAlleleList.addAll(variantToFeed.getKnownAlleleList());
		variantToFeed.setKnownAlleleList(Arrays.asList(hmFeature.getAlleles()));	/*FIXME: dodgy code (can't we loose existing alleles doing this?)*/

		if (variantToFeed.getReferencePosition() == null)	// otherwise we leave it as it is (had some trouble with overridden end-sites)
			variantToFeed.setReferencePosition(new ReferencePosition(hmFeature.getChr(), hmFeature.getStart(), (long) hmFeature.getEnd()));

		VariantRunData run = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
	
		String[] knownAlleles = hmFeature.getAlleles();
			
		// genotype fields
		for (int i=0; i<hmFeature.getGenotypes().length; i++)
		{
			String genotype = hmFeature.getGenotypes()[i].toUpperCase(), gtCode = "";
			String sIndividual = hmFeature.getSampleIDs()[i];

			boolean fInvalidGT = genotype.length() != 2;
			if (!fInvalidGT)
			{
				String allele1 = genotype.substring(0, 1);
				String allele2 = genotype.substring(1, 2);
				
				int nRefAlleleCount = 0, altAlleleCount = 0;
				
				if (knownAlleles[0].equals(allele1))
					nRefAlleleCount++;
				else if (knownAlleles[1].equals(allele1))
					altAlleleCount++;
							
				if (knownAlleles[0].equals(allele2))
					nRefAlleleCount++;
				else if (knownAlleles[1].equals(allele2))
					altAlleleCount++;
				
				if (nRefAlleleCount + altAlleleCount == 2)
					gtCode = nRefAlleleCount == 2 ? "0/0" : (nRefAlleleCount == 1 ? "0/1" : "1/1");
			}
			
			if (gtCode.length() == 0 && !"NN".equals(genotype))
				LOG.warn("Ignoring invalid HapMap genotype \"" + gtCode + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);

			SampleGenotype aGT = new SampleGenotype(gtCode);

			if (!usedSamples.containsKey(sIndividual))	// we don't want to persist each sample several times
			{
				Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
				if (ind == null)
				{	// we don't have any population data so we don't need to update the Individual if it already exists
					ind = new Individual(sIndividual);
					mongoTemplate.save(ind);
				}

				Integer sampleIndex = null;
				List<Integer> sampleIndices = project.getIndividualSampleIndexes(sIndividual);
				if (sampleIndices.size() > 0)
					mainLoop : for (Integer index : sampleIndices)	// see if we should re-use an existing sample (we assume it's the same sample if it's the same run)
					{
						List<Criteria> crits = new ArrayList<Criteria>();
						crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
						crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(runName));
						crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
						Query q = new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()])));
						q.fields().include(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + index);
						VariantRunData variantRunDataWithDataForThisSample = mongoTemplate.findOne(q, VariantRunData.class);
						if (variantRunDataWithDataForThisSample != null)
						{
							sampleIndex = index;
							break mainLoop;
						}
					}

				if (sampleIndex == null)
				{	// no sample exists for this individual in this project and run, we need to create one
					sampleIndex = 1;
					try
					{
						sampleIndex += (Integer) project.getSamples().keySet().toArray(new Comparable[project.getSamples().size()])[project.getSamples().size() - 1];
					}
					catch (ArrayIndexOutOfBoundsException ignored)
					{}	// if array was empty, we keep 1 for the first id value
					project.getSamples().put(sampleIndex, new GenotypingSample(sIndividual));
//					LOG.info("Sample created for individual " + sIndividual + " with index " + sampleIndex);
				}
				usedSamples.put(sIndividual, new SampleId(project.getId(), sampleIndex));	// add a sample for this individual to the project
			}			

			run.getSampleGenotypes().put(usedSamples.get(sIndividual).getSampleIndex(), aGT);
		}
		return run;
	}
}