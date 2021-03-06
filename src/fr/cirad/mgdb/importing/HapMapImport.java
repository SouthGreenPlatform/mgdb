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
import java.net.URL;
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

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
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

/**
 * The Class HapMapImport.
 */
public class HapMapImport extends AbstractGenotypeImport {
	
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
		new HapMapImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param mainFileUrl the main file URL
	 * @param importMode the import mode
	 * @return a project ID if it was created by this method, otherwise null
	 * @throws Exception the exception
	 */
	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mainFileUrl, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID);
        if (progress == null)
            progress = new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		progress.setPercentageEnabled(false);		

		FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), new RawHapMapCodec(), false);
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
            if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");

			if (importMode == 2) // drop database before importing
				mongoTemplate.getDb().dropDatabase();
			else if (project != null)
			{
				if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
				{	// empty project data before importing
					WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId())), DBVCFHeader.class);
					LOG.info(wr.getN() + " records removed from vcf_header");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantRunData.class);
					LOG.info(wr.getN() + " records removed from variantRunData");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
					project.clearEverythingExceptMetaData();
				}
				else
				{	// empty run data before importing
                    WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId()).and("_id." + VcfHeaderId.FIELDNAME_RUN).is(sRun)), DBVCFHeader.class);
					LOG.info(wr.getN() + " records removed from vcf_header");
                    if (project.getRuns().contains(sRun))
                    {
                    	LOG.info("Cleaning up existing run's data");
                    	List<Criteria> crits = new ArrayList<Criteria>();
						crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
						crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(sRun));
						crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
						wr = mongoTemplate.remove(new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))), VariantRunData.class);
						LOG.info(wr.getN() + " records removed from variantRunData");
                    }
					wr = mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);	// we are going to re-write it
				}
                if (mongoTemplate.count(null, VariantRunData.class) == 0 && doesDatabaseSupportImportingUnknownVariants(sModule))
                {	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
                    mongoTemplate.getDb().dropDatabase();
//                    project = null;
                }
			}

			Integer createdProject = null;
			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(2 /* Sequencing */);
				project.setTechnology(sTechnology);
				createdProject = project.getId();
			}
			project.setPloidyLevel(2);

			HashMap<String, Comparable> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);		
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
				try
				{
					Comparable variantId = null;
					for (String variantDescForPos : getIdentificationStrings(Type.SNP.toString(), hmFeature.getChr(), (long) hmFeature.getStart(), hmFeature.getName().length() == 0 ? null : Arrays.asList(new String[] {hmFeature.getName()})))
					{
						variantId = existingVariantIDs.get(variantDescForPos);
						if (variantId != null)
							break;
					}
					VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
					if (variant == null)
					{
						Comparable providedVariantId = hmFeature.getName() != null && hmFeature.getName().length() > 0 ? hmFeature.getName() : new ObjectId();
						variant = new VariantData(providedVariantId instanceof String && ObjectId.isValid((String)providedVariantId) ? new ObjectId((String) providedVariantId) : providedVariantId);
					}

					VariantRunData runToSave = addHapMapDataToVariant(mongoTemplate, variant, hmFeature, project, sRun, previouslyCreatedSamples);

					if (!project.getSequences().contains(hmFeature.getChr()))
						project.getSequences().add(hmFeature.getChr());

					int alleleCount = hmFeature.getAlleles().length;
					project.getAlleleCounts().add(alleleCount);	// it's a TreeSet so it will only be added if it's not already present
					if (alleleCount > 2)
						LOG.warn("Variant " + variant.getId() + " (" + variant.getReferencePosition().getSequence() + ":" + variant.getReferencePosition().getStartSite() + ") has more than 2 alleles!");
					
					if (variant.getKnownAlleleList().size() > 0)
					{	// we only import data related to a variant if we know its alleles
						if (!unsavedVariants.contains(variant))
							unsavedVariants.add(variant);
						if (!unsavedRuns.contains(runToSave))
							unsavedRuns.add(runToSave);
					}

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
					count++;
				}
				catch (Exception e)
				{
					throw new Exception("Error occured importing variant number " + (count + 1) + " (" + Type.SNP.toString() + ":" + hmFeature.getChr() + ":" + hmFeature.getStart() + ")", e);
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
			return createdProject;
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

		if (variantToFeed.getReferencePosition() == null)	// otherwise we leave it as it is (had some trouble with overridden end-sites)
			variantToFeed.setReferencePosition(new ReferencePosition(hmFeature.getChr(), hmFeature.getStart(), (long) hmFeature.getEnd()));

		VariantRunData run = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
			
		// genotype fields
		for (int i=0; i<hmFeature.getGenotypes().length; i++)
		{
			String sIndividual = hmFeature.getSampleIDs()[i];

			String genotype = hmFeature.getGenotypes()[i].toUpperCase(), gtCode = "";
			if (!"NN".equals(genotype) && genotype.length() == 2)
			{
				String allele1 = genotype.substring(0, 1);
				String allele2 = genotype.substring(1, 2);

				int firstAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(allele1);
				if (firstAlleleIndex == -1 && validNucleotides.contains(allele1))
				{
					firstAlleleIndex = variantToFeed.getKnownAlleleList().size();
					variantToFeed.getKnownAlleleList().add(allele1);
				}
				int secondAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(allele2);
				if (secondAlleleIndex == -1 && validNucleotides.contains(allele2))
				{
					secondAlleleIndex = variantToFeed.getKnownAlleleList().size();
					variantToFeed.getKnownAlleleList().add(allele2);
				}
				gtCode = firstAlleleIndex <= secondAlleleIndex ? (firstAlleleIndex + "/" + secondAlleleIndex) : (secondAlleleIndex + "/" + firstAlleleIndex);
			}
			if (!"NN".equals(genotype) && (gtCode.length() == 0 || !gtCode.matches("([0-9])([0-9])*/([0-9])([0-9])*")))
			{
				gtCode = "";
				LOG.warn("Ignoring invalid HapMap genotype \"" + genotype + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);
			}

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