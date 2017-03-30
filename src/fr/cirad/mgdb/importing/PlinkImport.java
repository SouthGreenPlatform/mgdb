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

import htsjdk.variant.variantcontext.VariantContext.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;
import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException;

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
import fr.cirad.tools.genotypes.PlinkEigenstratTool;
import fr.cirad.tools.mongo.MongoTemplateManager;

// TODO: Auto-generated Javadoc
/**
 * The Class PlinkImport.
 */
public class PlinkImport extends AbstractGenotypeImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
	/** The m_process id. */
	private String m_processID;
	
	private boolean fImportUnknownVariants = false;
	
	/**
	 * Instantiates a new PLINK import.
	 */
	public PlinkImport()
	{
	}

	/**
	 * Instantiates a new PLINK import.
	 *
	 * @param processID the process id
	 */
	public PlinkImport(String processID)
	{
		m_processID = processID;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length < 6)
			throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, MAP file, and PED file! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		File mapFile = new File(args[4]);
		if (!mapFile.exists() || mapFile.length() == 0)
			throw new Exception("File " + args[4] + " is missing or empty!");
		
		File pedFile = new File(args[5]);
		if (!pedFile.exists() || pedFile.length() == 0)
			throw new Exception("File " + args[5] + " is missing or empty!");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[6]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new PlinkImport().importToMongo(args[0], args[1], args[2], args[3], args[4], args[5], mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param mapFilePath the map file path
	 * @param pedFilePath the ped file path
	 * @param importMode the import mode
	 * @throws Exception the exception
	 */
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String mapFilePath, String pedFilePath, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID);
        if (progress == null)
            progress = new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		progress.setPercentageEnabled(false);		

		LinkedHashSet<Integer> redundantVariantIndexes = new LinkedHashSet<>();
		
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

			fImportUnknownVariants = doesDatabaseSupportImportingUnknownVariants(sModule);			

			if (m_processID == null)
				m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

			mongoTemplate.getDb().command(new BasicDBObject("profile", 0));	// disable profiling
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
				if (mongoTemplate.count(null, VariantRunData.class) == 0 && fImportUnknownVariants)
                {	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
                    mongoTemplate.getDb().dropDatabase();
//                    project = null;
                }
			}

			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(2 /* Sequencing */);
				project.setTechnology(sTechnology);
			}
			project.setPloidyLevel(2);

			HashMap<String, Comparable> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate);			
			
			String info = "Loading variant list from MAP file";
			LOG.info(info);
			progress.addStep(info);
			progress.moveToNextStep();
			LinkedHashMap<String, String> variantsAndPositions = PlinkEigenstratTool.getVariantsAndPositionsFromPlinkMapFile(new File(mapFilePath), redundantVariantIndexes, "\t");
			String[] variants = variantsAndPositions.keySet().toArray(new String[variantsAndPositions.size()]);
			
			info = "Checking genotype consistency";
//			LOG.info(info);
			progress.addStep(info);
			progress.moveToNextStep();
			HashMap<Comparable, ArrayList<String>> inconsistencies = new HashMap<>();
			
			if (!project.getVariantTypes().contains(Type.SNP.toString()))
				project.getVariantTypes().add(Type.SNP.toString());

			// rotate matrix using temporary files
			info = "Reading and reorganizing genotypes";
			LOG.info(info);
			progress.addStep(info);
			progress.moveToNextStep();	
			HashMap<String, String> userIndividualToPopulationMapToFill = new LinkedHashMap<>();
			File[] tempFiles = rotatePlinkPedFile(variants, pedFilePath, userIndividualToPopulationMapToFill);
			long count = importTempFileContents(progress, mongoTemplate, tempFiles, variantsAndPositions, existingVariantIDs, project, sRun, inconsistencies, userIndividualToPopulationMapToFill);

			LOG.info("Import took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");

			progress.addStep("Preparing database for searches");
			progress.moveToNextStep();
			MgdbDao.prepareDatabaseForSearches(mongoTemplate);
			progress.markAsComplete();
		}
		finally
		{
			if (ctx != null)
				ctx.close();
		}
	}

	public long importTempFileContents(ProgressIndicator progress, MongoTemplate mongoTemplate, File[] tempFiles, LinkedHashMap<String, String> variantsAndPositions, HashMap<String, Comparable> existingVariantIDs,
			GenotypingProject project, String sRun, HashMap<Comparable, ArrayList<String>> inconsistencies, Map<String, String> userIndividualToPopulationMap) throws Exception			
	{
		String[] individuals = userIndividualToPopulationMap.keySet().toArray(new String[userIndividualToPopulationMap.size()]);
		ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
		ArrayList<VariantRunData> unsavedRuns = new ArrayList<VariantRunData>();
		long count = 0;
		
		// loop over each variation and write to DB
		Scanner scanner = null;
		try
		{
			String info = "Importing genotypes";
			LOG.info(info);
			progress.addStep(info);
			progress.moveToNextStep();
			progress.setPercentageEnabled(true);
			
			int nNumberOfVariantsToSaveAtOnce = 1;
			HashMap<String /*individual*/, SampleId> previouslyCreatedSamples = new HashMap<String /*individual*/, SampleId>();

			for (File tempFile : tempFiles)
			{
				scanner = new Scanner(tempFile);
				long nPreviousProgressPercentage = -1;
				mainLoop : while (scanner.hasNextLine())
				{
					StringTokenizer variantFields = new StringTokenizer(scanner.nextLine(), "\t");
					String providedVariantId = variantFields.nextToken();
					if (providedVariantId.toString().startsWith("*"))
					{
						System.err.print("\r\nSkipping deprecated variant data: " + providedVariantId);
						continue;
					}

					String[] seqAndPos = variantsAndPositions.get(providedVariantId).split("\t");
					String sequence = seqAndPos[0];
					Long bpPosition = Long.parseLong(seqAndPos[1]);
					if ("0".equals(sequence) || 0 == bpPosition)
					{
						sequence = null;
						bpPosition = null;
					}
					Comparable variantId = null;
					for (String variantDescForPos : getIdentificationStrings(Type.SNP.toString(), sequence, bpPosition, Arrays.asList(new String[] {providedVariantId})))
					{
						variantId = existingVariantIDs.get(variantDescForPos);
						if (variantId != null)
							break;
					}
					
					if (variantId == null && !fImportUnknownVariants)
					{
//						if (fImportUnknownVariants)
//							LOG.warn("Import of unknown variant (" + providedVariantId + ") not yet implemented");
//						else
							LOG.warn("Skipping unknown variant: " + providedVariantId);
					}
					else
					{
						VariantData variant = mongoTemplate.findById(variantId == null ? providedVariantId : variantId, VariantData.class);							
						if (variant == null)
							variant = new VariantData(providedVariantId);
						if (!unsavedVariants.contains(variant))
							unsavedVariants.add(variant);
//						else
//							System.out.println(providedVariantId);

						String[][] alleles = new String[2][individuals.length];
						int nIndividualIndex = 0;
						while (nIndividualIndex < alleles[0].length)
						{
							ArrayList<String> inconsistentIndividuals = inconsistencies.get(variant.getId());
							boolean fInconsistentData = inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
							if (fInconsistentData)
								LOG.warn("Not adding inconsistent data: " + providedVariantId + " / " + individuals[nIndividualIndex]);

							String genotype = variantFields.nextToken();
							alleles[0][nIndividualIndex] = fInconsistentData ? "0" : genotype.substring(0, 1);
							alleles[1][nIndividualIndex++] = fInconsistentData ? "0" : genotype.substring(1, 2);
						}

						VariantRunData runToSave = addPlinkDataToVariant(mongoTemplate, variant, sequence, bpPosition, userIndividualToPopulationMap, alleles, project, sRun, previouslyCreatedSamples, fImportUnknownVariants);
						if (!unsavedRuns.contains(runToSave))
							unsavedRuns.add(runToSave);
						
						if (count == 0)
						{
							nNumberOfVariantsToSaveAtOnce = Math.max(1, 30000 / individuals.length);
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
		
							long nProgressPercentage = count * 100 / variantsAndPositions.size();
							if (nPreviousProgressPercentage != nProgressPercentage)
							{
								progress.setCurrentStepProgress(nProgressPercentage);
								if (count > 0 && (count % (10 * nNumberOfVariantsToSaveAtOnce) == 0))
								{
									info = count + " lines processed (" + nProgressPercentage + "%)"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
									LOG.debug(info);
								}
								nPreviousProgressPercentage = nProgressPercentage;
							}
						}

						if (variant.getReferencePosition() != null && !project.getSequences().contains(variant.getReferencePosition().getSequence()))
							project.getSequences().add(variant.getReferencePosition().getSequence());

						project.getAlleleCounts().add(variant.getKnownAlleleList().size());	// it's a TreeSet so it will only be added if it's not already present
						if (variant.getKnownAlleleList().size() > 2)
							LOG.warn("Variant " + variant.getId() + " (" + providedVariantId + ") has more than 2 alleles!");
					}
					count++;
				}
				scanner.close();
				
				if (existingVariantIDs.size() == 0)
				{	// we benefit from the fact that it's the first variant import into this database and therefore use bulk insert which is meant to be faster
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

			}
		}
		finally
		{
			scanner.close();
			for (File f : tempFiles)
				if (f != null)
					f.delete();
		}
		return count;
	}

	private File[] rotatePlinkPedFile(String[] variants, String pedFilePath, Map<String, String> userIndividualToPopulationMapToFill) throws IOException, WrongNumberArgsException
	{
		long before = System.currentTimeMillis();
		File pedFile = new File(pedFilePath);
		Runtime rt = Runtime.getRuntime();
		
		StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
		boolean fCalledFromCommandLine = stacktrace[stacktrace.length-1].getClassName().equals(getClass().getName()) && "main".equals(stacktrace[stacktrace.length-1].getMethodName());
		
		// we grant ourselves a portion of the currently available memory for reading data: this defines how many markers we treat at once
		long allocatableMemory = (long) ((fCalledFromCommandLine ? .8 : .5) * (rt.maxMemory() - rt.totalMemory() + rt.freeMemory()));
		float readableFilePortion = (float) allocatableMemory / pedFile.length();
		int nMaxMarkersReadAtOnce = (int) (readableFilePortion * variants.length) / 4;
		int nCurrentChunkIndex = 0, nNumberOfChunks = (int) Math.ceil((float) variants.length / nMaxMarkersReadAtOnce);
		StringBuffer[] stringBuffers = null;
		
		File[] outputFiles = new File[nNumberOfChunks];
		try
		{
			while (nCurrentChunkIndex < nNumberOfChunks)
			{
				int nMarkersReadAtOnce = nCurrentChunkIndex == nNumberOfChunks - 1 ? variants.length % nMaxMarkersReadAtOnce : nMaxMarkersReadAtOnce;
				if (nCurrentChunkIndex == 0/* || nCurrentChunkIndex == nNumberOfChunks - 1*/)
					stringBuffers = new StringBuffer[nMarkersReadAtOnce];

				outputFiles[nCurrentChunkIndex] = File.createTempFile(nCurrentChunkIndex + "-plinkImportVariantChunk-" + pedFile.getName() + "-", ".tsv");
				FileWriter fw = new FileWriter(outputFiles[nCurrentChunkIndex]);
				try
				{
					for (int i=0; i<nMarkersReadAtOnce; i++)
						stringBuffers[i] = new StringBuffer(variants[nCurrentChunkIndex*nMaxMarkersReadAtOnce + i]);
					Scanner sc = new Scanner(pedFile);
					while (sc.hasNextLine())
					{
						String sLine = sc.nextLine();
						if (nCurrentChunkIndex == 0)
							PlinkEigenstratTool.readIndividualFromPlinkPedLine(sLine, (HashMap<String, String>) userIndividualToPopulationMapToFill);	// important because it fills the map
						int nFirstPosToRead = sLine.length() - 4*(variants.length - nCurrentChunkIndex * nMaxMarkersReadAtOnce);
						for (int i=0; i<nMarkersReadAtOnce; i++)
							stringBuffers[i].append("\t" + sLine.charAt(nFirstPosToRead + i*4+1) + sLine.charAt(nFirstPosToRead + i*4+3));
					}
					sc.close();
					for (int i=0; i<nMarkersReadAtOnce; i++)
						fw.write(stringBuffers[i].toString() + "\n");
				}
				finally
				{
					fw.close();
				}
				
				if (nCurrentChunkIndex != nNumberOfChunks - 1)
					LOG.debug("rotatePlinkPedFile treated " + ((nCurrentChunkIndex+1) * nMaxMarkersReadAtOnce) + " markers in " + (System.currentTimeMillis() - before) + "ms");
				nCurrentChunkIndex++;
			}
		}
		catch (Throwable t)
		{
			for (File f : outputFiles)
				if (f != null)
					f.delete();
			throw t;
		}
		LOG.info("PED matrix transposition took " + (System.currentTimeMillis() - before) + "ms for " + variants.length + " markers and " + userIndividualToPopulationMapToFill.size() + " individuals");
		return outputFiles;
	}

	/**
	 * Adds the PLINK data to variant.
	 * @param fImportUnknownVariants 
	 */
	static private VariantRunData addPlinkDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, String sequence, Long bpPos, Map<String, String> userIndividualToPopulationMap, String[][] alleles, GenotypingProject project, String runName, Map<String /*individual*/, SampleId> usedSamples, boolean fImportUnknownVariants) throws Exception
	{
		// mandatory fields
		if (variantToFeed.getType() == null)
			variantToFeed.setType(Type.SNP.toString());
		else if (!variantToFeed.getType().equals(Type.SNP.toString()))
			throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

		if (fImportUnknownVariants && variantToFeed.getReferencePosition() == null && sequence != null)	// otherwise we leave it as it is (had some trouble with overridden end-sites)
			variantToFeed.setReferencePosition(new ReferencePosition(sequence, bpPos, bpPos));
//		System.out.println(variantToFeed.getId() + " " + variantToFeed.getSynonyms().get("il") + " (chr" + sequence + ")");

		VariantRunData run = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
				
		// genotype fields
		int i = -1;
		for (String sIndividual : userIndividualToPopulationMap.keySet())
		{
			i++;
			int firstAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(alleles[0][i]);
			if (firstAlleleIndex == -1 && validNucleotides.contains(alleles[0][i]))
			{
				firstAlleleIndex = variantToFeed.getKnownAlleleList().size();
				variantToFeed.getKnownAlleleList().add(alleles[0][i]);
			}
			int secondAlleleIndex = variantToFeed.getKnownAlleleList().indexOf(alleles[1][i]);
			if (secondAlleleIndex == -1 && validNucleotides.contains(alleles[1][i]))
			{
				secondAlleleIndex = variantToFeed.getKnownAlleleList().size();
				variantToFeed.getKnownAlleleList().add(alleles[1][i]);
			}
			String gtCode = firstAlleleIndex <= secondAlleleIndex ? (firstAlleleIndex + "/" + secondAlleleIndex) : (secondAlleleIndex + "/" + firstAlleleIndex);

			if (gtCode.equals("-1/-1"))
				gtCode = "";
			else if (!gtCode.matches("([0-9])([0-9])*/([0-9])([0-9])*"))
			{
				gtCode = "";
				LOG.warn("Ignoring invalid PLINK genotype \"" + alleles[0][i] + " " + alleles[1][i] + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);
			}

			SampleGenotype aGT = new SampleGenotype(gtCode);

			if (!usedSamples.containsKey(sIndividual))	// we don't want to persist each sample several times
			{
				Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
				if (ind == null)
				{	// we don't have any population data so we don't need to update the Individual if it already exists
					ind = new Individual(sIndividual);
					String sPop = userIndividualToPopulationMap.get(sIndividual);
					if (!sPop.equals(".") && sPop.length() == 3)
						ind.setPopulation(sPop);
					else if (!sIndividual.substring(0, 3).matches(".*\\d+.*") && sIndividual.substring(3).matches("\\d+"))
						ind.setPopulation(sIndividual.substring(0, 3));
					else
						LOG.warn("Unable to find 3-digit population code for individual " + sIndividual);
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
						VariantRunData variantRunDataWithDataForThisSample = mongoTemplate.findOne(q, VariantRunData.class);	/* FIXME: this can take forever if the DB is big */
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
	
	private HashMap<Comparable, ArrayList<String>> checkSynonymGenotypeConsistency(String pedFilePath, String[] variants, HashMap<String, Comparable> existingVariantIDs, String outputFilePrefix) throws IOException, WrongNumberArgsException
	{
		long before = System.currentTimeMillis();
		File pedFile = new File(pedFilePath);
		String sLine;
		HashMap<Comparable /*widde variant id*/, HashMap<String /*genotype*/, String /*synonyms*/>> genotypesByVariant;

		LOG.info("Checking genotype consistency between synonyms...");
		
		FileOutputStream inconsistencyFOS = new FileOutputStream(new File(pedFile.getParentFile() + File.separator + outputFilePrefix + "-INCONSISTENCIES.txt"));
		HashMap<Comparable /*widde variant id*/, ArrayList<String /*individual*/>> result = new HashMap<Comparable, ArrayList<String>>();
						
		int nLineCounter = 0;		
		Scanner pedScanner = new Scanner(pedFile);
		try
		{
			int lineCount = 0;
			while (pedScanner.hasNextLine())
			{
				genotypesByVariant = new HashMap<Comparable, HashMap<String, String>>();
				sLine = pedScanner.nextLine().replaceAll("\t", " ");
				if (sLine.length() > 0)
				{
					if (sLine.trim().length() == 0)
					{
						LOG.error("Found empty line in " + pedFile.getName() + " at position " + nLineCounter);
						continue;
					}
					else if (sLine.startsWith("#"))
					{
						LOG.info("Skipping comment at position " + nLineCounter + " in PED file: " + sLine);
						continue;
					}
	
					String sIndividual = PlinkEigenstratTool.readIndividualFromPlinkPedLine(sLine, null);
					String[] individualGenotypes = PlinkEigenstratTool.readGenotypesFromPlinkPedLine(sLine, new LinkedHashSet<Integer>(), variants);
		
					int nCurrentVariantIndex = -1;
					for (String genotype : individualGenotypes)
					{
						nCurrentVariantIndex++;
						
						String providedVariantName = variants[nCurrentVariantIndex];
						Comparable widdeId = existingVariantIDs.get(providedVariantName.toUpperCase());
						if (widdeId != null && widdeId.toString().startsWith("*"))
							continue;	// this is a deprecated SNP
						
						HashMap<String, String> synonymsByGenotype = genotypesByVariant.get(widdeId);
						if (synonymsByGenotype == null)
						{
							synonymsByGenotype = new HashMap<String, String>();
							genotypesByVariant.put(widdeId, synonymsByGenotype);
						}
						String synonymsWithGenotype = synonymsByGenotype.get(genotype);
						synonymsByGenotype.put(genotype, synonymsWithGenotype == null ? providedVariantName : (synonymsWithGenotype + ";" + providedVariantName));
						if (synonymsByGenotype.size() > 1)
						{
							ArrayList<String> individualsWithInconsistentGTs = result.get(widdeId);
							if (individualsWithInconsistentGTs == null)
							{
								individualsWithInconsistentGTs = new ArrayList<String>();
								result.put(widdeId, individualsWithInconsistentGTs);
							}
							individualsWithInconsistentGTs.add(sIndividual);
	
							inconsistencyFOS.write(sIndividual.getBytes());
							for (String gt : synonymsByGenotype.keySet())
								inconsistencyFOS.write(("\t" + synonymsByGenotype.get(gt) + "=" + gt).getBytes());
							inconsistencyFOS.write("\r\n".getBytes());
						}
					}
				}
				if (++lineCount%1000000 == 0)
					LOG.debug(lineCount + " lines processed (" + (System.currentTimeMillis() - before)/1000 + " sec) ");
			}
		}
		finally
		{
			pedScanner.close();
		}
		inconsistencyFOS.close();
		LOG.info("Inconsistency and missing data file was saved to the following location: " + pedFile.getParentFile().getAbsolutePath());
		return result;
	}
}