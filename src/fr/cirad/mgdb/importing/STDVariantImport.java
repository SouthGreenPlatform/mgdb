package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ExternalSort;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;

public class STDVariantImport extends AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(STDVariantImport.class);
	
	private int m_ploidy = 2;
	private String m_processID;
	
	public STDVariantImport()
	{
	}
	
	public STDVariantImport(String processID)
	{
		this();
		m_processID = processID;
	}
	
	public STDVariantImport(String processID, int nPloidy)
	{
		this();
		m_ploidy = nPloidy;
		m_processID = processID;
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, GENOTYPE file! An optional 6th parameter supports values '1' (empty project data before importing) and '2' (empty entire database before importing, including marker list).");

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
		new STDVariantImport().importToMongo(args[0], args[1], args[2], args[3], args[4], mode);
	}
	
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String mainFilePath, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		ProgressIndicator progress = ProgressIndicator.get(m_processID);
		if (progress == null)
			progress = new ProgressIndicator(m_processID, new String[] {"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		ProgressIndicator.registerProgressIndicator(progress);
		
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
			}
			
			mongoTemplate.getDb().command(new BasicDBObject("profile", 0));	// disable profiling
			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() != m_ploidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + m_ploidy + ") data!");

			if (importMode == 2) // drop database before importing
				mongoTemplate.getDb().dropDatabase();
			else if (project != null)
			{
				if (importMode == 1 || (project != null && project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
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
						String info = wr.getN() + " records removed from variantRunData";
						LOG.info(info);
                    }
					mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
				}
                if (mongoTemplate.count(null, VariantRunData.class) == 0 && doesDatabaseSupportImportingUnknownVariants(sModule))
                {	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
                    mongoTemplate.getDb().dropDatabase();
//                    project = null;
                }
			}
			
			progress.addStep("Reading marker IDs");
			progress.moveToNextStep();
			
			File genotypeFile = new File(mainFilePath);
	
            HashMap<String, Comparable> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate);
						
			progress.addStep("Checking genotype consistency");
			progress.moveToNextStep();
			
			HashMap<Comparable, ArrayList<String>> inconsistencies = checkSynonymGenotypeConsistency(existingVariantIDs, genotypeFile, sModule + "_" + sProject + "_" + sRun);
			
			// first sort genotyping data file by marker name (for faster import)
			BufferedReader in = new BufferedReader(new FileReader(genotypeFile));
			
			final Integer finalMarkerFieldIndex = 2;
			Comparator<String> comparator = new Comparator<String>() {						
				@Override
				public int compare(String o1, String o2) {	/* we want data to be sorted, first by locus, then by sample */
					String[] splitted1 = (o1.split(" ")), splitted2 = (o2.split(" "));
					return (splitted1[finalMarkerFieldIndex]/* + "_" + splitted1[finalSampleFieldIndex]*/).compareTo(splitted2[finalMarkerFieldIndex]/* + "_" + splitted2[finalSampleFieldIndex]*/);
				}
			};
			File sortedFile = new File("sortedImportFile_" + genotypeFile.getName());
			sortedFile.deleteOnExit();
			LOG.info("Sorted file will be " + sortedFile.getAbsolutePath());
			
			List<File> sortTempFiles = null;
			try
			{
				progress.addStep("Creating temp files to sort in batch");
				progress.moveToNextStep();			
				sortTempFiles = ExternalSort.sortInBatch(in, genotypeFile.length(), comparator, ExternalSort.DEFAULTMAXTEMPFILES, Charset.defaultCharset(), sortedFile.getParentFile(), false, 0, true, progress);
				//sortInBatch(in, genotypeFile.length(), comparator, progress);
				long afterSortInBatch = System.currentTimeMillis();
				LOG.info("sortInBatch took " + (afterSortInBatch - before)/1000 + "s");
				
				progress.addStep("Merging temp files");
				progress.moveToNextStep();
				ExternalSort.mergeSortedFiles(sortTempFiles, sortedFile, comparator, Charset.defaultCharset(), false, false, true, progress, genotypeFile.length());
				LOG.info("mergeSortedFiles took " + (System.currentTimeMillis() - afterSortInBatch)/1000 + "s");
			}
	        catch (java.io.IOException ioe)
	        {
	        	// it failed: let's cleanup
	        	if (sortTempFiles != null)
	            	for (File f : sortTempFiles)
	            		f.delete();
	        	if (sortedFile.exists())
	        		sortedFile.delete();
	        	LOG.error("Error occured sorting import file", ioe);
	        	return;
	        }

			// create project if necessary
			if (project == null)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(1 /* SNP chip */);
				project.setTechnology(sTechnology);
				project.getVariantTypes().add("SNP");
			}	
			project.setPloidyLevel(2);

			// import genotyping data
			progress.addStep("Processing genotype lines by thousands");
			progress.moveToNextStep();
			progress.setPercentageEnabled(false);
			HashMap<String, String> individualPopulations = new HashMap<String, String>();				
			in = new BufferedReader(new FileReader(sortedFile));
			String sLine = in.readLine();
			int nVariantSaveCount = 0;
			long lineCount = 0;
			String sPreviousVariant = null, sVariantName = null;
			ArrayList<String> linesForVariant = new ArrayList<String>(), unsavedVariants = new ArrayList<String>();
			TreeMap<String /* individual name */, SampleId> previouslySavedSamples = new TreeMap<String, SampleId>();	// will auto-magically remove all duplicates, and sort data, cool eh?
			TreeSet<String> affectedSequences = new TreeSet<String>();	// will contain all sequences containing variants for which we are going to add genotypes 
			do
			{
				if (sLine.length() > 0)
				{
					String[] splittedLine = sLine.trim().split(" ");
					sVariantName = splittedLine[2];
					individualPopulations.put(splittedLine[1], splittedLine[0]);
					if (!sVariantName.equals(sPreviousVariant))
					{
						if (sPreviousVariant != null)
						{	// save variant
							Comparable mgdbVariantId = existingVariantIDs.get(sPreviousVariant.toUpperCase());
							if (mgdbVariantId == null)
								LOG.warn("Unknown id: " + sPreviousVariant);
							else if (mgdbVariantId.toString().startsWith("*"))
								LOG.warn("Skipping deprecated variant data: " + sPreviousVariant);
							else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId, individualPopulations, inconsistencies, linesForVariant, 3, previouslySavedSamples, affectedSequences))
								nVariantSaveCount++;
							else
								unsavedVariants.add(sVariantName);
						}
						linesForVariant = new ArrayList<String>();
						sPreviousVariant = sVariantName;
					}
					linesForVariant.add(sLine);		
				}
				sLine = in.readLine();
				progress.setCurrentStepProgress((int) lineCount/1000);
				if (++lineCount % 100000 == 0)
				{
					String info = lineCount + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
					LOG.info(info);
				}
			}
			while (sLine != null);		

			Comparable mgdbVariantId = existingVariantIDs.get(sVariantName.toUpperCase());	// when saving the last variant there is not difference between sVariantName and sPreviousVariant
			if (mgdbVariantId == null)
				LOG.warn("Unknown id: " + sPreviousVariant);
			else if (mgdbVariantId.toString().startsWith("*"))
				LOG.warn("Skipping deprecated variant data: " + sPreviousVariant);
			else if (saveWithOptimisticLock(mongoTemplate, project, sRun, mgdbVariantId, individualPopulations, inconsistencies, linesForVariant, 3, previouslySavedSamples, affectedSequences))
				nVariantSaveCount++;
			else
				unsavedVariants.add(sVariantName);
	
			in.close();
			sortedFile.delete();
				
            if (!project.getVariantTypes().contains(Type.SNP.toString())) {
                project.getVariantTypes().add(Type.SNP.toString());
            }
            project.getSequences().addAll(affectedSequences);
			
			// save project data
            if (!project.getRuns().contains(sRun)) {
                project.getRuns().add(sRun);
            }
			mongoTemplate.save(project);
	
	    	LOG.info("Import took " + (System.currentTimeMillis() - before)/1000 + "s for " + lineCount + " CSV lines (" + nVariantSaveCount + " variants were saved)");
	    	if (unsavedVariants.size() > 0)
	    	   	LOG.warn("The following variants could not be saved because of concurrent writing: " + StringUtils.join(unsavedVariants, ", "));
	    	
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
	
	private static boolean saveWithOptimisticLock(MongoTemplate mongoTemplate, GenotypingProject project, String runName, Comparable mgdbVariantId, HashMap<String, String> individualPopulations, HashMap<Comparable, ArrayList<String>> inconsistencies, ArrayList<String> linesForVariant, int nNumberOfRetries, Map<String, SampleId> usedSamples, TreeSet<String> affectedSequences) throws Exception
	{
		if (linesForVariant.size() == 0)
			return false;
		
		for (int j=0; j<Math.max(1, nNumberOfRetries); j++)
		{			
			Query query = new Query(Criteria.where("_id").is(mgdbVariantId));
			query.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_KNOWN_ALLELE_LIST).include(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId()).include(VariantData.FIELDNAME_VERSION);
			
			VariantData variant = mongoTemplate.findOne(query, VariantData.class);
			Update update = variant == null ? null : new Update();
			if (update == null)
			{	// it's the first time we deal with this variant
				variant = new VariantData(mgdbVariantId);
				variant.setType(Type.SNP.toString());
			}
			else
			{
				ReferencePosition rp = variant.getReferencePosition();
				if (rp != null)
					affectedSequences.add(rp.getSequence());
			}
			
			String sVariantName = linesForVariant.get(0).trim().split(" ")[2];
//			if (!mgdbVariantId.equals(sVariantName))
//				variant.setSynonyms(markerSynonymMap.get(mgdbVariantId));	// provided id was a synonym
			
			VariantRunData theRun = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, mgdbVariantId));
			
			ArrayList<String> inconsistentIndividuals = inconsistencies.get(mgdbVariantId);
			for (String individualLine : linesForVariant)
			{				
				String[] cells = individualLine.trim().split(" ");
				String sIndividualName = cells[1];
						
				if (!usedSamples.containsKey(sIndividualName))	// we don't want to persist each sample several times
				{
					Individual ind = mongoTemplate.findById(sIndividualName, Individual.class);
					if (ind == null)
						ind = new Individual(sIndividualName);
					ind.setPopulation(individualPopulations.get(sIndividualName));
					mongoTemplate.save(ind);
					
					Integer sampleIndex = null;
					List<Integer> sampleIndices = project.getIndividualSampleIndexes(sIndividualName);
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
						project.getSamples().put(sampleIndex, new GenotypingSample(sIndividualName));
//						LOG.info("Sample created for individual " + sIndividualName + " with index " + newSampleIndex);
					}

					usedSamples.put(sIndividualName, new SampleId(project.getId(), sampleIndex));	// add a sample for this individual to the project
				}

				String gtString = "";
				boolean fInconsistentData = inconsistentIndividuals != null && inconsistentIndividuals.contains(sIndividualName);
				if (fInconsistentData)
					LOG.warn("Not adding inconsistent data: " + sVariantName + " / " + sIndividualName);
				else
				{
					ArrayList<Integer> alleleIndexList = new ArrayList<Integer>();	
					boolean fAddedSomeAlleles = false;
					for (int i=3; i<=4; i++)
					{
						int indexToUse = cells.length > i ? i : i - 1;
						if (!variant.getKnownAlleleList().contains(cells[indexToUse]))
						{
							variant.getKnownAlleleList().add(cells[indexToUse]);	// it's the first time we encounter this alternate allele for this variant
							fAddedSomeAlleles = true;
						}
						
						alleleIndexList.add(variant.getKnownAlleleList().indexOf(cells[indexToUse]));
					}
					
					if (fAddedSomeAlleles && update != null)
						update.set(VariantData.FIELDNAME_KNOWN_ALLELE_LIST, variant.getKnownAlleleList());
					
					Collections.sort(alleleIndexList);
					gtString = StringUtils.join(alleleIndexList, "/");
				}

				SampleGenotype genotype = new SampleGenotype(gtString);
				theRun.getSampleGenotypes().put(usedSamples.get(sIndividualName).getSampleIndex(), genotype);
			}
            project.getAlleleCounts().add(variant.getKnownAlleleList().size());	// it's a TreeSet so it will only be added if it's not already present
			
			try
			{
				if (update == null)
				{
					mongoTemplate.save(variant);
//					System.out.println("saved: " + variant.getId());
				}
				else if (!update.getUpdateObject().keySet().isEmpty())
				{
//					update.set(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId(), projectData);
					mongoTemplate.upsert(new Query(Criteria.where("_id").is(mgdbVariantId)).addCriteria(Criteria.where(VariantData.FIELDNAME_VERSION).is(variant.getVersion())), update, VariantData.class);
//					System.out.println("updated: " + variant.getId());
				}
				mongoTemplate.save(theRun);

				if (j > 0)
					LOG.info("It took " + j + " retries to save variant " + variant.getId());
				return true;
			}
			catch (OptimisticLockingFailureException olfe)
			{
//				LOG.info("failed: " + variant.getId());
			}
		}
		return false;	// all attempts failed
	}
	
	private static HashMap<Comparable, ArrayList<String>> checkSynonymGenotypeConsistency(HashMap<String, Comparable> markerIDs, File stdFile, String outputFilePrefix) throws IOException
	{
		long before = System.currentTimeMillis();
		BufferedReader in = new BufferedReader(new FileReader(stdFile));
		String sLine;
		final String separator = " ";
		long lineCount = 0;
		String sPreviousSample = null, sSampleName = null;
		HashMap<Comparable /*mgdb variant id*/, HashMap<String /*genotype*/, String /*synonyms*/>> genotypesByVariant = new HashMap<Comparable, HashMap<String, String>>();

		LOG.info("Checking genotype consistency between synonyms...");
		
		FileOutputStream inconsistencyFOS = new FileOutputStream(new File(stdFile.getParentFile() + File.separator + outputFilePrefix + "-INCONSISTENCIES.txt"));
		HashMap<Comparable /*mgdb variant id*/, ArrayList<String /*individual*/>> result = new HashMap<Comparable, ArrayList<String>>();
		
		while ((sLine = in.readLine()) != null)	
		{
			if (sLine.length() > 0)
			{
				String[] splittedLine = sLine.trim().split(separator);
				Comparable mgdbId = markerIDs.get(splittedLine[2].toUpperCase());
				if (mgdbId == null)
					mgdbId = splittedLine[2];
				else if (mgdbId.toString().startsWith("*"))
					continue;	// this is a deprecated SNP

				sSampleName = splittedLine[1];
				if (!sSampleName.equals(sPreviousSample))
				{				
					genotypesByVariant = new HashMap<Comparable, HashMap<String, String>>();
					sPreviousSample = sSampleName;
				}
				
				HashMap<String, String> synonymsByGenotype = genotypesByVariant.get(mgdbId);
				if (synonymsByGenotype == null)
				{
					synonymsByGenotype = new HashMap<String, String>();
					genotypesByVariant.put(mgdbId, synonymsByGenotype);
				}

				String genotype = splittedLine[3] + "," + splittedLine[splittedLine.length > 4 ? 4 : 3];
				String synonymsWithGenotype = synonymsByGenotype.get(genotype);
				synonymsByGenotype.put(genotype, synonymsWithGenotype == null ? splittedLine[2] : (synonymsWithGenotype + ";" + splittedLine[2]));
				if (synonymsByGenotype.size() > 1)
				{
					ArrayList<String> individualsWithInconsistentGTs = result.get(mgdbId);
					if (individualsWithInconsistentGTs == null)
					{
						individualsWithInconsistentGTs = new ArrayList<String>();
						result.put(mgdbId, individualsWithInconsistentGTs);
					}
					individualsWithInconsistentGTs.add(sSampleName);

					inconsistencyFOS.write(sSampleName.getBytes());
					for (String gt : synonymsByGenotype.keySet())
						inconsistencyFOS.write(("\t" + synonymsByGenotype.get(gt) + "=" + gt).getBytes());
					inconsistencyFOS.write("\r\n".getBytes());
				}
			}
			if (++lineCount%1000000 == 0)
				LOG.debug(lineCount + " lines processed (" + (System.currentTimeMillis() - before)/1000 + " sec) ");
		}
		in.close();
		inconsistencyFOS.close();
		
		LOG.info("Inconsistency and missing data file was saved to the following location: " + stdFile.getParentFile().getAbsolutePath());

		return result;
	}
}
