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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
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

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.Status;
import jhi.brapi.api.calls.BrapiCall;
import jhi.brapi.api.genomemaps.BrapiGenomeMap;
import jhi.brapi.api.genomemaps.BrapiMarkerPosition;
import jhi.brapi.api.markerprofiles.BrapiAlleleMatrix;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfile;
import jhi.brapi.api.markers.BrapiMarker;
import jhi.brapi.client.AsyncChecker;
import jhi.flapjack.io.brapi.BrapiClient;
import jhi.flapjack.io.brapi.BrapiClient.Pager;
import jhi.flapjack.io.brapi.BrapiService;
import jhi.flapjack.io.brapi.CallsUtils;

import retrofit2.Call;
import retrofit2.Response;

/**
 * The Class BrapiImport.
 */
public class BrapiImport extends AbstractGenotypeImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);

	/** The m_process id. */
	private String m_processID;
	
	private int m_ploidy = 2;

	private BrapiClient client = new BrapiClient();
	
	/**
	 * Instantiates a new hap map import.
	 * @throws Exception 
	 */
	public BrapiImport() throws Exception
	{
	}

	/**
	 * Instantiates a new hap map import.
	 *
	 * @param processID the process id
	 * @throws Exception 
	 */
	public BrapiImport(String processID) throws Exception
	{
		this();
		m_processID = processID;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 7)
			throw new Exception("You must pass 7 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, ENDPOINT URL, STUDY-ID, and MAP-UD! An optional 8th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new BrapiImport().importToMongo(args[0], args[1], args[2], args[3], args[4], args[5], args[6], mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param endpoint URL
	 * @param importMode the import mode
	 * @throws Exception the exception
	 */
	public void importToMongo(String sModule, String sProject, String sRun, String sTechnology, String endpointUrl, String studyDbId, String mapDbId, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
		final ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		
		GenericXmlApplicationContext ctx = null;
		File tempFile = File.createTempFile("brapiImportVariants-" + progress.getProcessId() + "-", ".tsv");
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
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
                if (mongoTemplate.count(null, VariantRunData.class) == 0 && doesDatabaseSupportImportingUnknownVariants(sModule))
                    mongoTemplate.getDb().dropDatabase(); // if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
			}

			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setTechnology(sTechnology);
			}
						
			client.initService(endpointUrl);			
			final BrapiService service = client.getService();

			// see if TSV format is supported by remote service
			Pager callPager = new Pager();
			List<BrapiCall> calls = new ArrayList<>();
			while (callPager.isPaging())
			{
				BrapiListResource<BrapiCall> br = service.getCalls(callPager.getPageSize(), callPager.getPage()).execute().body();
				calls.addAll(br.data());
				callPager.paginate(br.getMetadata());
			}

			CallsUtils callsUtils = new CallsUtils(calls);
			boolean fMayUseTsv = callsUtils.hasCall("allelematrix-search/status/{id}", CallsUtils.JSON, CallsUtils.POST);
//			fMayUseTsv=false;
			client.setMapID(mapDbId);
			
			Pager markerPager = new Pager();

			Pager mapPager = new Pager();						
			while (mapPager.isPaging())
			{
				BrapiListResource<BrapiGenomeMap> maps = service.getMaps(null, mapPager.getPageSize(), mapPager.getPage())
						.execute()
						.body();
				for (BrapiGenomeMap map : maps.data())
				{
					if (mapDbId.equals(map.getMapDbId()))
					{
						markerPager.setPageSize("" + Math.min(map.getMarkerCount() / 10, 200000));
						break;
					}
					LOG.info("Unable to determine marker count for map " + mapDbId);
				}
				mapPager.paginate(maps.getMetadata());
			}
			
			progress.addStep("Scanning existing marker IDs");
			progress.moveToNextStep();

            HashMap<String, Comparable> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true);
			
			progress.addStep("Reading remote marker list");
			progress.moveToNextStep();
			
			ArrayList<String> variantsToQueryGenotypesFor = new ArrayList<>();
			while (markerPager.isPaging())
			{
				LOG.debug("querying page " + markerPager.getPage());
				BrapiListResource<BrapiMarkerPosition> positions = service.getMapMarkerData(mapDbId, markerPager.getPageSize(), markerPager.getPage())
					.execute()
					.body();
		
				Map<String, VariantData> variantsToCreate = new HashMap<String, VariantData>();
				for (BrapiMarkerPosition bmp : positions.data())
				{
					variantsToQueryGenotypesFor.add(bmp.getMarkerDbId());
					if (existingVariantIDs.get(bmp.getMarkerDbId().toUpperCase()) != null)
						continue;	// we already have this one

					VariantData variant = new VariantData(bmp.getMarkerDbId());
					long startSite = (long) Double.parseDouble(bmp.getLocation());
					variant.setReferencePosition(new ReferencePosition(bmp.getLinkageGroup(), startSite));
					variantsToCreate.put(bmp.getMarkerDbId(), variant);
				}
				
				// get variant types for new variants
				if (variantsToCreate.size() > 0)
				{
					Pager subPager = new Pager();
					subPager.setPageSize("" + variantsToCreate.size());
					
					while (subPager.isPaging())
					{
						BrapiListResource<BrapiMarker> markerInfo = service.getMarkerInfo(variantsToCreate.keySet(), "exact", null, null, subPager.getPageSize(), subPager.getPage())
							.execute()
							.body();
						
						for (BrapiMarker marker : markerInfo.data())
						{
							VariantData variant = variantsToCreate.get(marker.getMarkerDbId());
							if (marker.getDefaultDisplayName() != null && marker.getDefaultDisplayName().length() > 0)
							{
								TreeSet<Comparable> internalSynonyms = variant.getSynonyms().get(VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL);
								if (internalSynonyms == null)
								{
									internalSynonyms = new TreeSet<>();
									variant.getSynonyms().put(VariantData.FIELDNAME_SYNONYM_TYPE_ID_INTERNAL, internalSynonyms);
								}
								for (String syn : marker.getSynonyms())
									internalSynonyms.add(syn);
							}
							if (marker.getType() != null && marker.getType().length() > 0)
							{
								variant.setType(marker.getType());
								if (!project.getVariantTypes().contains(marker.getType()))
									project.getVariantTypes().add(marker.getType());
							}
							if (marker.getRefAlt() != null && marker.getRefAlt().size() > 0)
								variant.setKnownAlleleList(marker.getRefAlt());
							
							// update list of existing variants (FIXME: this should be a separate method in AbstractGenotypeImport) 
							ArrayList<String> idAndSynonyms = new ArrayList<>();
							idAndSynonyms.add(variant.getId().toString());
							for (Collection<Comparable> syns : variant.getSynonyms().values())
								for (Comparable syn : syns)
									idAndSynonyms.add(syn.toString());
						}
						subPager.paginate(markerInfo.getMetadata());
					}
					mongoTemplate.insertAll(variantsToCreate.values());
				}
				
				markerPager.paginate(positions.getMetadata());
				int nCurrentPage = positions.getMetadata().getPagination().getCurrentPage();
				int nTotalPageCount = positions.getMetadata().getPagination().getTotalPages();
				progress.setCurrentStepProgress((int) ((nCurrentPage + 1) * 100f / nTotalPageCount));
			}

			progress.addStep(fMayUseTsv ? "Waiting for remote genotype file to be created" : "Downloading remote genotypes into temporary file");
			progress.moveToNextStep();
			
			client.setStudyID(studyDbId);			
			List<BrapiMarkerProfile> markerprofiles = client.getMarkerProfiles();
			HashMap<String, List<String>> germplasmToProfilesMap = new HashMap();
			for (BrapiMarkerProfile markerPofile : markerprofiles)
			{
				List<String> gpProfiles = germplasmToProfilesMap.get(markerPofile.getGermplasmDbId());
				if (gpProfiles == null)
				{
					gpProfiles = new ArrayList<String>();
					germplasmToProfilesMap.put(markerPofile.getGermplasmDbId(), gpProfiles);
				}
				gpProfiles.add(markerPofile.getMarkerProfileDbId());
			}
			HashMap<String, String> profileToGermplasmMap = new HashMap<>();
			for (String gp : germplasmToProfilesMap.keySet())
			{
				List<String> profiles = germplasmToProfilesMap.get(gp);
				if (profiles.size() > 1)
					throw new Exception("Only one markerprofile per germplasm is allowed when importing a run. Found " + profiles.size() + " for " + gp);
				profileToGermplasmMap.put(profiles.get(0), gp);
			}

			LOG.debug("Importing " + markerprofiles.size() + " individuals");
			List<String> markerProfileIDs = markerprofiles.stream().map(BrapiMarkerProfile::getMarkerProfileDbId).collect(Collectors.toList());
			
			long count = 0;			
			int ploidy = 0;
			LOG.debug("Importing from " + endpointUrl + " using " + (fMayUseTsv ? "TSV" : "JSON") + " format");

			if (fMayUseTsv)
			{	// first call to initiate data export on server-side
				Pager genotypePager = new Pager();
				Response<BrapiBaseResource<BrapiAlleleMatrix>> response = service.getAlleleMatrix_byPost(markerProfileIDs, null, CallsUtils.TSV, true, "", URLEncoder.encode("|", "UTF-8"), "/", genotypePager.getPageSize(), genotypePager.getPage()).execute();
				BrapiBaseResource<BrapiAlleleMatrix> br = response.body();
				if (response.isSuccessful())
					br = response.body();
				else
					throw new Exception(new String(response.errorBody().bytes()));
				List<Status> statusList = br.getMetadata().getStatus();
				String extractId = statusList != null && statusList.size() > 0 && statusList.get(0).getCode().equals("asynchid") ? statusList.get(0).getMessage() : null;
				
				while (genotypePager.isPaging())
				{	
					Call<BrapiBaseResource<Object>> statusCall = service.getAlleleMatrixStatus(extractId);

					// Make an initial call to check the status on the resource
					BrapiBaseResource<Object> statusPoll = statusCall.execute().body();
					Status status = AsyncChecker.checkAsyncStatus(statusPoll.getMetadata().getStatus());

					// Keep checking until the async call returns anything other than "INPROCESS"
					while (AsyncChecker.callInProcess(status))
					{
						// Wait for a second before polling again
						try { Thread.sleep(1000); }
						catch (InterruptedException e) {}
						
						int nProgress = statusPoll.getMetadata().getPagination().getCurrentPage();
						if (nProgress != 0)
							progress.setCurrentStepProgress(nProgress);
						
						// Clone the previous retrofit call so we can call it again
						statusPoll = statusCall.clone().execute().body();
						status = AsyncChecker.checkAsyncStatus(statusPoll.getMetadata().getStatus());
					}

					if (AsyncChecker.ASYNC_FAILED.equals(status.getMessage()))
					{
						progress.setError("BrAPI export failed on server-side");
						return;
					}
					
					if (!AsyncChecker.callFinished(status))
					{
						progress.setError("BrAPI export is in unknown status");
						return;
					}
					else
					{
						progress.addStep("Downloading remote genotypes into temporary file");
						progress.moveToNextStep();
						
						URI uri = new URI(statusPoll.getMetadata().getDatafiles().get(0));
						FileUtils.copyURLToFile(uri.toURL(), tempFile);

						importTsvToMongo(sModule, project, sRun, sTechnology, tempFile.getAbsolutePath(), profileToGermplasmMap, importMode, existingVariantIDs);
						return;	//finished
					}
				}
			}
			else
			{
				LOG.debug("Writing remote data to temp file: " + tempFile);
				FileWriter tempFileWriter = new FileWriter(tempFile);
				
				int GENOTYPE_QUERY_SIZE = 30000, nChunkSize = GENOTYPE_QUERY_SIZE / markerProfileIDs.size(), nChunkIndex = 0;
		        while (nChunkIndex * nChunkSize < variantsToQueryGenotypesFor.size())
		        {
					progress.setCurrentStepProgress((nChunkIndex * nChunkSize) * 100 / variantsToQueryGenotypesFor.size());
					
			        List<String> markerSubList = variantsToQueryGenotypesFor.subList(nChunkIndex * nChunkSize, Math.min(variantsToQueryGenotypesFor.size(), ++nChunkIndex * nChunkSize));

					Pager genotypePager = new Pager();
					genotypePager.setPageSize("" + 50000);	
						
					while (genotypePager.isPaging())
					{
						BrapiBaseResource<BrapiAlleleMatrix> br = null;
						Call<BrapiBaseResource<BrapiAlleleMatrix>> call = service.getAlleleMatrix_byPost(markerProfileIDs, markerSubList, CallsUtils.JSON, true, "", URLEncoder.encode("|", "UTF-8"), "/", genotypePager.getPageSize(), genotypePager.getPage());
						Response<BrapiBaseResource<BrapiAlleleMatrix>> response = call.execute();
						if (response.isSuccessful())
							br = response.body();
						else
							throw new Exception(new String(response.errorBody().bytes()));
		
						for (List<String> row : br.getResult().getData())
							if (row.get(2).length() > 0)
							{
								String genotype = row.get(2);
								if (ploidy == 0)
									ploidy = 1 + org.springframework.util.StringUtils.countOccurrencesOf(genotype, "/");	/* FIXME: check if other allele separators exist */
								tempFileWriter.write(". " + profileToGermplasmMap.get(row.get(1)) + " " + row.get(0) + " " + genotype.replaceAll("/", " ") + "\n");
							}
	
						genotypePager.paginate(br.getMetadata());
						tempFileWriter.flush();
//						int nCurrentPage = br.getMetadata().getPagination().getCurrentPage();
//						int nTotalPageCount = br.getMetadata().getPagination().getTotalPages();
//						progress.setCurrentStepProgress((int) ((nCurrentPage + 1) * 100f / nTotalPageCount));					
					}
		        }
				tempFileWriter.close();

				// STDVariantImport is convenient because it always sorts data by variants
				STDVariantImport stdVariantImport = new STDVariantImport(progress.getProcessId());
				mongoTemplate.save(project);	// save the project so it can be re-opened by our STDVariantImport
				stdVariantImport.allowDbDropIfNoGenotypingData(false);
				stdVariantImport.tryAndMatchRandomObjectIDs(true);
				stdVariantImport.importToMongo(sModule, sProject, sRun, sTechnology, tempFile.getAbsolutePath(), importMode);
			}

			LOG.info("BrapiImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
		}
		catch (SocketException se)
		{
			if ("Connection reset".equals(se.getMessage()))
			{
				LOG.error("Error invoking BrAPI service. Try and check server-side logs", se);
				throw new Exception("Error invoking BrAPI service", se);
			}
		}
		finally
		{
			if (tempFile.exists())
			{
//				System.out.println("temp file size: " + tempFile.length());
				tempFile.delete();
			}
			if (ctx != null)
				ctx.close();
		}
	}
	
	public void importTsvToMongo(String sModule, GenotypingProject project, String sRun, String sTechnology, String mainFilePath, Map<String, String> markerProfileToIndividualMap, int importMode, HashMap<String, Comparable> existingVariantIDs) throws Exception
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
            if (importMode == 0 && project != null && project.getPloidyLevel() > 0 && project.getPloidyLevel() != m_ploidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + m_ploidy + ") data!");
			
			File genotypeFile = new File(mainFilePath);
			
//			progress.addStep("Checking genotype consistency");
//			progress.moveToNextStep();
//			HashMap<Comparable, ArrayList<String>> inconsistencies = checkSynonymGenotypeConsistency(existingVariantIDs, genotypeFile, sModule + "_" + sProject + "_" + sRun);
			
			BufferedReader in = new BufferedReader(new FileReader(genotypeFile));
			
//			// create project if necessary
//			if (project == null)
//			{	// create it
//				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
//				project.setName(sProject);
//				project.setOrigin(1 /* SNP chip */);
//				project.setTechnology(sTechnology);
//			}

			// The first line is a list of marker profile IDs
			List<String> individualList = Arrays.asList(in.readLine().split("\t"));
			List<String> individuals = individualList.subList(1, individualList.size());

			// import genotyping data
			progress.addStep("Processing variant lines");
			progress.moveToNextStep();
			progress.setPercentageEnabled(false);		
			String sLine = in.readLine();
			int nVariantSaveCount = 0;
			long lineCount = 0;
			String sVariantName = null;
			ArrayList<String> unsavedVariants = new ArrayList<String>();
			TreeMap<String /* individual name */, SampleId> previouslySavedSamples = new TreeMap<String, SampleId>();	// will auto-magically remove all duplicates, and sort data, cool eh?
			TreeSet<String> affectedSequences = new TreeSet<String>();	// will contain all sequences containing variants for which we are going to add genotypes
			HashMap<String /*individual*/, Comparable> phasingGroup = new HashMap<>();
			do
			{
				if (sLine.length() > 0)
				{
					String[] splittedLine = sLine.trim().split("\t");
					sVariantName = splittedLine[0];
					Comparable mgdbVariantId = existingVariantIDs.get(sVariantName.toUpperCase());
					if (mgdbVariantId == null)
						LOG.warn("Unknown id: " + sVariantName);
					else if (mgdbVariantId.toString().startsWith("*"))
						LOG.warn("Skipping deprecated variant data: " + sVariantName);
					else if (saveWithOptimisticLock(mongoTemplate, project, sRun, individuals, markerProfileToIndividualMap, mgdbVariantId, new HashMap<Comparable, ArrayList<String>>() /*FIXME or ditch me*/, sLine, 3, previouslySavedSamples, affectedSequences, phasingGroup))
						nVariantSaveCount++;
					else
						unsavedVariants.add(sVariantName);
				}
				sLine = in.readLine();
				progress.setCurrentStepProgress((int) ++lineCount);
			}
			while (sLine != null);

			in.close();

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
	
	private static boolean saveWithOptimisticLock(MongoTemplate mongoTemplate, GenotypingProject project, String runName, List<String> markerProfiles, Map<String, String> markerProfileToIndividualMap, Comparable mgdbVariantId, HashMap<Comparable, ArrayList<String>> inconsistencies, String lineForVariant, int nNumberOfRetries, Map<String, SampleId> usedSamples, TreeSet<String> affectedSequences, HashMap<String /*individual*/, Comparable> phasingGroup) throws Exception
	{		
		for (int j=0; j<Math.max(1, nNumberOfRetries); j++)
		{			
			Query query = new Query(Criteria.where("_id").is(mgdbVariantId));
			query.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_KNOWN_ALLELE_LIST).include(VariantData.FIELDNAME_PROJECT_DATA + "." + project.getId()).include(VariantData.FIELDNAME_VERSION);
			
			VariantData variant = mongoTemplate.findOne(query, VariantData.class);
			Update update = variant == null ? null : new Update();
			ReferencePosition rp = variant.getReferencePosition();
			if (rp != null)
				affectedSequences.add(rp.getSequence());
			
			String sVariantName = lineForVariant.trim().split("\t")[0];
			
			VariantRunData theRun = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, mgdbVariantId));
			
			ArrayList<String> inconsistentIndividuals = inconsistencies.get(mgdbVariantId);
			String[] cells = lineForVariant.trim().split("\t");
			for (int k=1; k<=markerProfiles.size(); k++)
			{				
				String sIndividualName = markerProfileToIndividualMap.get(markerProfiles.get(k - 1));

				if (!usedSamples.containsKey(sIndividualName))	// we don't want to persist each sample several times
				{
					Individual ind = mongoTemplate.findById(sIndividualName, Individual.class);
					if (ind == null)
						ind = new Individual(sIndividualName);
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
					boolean fAddedSomeAlleles = false;
					ArrayList<Integer> alleleIndexList = new ArrayList<Integer>();
					String phasedGT = null;
					if (k < cells.length && cells[k].length() > 0/* && !"N".equals(cells[k])*/)
					{
						if (cells[k].contains("|"))
							phasedGT = cells[k];
						
			            Comparable phasedGroup = phasingGroup.get(sIndividualName);
			            if (phasedGroup == null || (phasedGT == null/* && !genotype.isNoCall()*/))
			                phasingGroup.put(sIndividualName, variant.getId());

						String[] alleles = cells[k].split(phasedGT != null ? "\\|" : "\\/"); 	/*FIXME: deal with other separators*/
						for (int i=0; i<alleles.length; i++)
						{
							int indexToUse = alleles.length > i ? i : i - 1;
							if (!variant.getKnownAlleleList().contains(alleles[indexToUse]))
							{
								variant.getKnownAlleleList().add(alleles[indexToUse]);	// it's the first time we encounter this alternate allele for this variant
								fAddedSomeAlleles = true;
							}
							alleleIndexList.add(variant.getKnownAlleleList().indexOf(alleles[indexToUse]));
						}
						if (project.getPloidyLevel() == 0)
							project.setPloidyLevel(alleles.length);
					}
					Collections.sort(alleleIndexList);
					gtString = StringUtils.join(alleleIndexList, "/");

					SampleGenotype genotype = new SampleGenotype(gtString);
					theRun.getSampleGenotypes().put(usedSamples.get(sIndividualName).getSampleIndex(), genotype);
		            if (phasedGT != null) {
		            	genotype.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_GT, StringUtils.join(alleleIndexList, "|"));
		            	genotype.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndividualName));
		            }
					
					if (fAddedSomeAlleles && update != null)
						update.set(VariantData.FIELDNAME_KNOWN_ALLELE_LIST, variant.getKnownAlleleList());
				}
			}
            project.getAlleleCounts().add(variant.getKnownAlleleList().size());	// it's a TreeSet so it will only be added if it's not already present

			try
			{
				/*if (update == null)
				{
					mongoTemplate.save(variant);
//					System.out.println("saved: " + variant.getId());
				}
				else */if (!update.getUpdateObject().keySet().isEmpty())
				{
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
		final String separator = "\t";
		long lineCount = 0;
		String individual = null;
		HashMap<Comparable /*mgdb variant id*/, HashMap<String /*genotype*/, String /*synonyms*/>> genotypesByVariant/* = new HashMap<Comparable, HashMap<String, String>>()*/;

		LOG.info("Checking genotype consistency between synonyms...");
		
		FileOutputStream inconsistencyFOS = new FileOutputStream(new File(stdFile.getParentFile() + File.separator + outputFilePrefix + "-INCONSISTENCIES.txt"));
		HashMap<Comparable /*mgdb variant id*/, ArrayList<String /*individual*/>> result = new HashMap<Comparable, ArrayList<String>>();
		
		// The first line is a list of marker profile IDs
		List<String> individualList = Arrays.asList(in.readLine().split("\t"));
		List<String> individuals = individualList.subList(1, individualList.size());
		
		while ((sLine = in.readLine()) != null)	
		{
			if (sLine.length() > 0)
			{
				String[] splittedLine = sLine.trim().split(separator);
				Comparable mgdbId = markerIDs.get(splittedLine[0].toUpperCase());
				if (mgdbId == null)
					mgdbId = splittedLine[0];
				else if (mgdbId.toString().startsWith("*"))
					continue;	// this is a deprecated SNP

//				if (!sSampleName.equals(sPreviousSample))
				{				
					genotypesByVariant = new HashMap<Comparable, HashMap<String, String>>();
//					sPreviousSample = sSampleName;
				}
				
				HashMap<String, String> synonymsByGenotype = genotypesByVariant.get(mgdbId);
				if (synonymsByGenotype == null)
				{
					synonymsByGenotype = new HashMap<String, String>();
					genotypesByVariant.put(mgdbId, synonymsByGenotype);
				}

				for (int i=1; i<splittedLine.length; i++)
				{
					individual = individuals.get(i - 1);
					String genotype = splittedLine[i];
					String synonymsWithGenotype = synonymsByGenotype.get(genotype);
					synonymsByGenotype.put(genotype, synonymsWithGenotype == null ? splittedLine[0] : (synonymsWithGenotype + ";" + splittedLine[0]));
					if (synonymsByGenotype.size() > 1)
					{
						ArrayList<String> individualsWithInconsistentGTs = result.get(mgdbId);
						if (individualsWithInconsistentGTs == null)
						{
							individualsWithInconsistentGTs = new ArrayList<String>();
							result.put(mgdbId, individualsWithInconsistentGTs);
						}
						individualsWithInconsistentGTs.add(individual);
	
						inconsistencyFOS.write(individual.getBytes());
						for (String gt : synonymsByGenotype.keySet())
							inconsistencyFOS.write(("\t" + synonymsByGenotype.get(gt) + "=" + gt).getBytes());
						inconsistencyFOS.write("\r\n".getBytes());
					}
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