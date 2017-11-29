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
package fr.cirad.mgdb.exporting.individualoriented;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

// TODO: Auto-generated Javadoc
/**
 * The Class AbstractIndividualOrientedExportHandler.
 */
public abstract class AbstractIndividualOrientedExportHandler implements IExportHandler
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(AbstractIndividualOrientedExportHandler.class);
	
	/** The individual oriented export handlers. */
	static private TreeMap<String, AbstractIndividualOrientedExportHandler> individualOrientedExportHandlers = null;
	
	/**
	 * Export data.
	 *
	 * @param outputStream the output stream
	 * @param sModule the module
	 * @param individualExportFiles the individual export files
	 * @param fDeleteSampleExportFilesOnExit whether or not to delete sample export files on exit
	 * @param progress the progress
	 * @param markerCursor the marker cursor
	 * @param markerSynonyms the marker synonyms
	 * @param readyToExportFiles the ready to export files
	 * @throws Exception the exception
	 */
	abstract public void exportData(OutputStream outputStream, String sModule, Collection<File> individualExportFiles, boolean fDeleteSampleExportFilesOnExit, ProgressIndicator progress, DBCursor markerCursor, Map<Comparable, Comparable> markerSynonyms, Map<String, InputStream> readyToExportFiles) throws Exception;

	/**
	 * Gets the individuals from samples.
	 *
	 * @param sModule the module
	 * @param sampleIDs the sample ids
	 * @return the individuals from samples
	 */
	protected List<Individual> getIndividualsFromSamples(final String sModule, final List<SampleId> sampleIDs)
	{
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		HashMap<Integer, GenotypingProject> loadedProjects = new HashMap<Integer, GenotypingProject>();
		ArrayList<Individual> result = new ArrayList<Individual>();
		for (SampleId spId : sampleIDs)
		{
			GenotypingProject project = loadedProjects.get(spId.getProject());
			if (project == null)
			{
				project = mongoTemplate.findById(spId.getProject(), GenotypingProject.class);
				loadedProjects.put(spId.getProject(), project);
			}
			Integer spIndex = spId.getSampleIndex();
			String individual = project.getSamples().get(spIndex).getIndividual();
			result.add(mongoTemplate.findById(individual, Individual.class));
		}
		return result;
	}

	/**
	 * Creates the export files.
	 *
	 * @param sModule the module
	 * @param markerCursor the marker cursor
	 * @param sampleIDs1 the sample ids for group 1
	 * @param sampleIDs2 the sample ids for group 2
	 * @param exportID the export id
	 * @param annotationFieldThresholds the annotation field thresholds for group 1
	 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
	 * @param progress the progress
	 * @return the linked hash map
	 * @throws Exception the exception
	 */
	public TreeMap<String, File> createExportFiles(String sModule, DBCursor markerCursor, List<SampleId> sampleIDs1, List<SampleId> sampleIDs2, String exportID, HashMap<String, Integer> annotationFieldThresholds, HashMap<String, Integer> annotationFieldThresholds2, final ProgressIndicator progress) throws Exception
	{
		long before = System.currentTimeMillis();

		List<String> individuals1 = getIndividualsFromSamples(sModule, sampleIDs1).stream().map(ind -> ind.getId()).collect(Collectors.toList());	
		List<String> individuals2 = getIndividualsFromSamples(sModule, sampleIDs2).stream().map(ind -> ind.getId()).collect(Collectors.toList());

		ArrayList<SampleId> sampleIDs = (ArrayList<SampleId>) CollectionUtils.union(sampleIDs1, sampleIDs2);
		List<Individual> individuals = getIndividualsFromSamples(sModule, sampleIDs);

		HashMap<Object, Integer> individualOutputGenotypeCounts = new HashMap<Object, Integer>();	// will help us to keep track of missing genotypes
		TreeMap<String, File> files = new TreeMap<String, File>(new AlphaNumericComparator<String>());
		int i = 0;
		for (Individual individual : individuals)
			if (!files.containsKey(individual.getId()))
			{
				File file = File.createTempFile(exportID.replaceAll("\\|", "&curren;") +  "-" + individual.getId() + "-",".tsv");
				files.put(individual.getId(), file);
				if (i == 0)
					LOG.debug("First temp file for export " + exportID + ": " + file.getPath());
				files.put(individual.getId(), file);
				BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
				os.write((individual.getId() + LINE_SEPARATOR).getBytes());
				os.close();
				i++;
			}
		
		final MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		int markerCount = markerCursor.count();
		
		short nProgress = 0, nPreviousProgress = 0;
		int avgObjSize = (Integer) mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class)).getStats().get("avgObjSize");
		int nChunkSize = nMaxChunkSizeInMb*1024*1024 / avgObjSize;		
		long nLoadedMarkerCount = 0;
		markerCursor.batchSize(nChunkSize);
		while (markerCursor.hasNext())
		{
			int nLoadedMarkerCountInLoop = 0;
			final Map<Comparable, String> markerChromosomalPositions = new LinkedHashMap<Comparable, String>();
			boolean fStartingNewChunk = true;
			while (markerCursor.hasNext() && (fStartingNewChunk || nLoadedMarkerCountInLoop%nChunkSize != 0)) {
				DBObject exportVariant = markerCursor.next();
				DBObject refPos = (DBObject) exportVariant.get(VariantData.FIELDNAME_REFERENCE_POSITION);
				markerChromosomalPositions.put((Comparable) exportVariant.get("_id"), refPos == null ? null : (refPos.get(ReferencePosition.FIELDNAME_SEQUENCE) + ":" + refPos.get(ReferencePosition.FIELDNAME_START_SITE)));
				nLoadedMarkerCountInLoop++;
				fStartingNewChunk = false;
			}

			HashMap<String, StringBuffer> individualGenotypeBuffers = new HashMap<String, StringBuffer>();	// keeping all files open leads to failure (see ulimit command), keeping them closed and reopening them each time we need to write a genotype is too time consuming: so our compromise is to reopen them only once per chunk
			List<Comparable> currentMarkers = new ArrayList<Comparable>(markerChromosomalPositions.keySet());
			LinkedHashMap<VariantData, Collection<VariantRunData>> variantsAndRuns = MgdbDao.getSampleGenotypes(mongoTemplate, sampleIDs, currentMarkers, true, null /*new Sort(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ChromosomalPosition.FIELDNAME_SEQUENCE).and(new Sort(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ChromosomalPosition.FIELDNAME_START_SITE))*/);	// query mongo db for matching genotypes
			VariantData[] variants = variantsAndRuns.keySet().toArray(new VariantData[variantsAndRuns.size()]);

			for (i=0; i<variantsAndRuns.size(); i++)	// read data and write results into temporary files (one per sample)
			{
				HashMap<String, List<String>> individualGenotypes = new HashMap<String, List<String>>();
				
				long markerIndex = nLoadedMarkerCount + currentMarkers.indexOf(variants[i].getId());
				Collection<VariantRunData> runs = variantsAndRuns.get(variants[i]);
				if (runs != null)
					for (VariantRunData run : runs)
						for (Integer sampleIndex : run.getSampleGenotypes().keySet())
						{
							SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sampleIndex);
							List<String> alleles = variants[i].getAllelesFromGenotypeCode(sampleGenotype.getCode());
							String individualName = individuals.get(sampleIDs.indexOf(new SampleId(run.getId().getProjectId(), sampleIndex))).getId();
							
							if (!VariantData.gtPassesAnnotationFilters(individualName, sampleGenotype, individuals1, annotationFieldThresholds, individuals2, annotationFieldThresholds2))
								continue;	// skip genotype
	
							List<String> storedIndividualGenotypes = individualGenotypes.get(individualName);
							if (storedIndividualGenotypes == null)
							{
								storedIndividualGenotypes = new ArrayList<String>();
								individualGenotypes.put(individualName, storedIndividualGenotypes);
							}
	
							String sAlleles = StringUtils.join(alleles, ' ');
							storedIndividualGenotypes.add(sAlleles);
						}

				for (String individual : individualGenotypes.keySet())
				{
					StringBuffer genotypeBuffer = individualGenotypeBuffers.get(individual);
					if (genotypeBuffer == null)
					{
						genotypeBuffer = new StringBuffer(); 
						individualGenotypeBuffers.put(individual, genotypeBuffer); // we are about to write individual's first genotype for this chunk
					}
					Integer gtCount = Helper.getCountForKey(individualOutputGenotypeCounts, individual);
					while (gtCount < markerIndex)
					{
						genotypeBuffer.append(LINE_SEPARATOR);
						individualOutputGenotypeCounts.put(individual, ++gtCount);
					}
					List<String> storedIndividualGenotypes = individualGenotypes.get(individual);
					for (int j=0; j<storedIndividualGenotypes.size(); j++)
					{
						String storedIndividualGenotype = storedIndividualGenotypes.get(j);
						genotypeBuffer.append(storedIndividualGenotype + (j == storedIndividualGenotypes.size() - 1 ? LINE_SEPARATOR : "|"));
					}
					individualOutputGenotypeCounts.put(individual, gtCount + 1);
				}
			}
			
			// write genotypes collected in this chunk to each individual's file
			for (Individual individual : individuals)
			{
				BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(files.get(individual.getId()), true));
				StringBuffer chunkStringBuffer = individualGenotypeBuffers.get(individual.getId());
				if (chunkStringBuffer != null)
					os.write(chunkStringBuffer.toString().getBytes());
				
				// deal with trailing missing genotypes
				Integer gtCount = Helper.getCountForKey(individualOutputGenotypeCounts, individual.getId());
				while (gtCount < nLoadedMarkerCount + currentMarkers.size())
				{
					os.write(LINE_SEPARATOR.getBytes());
					individualOutputGenotypeCounts.put(individual.getId(), ++gtCount);
				}
				os.close();
			}
			
			if (progress.hasAborted())
				break;

			nLoadedMarkerCount += nLoadedMarkerCountInLoop;			
			nProgress = (short) (nLoadedMarkerCount * 100 / markerCount);
			if (nProgress > nPreviousProgress)
			{
//				LOG.debug("============= createExportFiles: " + nProgress + "% =============");
				progress.setCurrentStepProgress(nProgress);
				nPreviousProgress = nProgress;
			}
		}

	 	progress.setCurrentStepProgress((short) 100);

	 	if (!progress.hasAborted())
	 		LOG.info("createExportFiles took " + (System.currentTimeMillis() - before)/1000d + "s to process " + markerCount + " variants and " + files.size() + " individuals");
		
		return files;
	}
	
	/**
	 * Gets the individual oriented export handlers.
	 *
	 * @return the individual oriented export handlers
	 * @throws ClassNotFoundException the class not found exception
	 * @throws InstantiationException the instantiation exception
	 * @throws IllegalAccessException the illegal access exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws InvocationTargetException the invocation target exception
	 * @throws NoSuchMethodException the no such method exception
	 * @throws SecurityException the security exception
	 */
	public static TreeMap<String, AbstractIndividualOrientedExportHandler> getIndividualOrientedExportHandlers() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
	{
		if (individualOrientedExportHandlers == null)
		{
			individualOrientedExportHandlers = new TreeMap<String, AbstractIndividualOrientedExportHandler>();
			ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
			provider.addIncludeFilter(new AssignableTypeFilter(AbstractIndividualOrientedExportHandler.class));
			try
			{
				for (BeanDefinition component : provider.findCandidateComponents("fr.cirad"))
				{
				    Class cls = Class.forName(component.getBeanClassName());
				    if (!Modifier.isAbstract(cls.getModifiers()))
				    {
						AbstractIndividualOrientedExportHandler exportHandler = (AbstractIndividualOrientedExportHandler) cls.getConstructor().newInstance();
						String sFormat = exportHandler.getExportFormatName();
						AbstractIndividualOrientedExportHandler previouslyFoundExportHandler = individualOrientedExportHandlers.get(sFormat);
						if (previouslyFoundExportHandler != null)
						{
							if (exportHandler.getClass().isAssignableFrom(previouslyFoundExportHandler.getClass()))
							{
								LOG.debug(previouslyFoundExportHandler.getClass().getName() + " implementation was preferred to " + exportHandler.getClass().getName() + " to handle exporting to '" + sFormat + " format");
								continue;	// skip adding the current exportHandler because we already have a "better" one
							}
							else if (previouslyFoundExportHandler.getClass().isAssignableFrom(exportHandler.getClass()))
								LOG.debug(exportHandler.getClass().getName() + " implementation was preferred to " + previouslyFoundExportHandler.getClass().getName() + " to handle exporting to " + sFormat + " format");
							else
								LOG.warn("Unable to choose between " + previouslyFoundExportHandler.getClass().getName() + " and " + exportHandler.getClass().getName() + ". Keeping first found: " + previouslyFoundExportHandler.getClass().getName());
						}
				    	individualOrientedExportHandlers.put(sFormat, exportHandler);
				    }
				}
			}
			catch (Exception e)
			{
				LOG.warn("Error scanning export handlers", e);
			}
		}
		return individualOrientedExportHandlers;
	}

	/* (non-Javadoc)
	 * @see fr.cirad.mgdb.exporting.IExportHandler#getSupportedVariantTypes()
	 */
	@Override
	public List<String> getSupportedVariantTypes()
	{
		return null;	// means any type
	}
}
