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
package fr.cirad.mgdb.exporting.markeroriented;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCursor;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class AbstractMarkerOrientedExportHandler.
 */
public abstract class AbstractMarkerOrientedExportHandler implements IExportHandler
{

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(AbstractMarkerOrientedExportHandler.class);

	/** The marker oriented export handlers. */
	static private TreeMap<String, AbstractMarkerOrientedExportHandler> markerOrientedExportHandlers = null;

	/**
	 * Export data.
	 *
	 * @param outputStream the output stream
	 * @param sModule the module
	 * @param sampleIDs the sample i ds
	 * @param progress the progress
	 * @param markerCursor the marker cursor
	 * @param markerSynonyms the marker synonyms
	 * @param nMinimumReadDepth the n minimum read depth
	 * @param readDepthThreshold the read depth threshold
	 * @param readyToExportFiles the ready to export files
	 * @throws Exception the exception
	 */
	abstract public void exportData(OutputStream outputStream, String sModule, List<SampleId> sampleIDs, ProgressIndicator progress, DBCursor markerCursor, Map<Comparable, Comparable> markerSynonyms, int nMinimumReadDepth, int readDepthThreshold, Map<String, InputStream> readyToExportFiles) throws Exception;

	/**
	 * Gets the individuals from samples.
	 *
	 * @param sModule the module
	 * @param sampleIDs the sample i ds
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
	 * Gets the marker oriented export handlers.
	 *
	 * @return the marker oriented export handlers
	 * @throws ClassNotFoundException the class not found exception
	 * @throws InstantiationException the instantiation exception
	 * @throws IllegalAccessException the illegal access exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws InvocationTargetException the invocation target exception
	 * @throws NoSuchMethodException the no such method exception
	 * @throws SecurityException the security exception
	 */
	public static TreeMap<String, AbstractMarkerOrientedExportHandler> getMarkerOrientedExportHandlers() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
	{
		if (markerOrientedExportHandlers == null)
		{
			markerOrientedExportHandlers = new TreeMap<String, AbstractMarkerOrientedExportHandler>();
			ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
			provider.addIncludeFilter(new AssignableTypeFilter(AbstractMarkerOrientedExportHandler.class));
			try
			{
				for (BeanDefinition component : provider.findCandidateComponents("fr.cirad"))
				{
				    Class cls = Class.forName(component.getBeanClassName());
				    if (!Modifier.isAbstract(cls.getModifiers()))
				    {
						AbstractMarkerOrientedExportHandler exportHandler = (AbstractMarkerOrientedExportHandler) cls.getConstructor().newInstance();
						String sFormat = exportHandler.getExportFormatName();
						AbstractMarkerOrientedExportHandler previouslyFoundExportHandler = markerOrientedExportHandlers.get(sFormat);
						if (previouslyFoundExportHandler != null)
						{
							if (exportHandler.getClass().isAssignableFrom(previouslyFoundExportHandler.getClass()))
							{
								LOG.debug(previouslyFoundExportHandler.getClass().getName() + " implementation was preferred to " + exportHandler.getClass().getName() + " to handle exporting to '" + sFormat + "' format");
								continue;	// skip adding the current exportHandler because we already have a "better" one
							}
							else if (previouslyFoundExportHandler.getClass().isAssignableFrom(exportHandler.getClass()))
								LOG.debug(exportHandler.getClass().getName() + " implementation was preferred to " + previouslyFoundExportHandler.getClass().getName() + " to handle exporting to " + sFormat + "' format");
							else
								LOG.warn("Unable to choose between " + previouslyFoundExportHandler.getClass().getName() + " and " + exportHandler.getClass().getName() + ". Keeping first found: " + previouslyFoundExportHandler.getClass().getName());
						}
				    	markerOrientedExportHandlers.put(sFormat, exportHandler);
				    }
				}
			}
			catch (Exception e)
			{
				LOG.warn("Error scanning export handlers", e);
			}
		}
		return markerOrientedExportHandlers;
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
