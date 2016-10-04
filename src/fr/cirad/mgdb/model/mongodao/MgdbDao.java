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
package fr.cirad.mgdb.model.mongodao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.tools.Helper;

// TODO: Auto-generated Javadoc
/**
 * The Class MgdbDao.
 */
public class MgdbDao extends Helper
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(MgdbDao.class);
	
	/** The Constant COLLECTION_NAME_TAGGED_VARIANT_IDS. */
	static final public String COLLECTION_NAME_TAGGED_VARIANT_IDS = "taggedVariants";
	
	/** The Constant COLLECTION_NAME_CACHED_COUNTS. */
	static final public String COLLECTION_NAME_CACHED_COUNTS = "cachedCounts";
	
	/** The Constant FIELD_NAME_CACHED_COUNT_VALUE. */
	static final public String FIELD_NAME_CACHED_COUNT_VALUE = "val";
	
	/**
	 * Prepare database for searches.
	 *
	 * @param mongoTemplate the mongo template
	 * @return the list
	 */
	public static List<Comparable> prepareDatabaseForSearches(MongoTemplate mongoTemplate)
	{
		// empty count cache
		mongoTemplate.dropCollection(COLLECTION_NAME_CACHED_COUNTS);
		
		DBCollection variantColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class));
		DBCollection runColl = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class));

		// create indexes
		LOG.debug("Creating index on field " + VariantData.FIELDNAME_TYPE + " of collection " + variantColl.getName());
		variantColl.createIndex(VariantData.FIELDNAME_TYPE);
		LOG.debug("Creating index on field " + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE + " of collection " + variantColl.getName());
		variantColl.createIndex(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE);
		LOG.debug("Creating index on fields _id." + VariantRunDataId.FIELDNAME_VARIANT_ID + ", _id." + VariantRunDataId.FIELDNAME_PROJECT_ID + " of collection " + runColl.getName());
		BasicDBObject runCollIndexKeys = new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, 1);
		runCollIndexKeys.put("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, 1);
		runColl.createIndex(runCollIndexKeys);
		
		// tag variant IDs across database
		List<Comparable> result = new ArrayList<Comparable>();
		mongoTemplate.dropCollection(COLLECTION_NAME_TAGGED_VARIANT_IDS);
		long totalVariantCount = mongoTemplate.count(new Query(), VariantData.class);
		long totalIndividualCount = mongoTemplate.count(new Query(), Individual.class);
		long maxGenotypeCount = totalVariantCount*totalIndividualCount;
		long numberOfTaggedVariants = Math.min(totalVariantCount / 2, maxGenotypeCount > 200000000 ? 300 : (maxGenotypeCount > 100000000 ? 200 : (maxGenotypeCount > 50000000 ? 100 : (maxGenotypeCount > 20000000 ? 50 : (maxGenotypeCount > 5000000 ? 40 : 30)))));
		int nChunkSize = (int) Math.max(1, (int) totalVariantCount / (numberOfTaggedVariants - 1));
		LOG.debug("Number of variants between 2 tagged ones: " + nChunkSize);
		
		DBCollection collection = mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_TAGGED_VARIANT_IDS);
		Comparable cursor = null;
		for (int nChunkNumber=0; nChunkNumber<(float) totalVariantCount / nChunkSize; nChunkNumber++)
		{
			long before = System.currentTimeMillis();
			Query q = new Query();
			q.fields().include("_id");
			q.limit(nChunkSize);
			q.with(new Sort(Sort.Direction.ASC, "_id"));
			if (cursor != null)
				q.addCriteria(Criteria.where("_id").gt(ObjectId.isValid(cursor.toString()) ? new ObjectId(cursor.toString()) : cursor));
			List<VariantData> chunk = mongoTemplate.find(q, VariantData.class);
			cursor = chunk.get(chunk.size() - 1).getId();
			collection.save(new BasicDBObject("_id", cursor));
			result.add(cursor.toString());
			LOG.debug("Variant " + cursor + " tagged as position " + nChunkNumber + " (" + (System.currentTimeMillis() - before) + "ms)");
		}
		return result;
	}
	
	/**
	 * Estimate number of variants to query at once.
	 *
	 * @param totalNumberOfMarkersToQuery the total number of markers to query
	 * @param nNumberOfWantedGenotypes the n number of wanted genotypes
	 * @return the int
	 * @throws Exception the exception
	 */
	public static int estimateNumberOfVariantsToQueryAtOnce(int totalNumberOfMarkersToQuery, int nNumberOfWantedGenotypes) throws Exception
	{
		if (totalNumberOfMarkersToQuery <= 0)
			throw new Exception("totalNumberOfMarkersToQuery must be >0");
		
		int nSampleCount = Math.max(1 /*in case someone would pass 0 or less*/, nNumberOfWantedGenotypes);
		int nResult = Math.max(1, 200000/nSampleCount);
		
		return Math.min(nResult, totalNumberOfMarkersToQuery);
	}
	
	/**
	 * Gets the sample genotypes.
	 *
	 * @param mongoTemplate the mongo template
	 * @param variantFieldsToReturn the variant fields to return
	 * @param projectIdToReturnedRunFieldListMap the project id to returned run field list map
	 * @param variantIdListToRestrictTo the variant id list to restrict to
	 * @param sort the sort
	 * @return the sample genotypes
	 * @throws Exception the exception
	 */
	private static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, ArrayList<String> variantFieldsToReturn, HashMap<Integer, ArrayList<String>> projectIdToReturnedRunFieldListMap, List<? extends Comparable> variantIdListToRestrictTo, Sort sort) throws Exception
	{
		Query variantQuery = new Query();
		Criteria runQueryVariantCriteria = null;

//		query.with(sort == null ? new Sort(Direction.ASC, "_id") : sort);
		if (sort != null)
			variantQuery.with(sort);
		if (variantIdListToRestrictTo != null && variantIdListToRestrictTo.size() > 0)
		{
			List<Object> possiblyModifiedVariantIdList = new ArrayList<Object>();
			for (Comparable id : variantIdListToRestrictTo)
				possiblyModifiedVariantIdList.add(ObjectId.isValid(id.toString()) ? new ObjectId(id.toString()) : id);
			variantQuery.addCriteria(new Criteria().where("_id").in(possiblyModifiedVariantIdList));
			runQueryVariantCriteria = new Criteria().where("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).in(possiblyModifiedVariantIdList);
		}
		variantQuery.fields().include("_id");
		for (String returnedField : variantFieldsToReturn)
			variantQuery.fields().include(returnedField);
		
		LinkedHashMap<Comparable, VariantData> variantIdToVariantMap = new LinkedHashMap<Comparable, VariantData>();		
		List<VariantData> variants = mongoTemplate.find(variantQuery, VariantData.class);
		for (VariantData vd : variants)
			variantIdToVariantMap.put(vd.getId(), vd);
		
		// next block may be removed at some point (only some consistency checking)
		if (variantIdListToRestrictTo != null && variantIdListToRestrictTo.size() != variants.size())
		{
			mainLoop : for (Comparable vi : variantIdListToRestrictTo)
			{
				for (VariantData vd : variants)
				{
					if (!variantIdToVariantMap.containsKey(vd.getId()))
						variantIdToVariantMap.put(vd.getId(), vd);
					
					if (vd.getId().equals(vi))
						continue mainLoop;
				}
				LOG.error(vi + " requested but not returned");
			}
			throw new Exception("Found " + variants.size() + " variants where " + variantIdListToRestrictTo.size() + " were expected");
		}
		else if ((variantIdListToRestrictTo != null && variantIdListToRestrictTo.size() > 0) && variants.size() > 1)
		{
			if (!variants.get(0).getId().toString().equals(variantIdListToRestrictTo.get(0).toString()))
				throw new Exception("First returned variant (" + variants.get(0).getId() + ") differs from first requested variant (" +variantIdListToRestrictTo.get(0) + ")");
			if (!variants.get(variants.size() - 1).getId().toString().equals(variantIdListToRestrictTo.get(variants.size() - 1).toString()))
				throw new Exception("Last returned variant (" + variants.get(variants.size() - 1).getId() + ") differs from last requested variant (" +variantIdListToRestrictTo.get(variants.size() - 1) + ")");
		}
			
		LinkedHashMap<VariantData, Collection<VariantRunData>> result = new LinkedHashMap<VariantData, Collection<VariantRunData>>();
		for (Comparable variantId : variantIdListToRestrictTo)
			result.put(variantIdToVariantMap.get(ObjectId.isValid(variantId.toString()) ? new ObjectId(variantId.toString()) : variantId), null);

		for (int projectId : projectIdToReturnedRunFieldListMap.keySet())
		{
			Query runQuery = new Query();
			if (runQueryVariantCriteria != null)
				runQuery.addCriteria(runQueryVariantCriteria);
			runQuery.addCriteria(new Criteria().where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(projectId));

			runQuery.fields().include("_id");
			for (String returnedField : projectIdToReturnedRunFieldListMap.get(projectId))
				runQuery.fields().include(returnedField);
			
			List<VariantRunData> runs = mongoTemplate.find(runQuery, VariantRunData.class);
			for (VariantRunData run : runs)
			{
				Comparable variantId = run.getId().getVariantId();
				VariantData variant = variantIdToVariantMap.get(variantId);
				Collection<VariantRunData> variantRuns = result.get(variant);
				if (variantRuns == null)
				{
					variantRuns = new ArrayList<VariantRunData>();
					result.put(variant, variantRuns);
				}
				variantRuns.add(run);
			}
		}
	
		if (result.size() != variantIdListToRestrictTo.size())
			throw new Exception("Bug: we should be returning " + variantIdListToRestrictTo.size() + " results but we only have " + result.size());

		return result;
	}
	
	/**
	 * Gets the sample genotypes.
	 *
	 * @param mongoTemplate the mongo template
	 * @param sampleIDs the sample i ds
	 * @param variantIdListToRestrictTo the variant id list to restrict to
	 * @param fReturnVariantTypes whether or not to return variant types
	 * @param sort the sort
	 * @return the sample genotypes
	 * @throws Exception the exception
	 */
	public static LinkedHashMap<VariantData, Collection<VariantRunData>> getSampleGenotypes(MongoTemplate mongoTemplate, List<SampleId> sampleIDs, List<? extends Comparable> variantIdListToRestrictTo, boolean fReturnVariantTypes, Sort sort) throws Exception
	{
		ArrayList<String> variantFieldsToReturn = new ArrayList<String>();
		variantFieldsToReturn.add(VariantData.FIELDNAME_KNOWN_ALLELE_LIST);
		variantFieldsToReturn.add(VariantData.FIELDNAME_REFERENCE_POSITION);
		if (fReturnVariantTypes)
			variantFieldsToReturn.add(VariantData.FIELDNAME_TYPE);
		
		HashMap<Integer /*project id*/, ArrayList<String>> projectIdToReturnedRunFieldListMap = new HashMap<Integer, ArrayList<String>>();
		for (SampleId sampleID : sampleIDs)
		{
			ArrayList<String> returnedFields = projectIdToReturnedRunFieldListMap.get(sampleID.getProject());
			if (returnedFields == null)
			{
				returnedFields = new ArrayList<String>();
				returnedFields.add(/*VariantData.FIELDNAME_PROJECT_DATA + "." + sampleID.getProject() + "." + */"_class");
				returnedFields.add(/*VariantData.FIELDNAME_PROJECT_DATA + "." + sampleID.getProject() + "." + */VariantRunData.SECTION_ADDITIONAL_INFO);
				projectIdToReturnedRunFieldListMap.put(sampleID.getProject(), returnedFields);
			}
			returnedFields.add(/*VariantData.FIELDNAME_PROJECT_DATA + "." + sampleID.getProject() + "." + */VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + sampleID.getSampleIndex());
		}

		LinkedHashMap<VariantData, Collection<VariantRunData>> result = getSampleGenotypes(mongoTemplate, variantFieldsToReturn, projectIdToReturnedRunFieldListMap, variantIdListToRestrictTo, sort);
		
		return result;
	}
}