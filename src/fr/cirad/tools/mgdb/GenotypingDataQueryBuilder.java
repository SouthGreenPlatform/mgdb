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
package fr.cirad.tools.mgdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.math.util.MathUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class GenotypingDataQueryBuilder.
 */
public class GenotypingDataQueryBuilder implements Iterator<List<DBObject>>
{
	
	/** The Constant LOG. */
	protected static final Logger LOG = Logger.getLogger(GenotypingDataQueryBuilder.class);
	
	/** The mongo template. */
	private MongoTemplate mongoTemplate;
	
	/** The genotyping project. */
	private GenotypingProject genotypingProject;
	
	/** The individual index to sample list map. */
	private HashMap<Integer, List<Integer>> individualIndexToSampleListMap;
	
	/** The variant cursor. */
	private DBCursor variantCursor; 
	
	/** The operator. */
	private String operator;
	
	/** The genotype quality threshold. */
	private Integer genotypeQualityThreshold;
	
	/** The read depth threshold. */
	private Integer readDepthThreshold;
	
	/** The missing data. */
	private Double missingData;
	
	/** The minmaf. */
	private Float minmaf;
	
	/** The maxmaf. */
	private Float maxmaf;
	
	/** The gene names. */
	private String geneNames;
	
	/** The variant effects. */
	private String variantEffects;
	
	/** The selected individuals. */
	private List<String> selectedIndividuals;
	
	/** The project effect annotations. */
	private Collection<String> projectEffectAnnotations;
	
	/** The fields to return. */
	private Collection<String> fieldsToReturn;
	
	/** The n total variant count. */
	private long nTotalVariantCount = 0;
	
	/** The n variants queried at once. */
	private int nNVariantsQueriedAtOnce = 0;	/* will remain so if we are working on the full dataset */
	
	private int nNextCallCount = 0;
	
	private int maxAlleleCount = 0;
	
	/** The current tagged variant. */
	private Comparable currentTaggedVariant = null;

	private List<List<Comparable>> preFilteredIDs = new ArrayList();

	private boolean fKeepTrackOfPreFilters = false;
	
	/** The Constant MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE. */
	static final private int MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE = 350000;
	
	/** The Constant NUMBER_OF_SIMULTANEOUS_QUERY_THREADS. */
	static final private int NUMBER_OF_SIMULTANEOUS_QUERY_THREADS = 5;

	/** The Constant AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX. */
	static final public String AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX = "_ALL_"; // used to differentiate aggregation query with $and operator 
	
	/** The Constant AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX. */
	static final public String AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX = "_ATLO_";  // used to differentiate find query with $or operator
	
	/** The Constant AGGREGATION_QUERY_NEGATION_SUFFIX. */
	static final public String AGGREGATION_QUERY_NEGATION_SUFFIX = "_NEG_";	// used to indicate that the match operator should be negated in the aggregation query
	
	/** The Constant AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY. */
	static final public String AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY = "WITHOUT_ABNORMAL_HETEROZYGOSITY";

	/** The Constant GENOTYPE_CODE_LABEL_ALL. */
	static final public String GENOTYPE_CODE_LABEL_ALL = "Any";

	/** The Constant GENOTYPE_CODE_LABEL_NOT_ALL_SAME. */
	static final public String GENOTYPE_CODE_LABEL_NOT_ALL_SAME = "Not all same";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_SAME. */
	static final public String GENOTYPE_CODE_LABEL_ALL_SAME = "All same";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_DIFFERENT. */
	static final public String GENOTYPE_CODE_LABEL_ALL_DIFFERENT = "All different";

	/** The Constant GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT. */
	static final public String GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT = "Not all different";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF = "All Homozygous Ref";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF = "At least one Homozygous Ref";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR = "All Homozygous Var";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR = "At least one Homozygous Var";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS = "All Heterozygous";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS = "At least one Heterozygous";

	/** The Constant GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY. */
	static final public String GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY = "Without abnormal heterozygosity";

	/** The Constant genotypeCodeToDescriptionMap. */
	static final private HashMap<String, String> genotypeCodeToDescriptionMap = new LinkedHashMap<String, String>();

	/** The Constant genotypeCodeToQueryMap. */
	static final private HashMap<String, String> genotypeCodeToQueryMap = new HashMap<String, String>();	

	static
	{
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL, "This will return all variants whithout applying any filters");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_SAME, "This will return variants where not all selected individuals have the same genotype");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_SAME, "This will return variants where all selected individuals have the same genotype");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_DIFFERENT, "This will return variants where none of the selected individuals have the same genotype");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT, "This will return variants where some of the selected individuals have the same genotypes");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF, "This will return variants where selected individuals are all homozygous with the reference allele");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF, "This will return variants where selected individuals are at least one homozygous with the reference allele");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR, "This will return variants where selected individuals are all homozygous with an alternate allele");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR, "This will return variants where selected individuals are at least one homozygous with an alternate allele");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS, "This will return variants where selected individuals are all heterozygous");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS, "This will return variants where selected individuals are at least one heterozygous");
		genotypeCodeToDescriptionMap.put(GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY, "This will return variants where each allele found in heterozygous genotypes is also found in homozygous ones (only for diploid, bi-allelic data)");
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL, null);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_SAME, "$eq");
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_SAME, "$eq" + GenotypingDataQueryBuilder.AGGREGATION_QUERY_NEGATION_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_DIFFERENT, "$ne");
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT, "$ne" + GenotypingDataQueryBuilder.AGGREGATION_QUERY_NEGATION_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF, "^0(/0)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF, "^0(/0)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR, "^([1-9][0-9]*)(/\\1)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR, "^([1-9][0-9]*)(/\\1)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS, "([0-9])([0-9])*(/(?!\\1))+([0-9])*"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS, "([0-9])([0-9])*(/(?!\\1))+([0-9])*"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypeCodeToQueryMap.put(GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY, GenotypingDataQueryBuilder.AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY);
	}
	
	/**
	 *  
	 *
	 * @param sModule the module
	 * @param projId the proj id
	 * @param tempExportColl the temp export coll
	 * @param operator the operator
	 * @param genotypeQualityThreshold the genotype quality threshold
	 * @param readDepthThreshold the read depth threshold
	 * @param missingData the missing data
	 * @param minmaf the minmaf
	 * @param maxmaf the maxmaf
	 * @param geneNames the gene names
	 * @param variantEffects the variant effects
	 * @param selectedIndividuals the selected individuals
	 * @param projectEffectAnnotations the project effect annotations
	 * @param fieldsToReturn the fields to return
	 */
	public GenotypingDataQueryBuilder(String sModule, int projId, DBCollection tempExportColl, String operator, Integer genotypeQualityThreshold, Integer readDepthThreshold, Double missingData, Float minmaf, Float maxmaf, String geneNames, String variantEffects, List<String> selectedIndividuals, Collection<String> projectEffectAnnotations, Collection<String> fieldsToReturn)
	{
		this.mongoTemplate = MongoTemplateManager.get(sModule);
		this.operator = operator;
		this.genotypeQualityThreshold = genotypeQualityThreshold;
		this.readDepthThreshold = readDepthThreshold;
		this.missingData = missingData;
		this.minmaf = minmaf;
		this.maxmaf = maxmaf;
		this.geneNames = geneNames;
		this.variantEffects = variantEffects;
		this.selectedIndividuals = selectedIndividuals;
		this.projectEffectAnnotations = projectEffectAnnotations;
		this.fieldsToReturn = fieldsToReturn;
		this.genotypingProject = (GenotypingProject) mongoTemplate.findById(Integer.valueOf(projId), GenotypingProject.class);
		this.individualIndexToSampleListMap = new HashMap<Integer, List<Integer>>();
		
		int nSampleCount = 0;		
		for (int k=0; k<selectedIndividuals.size(); k++)
		{
			List<Integer> sampleIndexes = genotypingProject.getIndividualSampleIndexes(selectedIndividuals.get(k));
			individualIndexToSampleListMap.put(k, sampleIndexes);
			nSampleCount += sampleIndexes.size();
		}
				
		this.nTotalVariantCount = tempExportColl.count();
		if (this.nTotalVariantCount == 0)
		{
			DBCollection taggedVarColl = mongoTemplate.getCollection(MgdbDao.COLLECTION_NAME_TAGGED_VARIANT_IDS);
			this.nTotalVariantCount = taggedVarColl.count() + 1;
			if (this.nTotalVariantCount == 1)
			{	// list does not exist: create it
				MgdbDao.prepareDatabaseForSearches(mongoTemplate);
				this.nTotalVariantCount = taggedVarColl.count() + 1;
			}
			this.variantCursor = taggedVarColl.find()/*.addOption(Bytes.QUERYOPTION_NOTIMEOUT)*/;
		}
		else
		{
	    	// we may need to split the query into several parts if the number of input variants is large (otherwise the query may exceed 16Mb)
			this.nNVariantsQueriedAtOnce = MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE / Math.max(1, nSampleCount * (NUMBER_OF_SIMULTANEOUS_QUERY_THREADS / 2));
			this.variantCursor = tempExportColl.find()/*.addOption(Bytes.QUERYOPTION_NOTIMEOUT)*/;
		}
    }
	
	public List<Comparable> getPreFilteredIDsForChunk(int n)
	{
		return preFilteredIDs.get(n);
	}
	
	public boolean isKeepingTrackOfPreFilters() {
		return fKeepTrackOfPreFilters;
	}

	public void keepTrackOfPreFilters(boolean fKeepTrackOfPreFilters) throws Exception {
		if (fKeepTrackOfPreFilters && nNVariantsQueriedAtOnce == 0)
			throw new Exception("Keeping track of pre-filters can only be enabled on data where a preliminary, variant-level, filter was applied");

		this.fKeepTrackOfPreFilters = fKeepTrackOfPreFilters;
	}
	
	/**
	 * Gets the number of queries.
	 *
	 * @return the number of queries
	 */
	public int getNumberOfQueries()
	{
		return (int) (nNVariantsQueriedAtOnce > 0 ? (nTotalVariantCount / nNVariantsQueriedAtOnce) + 1 : nTotalVariantCount);
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove()
	{
		throw new UnsupportedOperationException("Removal not supported");
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext()
	{
		return variantCursor.hasNext() || currentTaggedVariant != null;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public List<DBObject> next()
	{
		nNextCallCount++;
		boolean fZygosityRegex = false;
		boolean fIsWithoutAbnormalHeterozygosityQuery = false;
		boolean fNegateMatch = false;
		boolean fOr = false;
		String cleanOperator = operator;
		if (cleanOperator != null)
		{
			if (cleanOperator.endsWith(AGGREGATION_QUERY_NEGATION_SUFFIX))
			{
				fNegateMatch = true;
				cleanOperator = cleanOperator.substring(0, cleanOperator.length() - AGGREGATION_QUERY_NEGATION_SUFFIX.length());
			}
			else if (cleanOperator.endsWith(AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX)) {
				fZygosityRegex = true;
				cleanOperator = cleanOperator.substring(0, cleanOperator.length() - AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX.length());
			}
			else if (cleanOperator.endsWith(AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX)) {
				fZygosityRegex = true;
				fOr = true;
				cleanOperator = cleanOperator.substring(0, cleanOperator.length() - AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX.length());
			}
			else if (cleanOperator.equals(AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY)) {
				fIsWithoutAbnormalHeterozygosityQuery = true;
				cleanOperator = cleanOperator.substring(0, cleanOperator.length() - AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY.length());
			}
		}
				        
		List<DBObject> pipeline = new ArrayList<DBObject>();
		BasicDBList initialMatchList = new BasicDBList();
		pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", initialMatchList)));

		if ("$ne".equals(cleanOperator) && !fNegateMatch)
        {
	        int nMaxNumberOfAllelesForOneVariant = maxAlleleCount > 0 ? maxAlleleCount : genotypingProject.getAlleleCounts().last(), nPloidy = genotypingProject.getPloidyLevel();
	        int nNumberOfPossibleGenotypes = (int) (nMaxNumberOfAllelesForOneVariant + MathUtils.factorial(nMaxNumberOfAllelesForOneVariant)/(MathUtils.factorial(nPloidy)*MathUtils.factorial(nMaxNumberOfAllelesForOneVariant-nPloidy)) + (missingData != null && missingData >= 100/selectedIndividuals.size() ? 1 : 0));
	        if (selectedIndividuals.size() > nNumberOfPossibleGenotypes)
	        {
	        	initialMatchList.add(new BasicDBObject("_id", null));	// return no results
	        	if (nNextCallCount == 1)
		        	LOG.warn("Aborting 'all different' filter (more individuals than possible genotypes)");
	        	return pipeline;
	        }
        }
		
        if (mongoTemplate.count(null, GenotypingProject.class) != 1)
			initialMatchList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, genotypingProject.getId()));
    	
        if (nNVariantsQueriedAtOnce == 0)
        {	// splitting using tagged variants because we have no filter on variant features
        	BasicDBList chunkMatchAndList = new BasicDBList();
        	Comparable leftBound = null, rightBound = null;
			if (currentTaggedVariant != null)
			{
				leftBound = ObjectId.isValid(currentTaggedVariant.toString()) ? new ObjectId(currentTaggedVariant.toString()) : currentTaggedVariant;
				chunkMatchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$gt", leftBound)));
			}
			currentTaggedVariant = variantCursor.hasNext() ? (Comparable) variantCursor.next().get("_id") : null;
			if (currentTaggedVariant != null)
			{
				rightBound = ObjectId.isValid(currentTaggedVariant.toString()) ? new ObjectId(currentTaggedVariant.toString()) : currentTaggedVariant;
				chunkMatchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$lte", rightBound)));
			}
			
			initialMatchList.add(chunkMatchAndList.size() > 1 ? new BasicDBObject("$and", chunkMatchAndList) : (BasicDBObject) chunkMatchAndList.iterator().next());
        }
        else
        {	// splitting using previously applied variant feature filter
	        ArrayList<Comparable> aVariantSubList = new ArrayList<Comparable>();	
	    	int variantIndex = 0;
	    	while (variantCursor.hasNext() && variantIndex<nNVariantsQueriedAtOnce)
	    	{
	    		aVariantSubList.add((Comparable) variantCursor.next().get("_id"));
	    		variantIndex++;
	    	}
	    	
	        if (/*aVariantSubList.size() > 0 && */fKeepTrackOfPreFilters)
	        	preFilteredIDs.add(aVariantSubList);
	        initialMatchList.add(tryAndShrinkIdList("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, aVariantSubList, 4));
        }

		/* Step to match variants according to annotations */			
		if (projectEffectAnnotations.size() > 0 && (geneNames.length() > 0 || variantEffects.length() > 0))
		{
			if (geneNames.length() > 0)
			{
				BasicDBObject geneNameDBO;
				if ("-".equals(geneNames))
					geneNameDBO = new BasicDBObject("$in", new String[] {"", null});
				else if ("+".equals(geneNames))
					geneNameDBO = new BasicDBObject("$regex", ".*");
				else
					geneNameDBO = new BasicDBObject("$in", Helper.split(geneNames, ","));
				initialMatchList.add(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, geneNameDBO));
			}
			if (variantEffects.length() > 0)
				initialMatchList.add(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, new BasicDBObject("$in", Helper.split(variantEffects, ","))));
        }
		
        boolean fMultiRunProject = genotypingProject.getRuns().size() > 1;
        
        DBObject project = new BasicDBObject();
        if (fieldsToReturn.size() > 0)
        {
			for (String field : fieldsToReturn)
				if (field != null && field.length() > 0)
					project.put(field.replaceAll("\\.", "¤"), "$" + field);	        
        }
        
        boolean fMafApplied = maxmaf != null && maxmaf.floatValue() < 50F || minmaf != null && minmaf.floatValue() > 0.0F;
        boolean fMissingDataApplied = missingData != null && missingData < 100;
        boolean fCompareBetweenGenotypes = cleanOperator != null && !fZygosityRegex && !fIsWithoutAbnormalHeterozygosityQuery;
        if ("$ne".equals(cleanOperator) && fNegateMatch)
        {
        	int nMaxNumberOfAllelesForOneVariant = maxAlleleCount > 0 ? maxAlleleCount : genotypingProject.getAlleleCounts().last(), nPloidy = genotypingProject.getPloidyLevel();
	        int nNumberOfPossibleGenotypes = (int) (nMaxNumberOfAllelesForOneVariant + MathUtils.factorial(nMaxNumberOfAllelesForOneVariant)/(MathUtils.factorial(nPloidy)*MathUtils.factorial(nMaxNumberOfAllelesForOneVariant-nPloidy)) + (missingData != null && missingData >= 100/selectedIndividuals.size() ? 1 : 0));
	        if (selectedIndividuals.size() > nNumberOfPossibleGenotypes)
	        {
	        	fCompareBetweenGenotypes = false;	// we know this applying this filter would not affect the query
	        	if (nNextCallCount == 1)
		        	LOG.warn("Ignoring 'not all different' filter (more individuals than possible genotypes)");
	        }
        }

		DBObject groupFields = new BasicDBObject("_id", "$_id." + VariantRunDataId.FIELDNAME_VARIANT_ID); // group multi-run records by variant id
    	BasicDBObject vars = new BasicDBObject();
    	BasicDBObject in = new BasicDBObject();
    	BasicDBList altAlleleCountList = new BasicDBList();
        BasicDBList missingGenotypeCountList = new BasicDBList();
        BasicDBList distinctGenotypeList = new BasicDBList();
        
		for (int j=0; j<selectedIndividuals.size(); j++)
		{
			BasicDBList individualSampleGenotypeList = new BasicDBList();
			List<Integer> individualSamples = individualIndexToSampleListMap.get(j);
			BasicDBList conditionsWhereGqOrDpIsTooLow = new BasicDBList();

			for (int k=0; k<individualSamples.size(); k++)	// this loop is executed only once for single-run projects
	    	{
				Integer individualSample = individualSamples.get(k);
				String pathToGT = individualSample + "." + SampleGenotype.FIELDNAME_GENOTYPECODE;
				groupFields.put(pathToGT.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT));
				individualSampleGenotypeList.add("$" + pathToGT.replaceAll("\\.", "¤"));
        		
				if (genotypeQualityThreshold != null && genotypeQualityThreshold > 1)
				{
					String pathToGQ = individualSample + "." + SampleGenotype.SECTION_ADDITIONAL_INFO + "." + VariantData.GT_FIELD_GQ;
					groupFields.put(pathToGQ.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGQ));
					
					BasicDBList qualTooLowList = new BasicDBList();
					qualTooLowList.add(fMultiRunProject ? new BasicDBObject("$arrayElemAt", new Object[] {"$" + pathToGQ.replaceAll("\\.", "¤"), 0}) : ("$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGQ));
					qualTooLowList.add(genotypeQualityThreshold);

					BasicDBObject qualTooLow = new BasicDBObject("$lt", qualTooLowList);
					conditionsWhereGqOrDpIsTooLow.add(qualTooLow);
				}
				if (readDepthThreshold != null && readDepthThreshold > 1)
				{
					String pathToDP = individualSample + "." + SampleGenotype.SECTION_ADDITIONAL_INFO + "." + VariantData.GT_FIELD_DP;
					groupFields.put(pathToDP.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToDP));

					BasicDBList depthTooLowList = new BasicDBList();
					depthTooLowList.add(fMultiRunProject ? new BasicDBObject("$arrayElemAt", new Object[] {"$" + pathToDP.replaceAll("\\.", "¤"), 0}) : ("$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToDP));
					depthTooLowList.add(readDepthThreshold);

					BasicDBObject depthTooLow = new BasicDBObject("$lt", depthTooLowList);
					conditionsWhereGqOrDpIsTooLow.add(depthTooLow);
				}

		        if (k > 0)
					continue;	// the remaining code in this loop must only be executed once
				
		        Object possiblyConstrainedPathToGT = conditionsWhereGqOrDpIsTooLow.size() == 0 ? "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT : new BasicDBObject("$cond", new Object[] {new BasicDBObject("$or", conditionsWhereGqOrDpIsTooLow), "", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT});
                if (fMafApplied)
                {	// count alternate alleles
                	BasicDBList condList = new BasicDBList();
                    String allRefGtCode = genotypingProject.getPloidyLevel() != 1 ? "0/0" : "0";
                    condList.add(new BasicDBObject("$eq", new Object[] {fMultiRunProject ? ("$$u" + j) : (possiblyConstrainedPathToGT), fMultiRunProject ? new Object[] {allRefGtCode} : allRefGtCode}));
                    condList.add(0);
                    if (genotypingProject.getPloidyLevel() == 1)
                        condList.add(2);
                    else
                    	condList.add(new BasicDBObject("$add", new Object[] {1, new BasicDBObject("$cmp",  new Object[] {fMultiRunProject ? ("$$u" + j) : (possiblyConstrainedPathToGT), fMultiRunProject ? new Object[] {"0/1"} : "0/1"})}));
                    altAlleleCountList.add(new BasicDBObject("$cond", condList));
                }

                if (fMissingDataApplied || fMafApplied || fCompareBetweenGenotypes)
                {	// count missing genotypes
                	if (fMultiRunProject)
                		missingGenotypeCountList.add(new BasicDBObject("$abs", new BasicDBObject("$cmp", new Object[] {new BasicDBObject("$size", "$$u" + j), 1})));
                	else
                	{
                		BasicDBObject missingGtCalculation = new BasicDBObject("$add", new Object[] {1, new BasicDBObject("$cmp", new Object[] {"", new BasicDBObject("$ifNull", new Object[] {possiblyConstrainedPathToGT, ""})})});
       					missingGenotypeCountList.add(missingGtCalculation);
                	}
                }
                
                if (fCompareBetweenGenotypes || fZygosityRegex || fIsWithoutAbnormalHeterozygosityQuery)
                {	// count distinct non-missing genotypes
                	if (fMultiRunProject)
                	{
	                	BasicDBList condList = new BasicDBList();
	                    condList.add(new BasicDBObject("$eq", new Object[] {1, new BasicDBObject("$size", "$$u" + j)}));
	                    condList.add("$$u" + j);
	                    condList.add(new Object[0]);
	                    distinctGenotypeList.add(new BasicDBObject("$cond", condList));
                	}
                	else
                		distinctGenotypeList.add(new Object[] {possiblyConstrainedPathToGT});
                }
	    	}
			if (individualSampleGenotypeList.size() > 1)
			{	// we're in the case of a multi-run project
				BasicDBObject union = new BasicDBObject("input", new BasicDBObject("$setUnion", individualSampleGenotypeList));
				union.put("as", "gt");
				union.put("cond", new BasicDBObject("$ne", Arrays.asList("$$gt", "")));
				BasicDBObject filteredGenotypeUnion = new BasicDBObject("$filter", union);	// union of (non-missing) genotypes for a given multi-sample individual
				

				if (conditionsWhereGqOrDpIsTooLow.size() == 0)
					vars.put("u" + j, filteredGenotypeUnion);
				else
					vars.put("u" + j, new BasicDBObject("$cond", new Object[] { new BasicDBObject("$and", conditionsWhereGqOrDpIsTooLow), new Object[0], filteredGenotypeUnion}));
			}
		}
		
		if (fMafApplied)
			in.put("a", new BasicDBObject("$add", altAlleleCountList));	// number of alternate alleles in selected population

		if (fMissingDataApplied || fMafApplied || fCompareBetweenGenotypes)
        	in.put("m", new BasicDBObject("$add", missingGenotypeCountList));	//  number of missing genotypes in selected population
		
		if (fCompareBetweenGenotypes)
		{
			if (fMultiRunProject)
				in.put("dc", new BasicDBObject("$size", new BasicDBObject("$setUnion", distinctGenotypeList)));	//  number of distinct non-missing genotypes in selected population (all same, not all same, all different, not all different)
			else
			{
				BasicDBObject filter = new BasicDBObject("input", new BasicDBObject("$setUnion", distinctGenotypeList));
				filter.put("as", "gt");
				filter.put("cond", new BasicDBObject("$ne", Arrays.asList(new BasicDBObject("$ifNull", new Object[] {"$$gt", ""}), "")));
				in.put("dc", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
			}
		}
		else if (fZygosityRegex || fIsWithoutAbnormalHeterozygosityQuery)
		{	//  distinct non-missing genotypes in selected population (zygosity comparison)
			if (fMultiRunProject)
				in.put("d", new BasicDBObject("$setUnion", distinctGenotypeList));
			else
			{
				BasicDBObject filter = new BasicDBObject("input", new BasicDBObject("$setUnion", distinctGenotypeList));
				filter.put("as", "gt");
				filter.put("cond", new BasicDBObject("$ne", Arrays.asList(new BasicDBObject("$ifNull", new Object[] {"$$gt", ""}), "")));
				in.put("d", new BasicDBObject("$filter", filter));
			}
		}
						
		BasicDBList mainMatchList = new BasicDBList();
		
		if (fMissingDataApplied)
			mainMatchList.add(new BasicDBObject("g.m", new BasicDBObject("$lte", selectedIndividuals.size() * missingData / 100)));
			
		if (fMafApplied || fCompareBetweenGenotypes || fIsWithoutAbnormalHeterozygosityQuery)
        {	// we need to calculate extra fields via an additional $let operator
            BasicDBObject subIn = new BasicDBObject();
            // keep previously computed fields
            if (fMafApplied || fCompareBetweenGenotypes)
            	subIn.put("m", "$$m");
            if (fMafApplied)
            	subIn.put("a", "$$a");
            if (fZygosityRegex)
            	subIn.put("d", "$$d");
            if (fCompareBetweenGenotypes)
            	subIn.put("dc", "$$dc");
            
            if (fCompareBetweenGenotypes)
            {	// dm = d + m
            	 subIn.put("dm", new BasicDBObject("$add", new Object[] {"$$dc", "$$m"}));
            	 
            	 mainMatchList.add(new BasicDBObject("g.m", new BasicDBObject("$lt", selectedIndividuals.size() - 1)));	// if only one individual's genotype is not treated as missing then the filter makes no more sense
            	 if ("$eq".equals(cleanOperator))
           			 mainMatchList.add(new BasicDBObject("g.dc", new BasicDBObject(fNegateMatch ? "$ne" /*not all same*/ : "$eq" /*all same*/, 1)));
            	 else if ("$ne".equals(cleanOperator))
            		 mainMatchList.add(new BasicDBObject("g.dm", new BasicDBObject(fNegateMatch ? "$lt" /*not all different*/ : "$eq" /*all different*/, selectedIndividuals.size())));
            	 else
            		 LOG.error("Invalid operator: " + operator);
            }
            
            if (fMafApplied)
            {	// allele frequency
        		BasicDBObject secondLet = new BasicDBObject("vars", new BasicDBObject("t", new BasicDBObject("$subtract", new Object[] {selectedIndividuals.size(), "$$m"})));
            	BasicDBList condList = new BasicDBList(), divideList = new BasicDBList();
                condList.add(new BasicDBObject("$eq", new Object[] {"$$t", 0}));
                condList.add(null);
                condList.add("$$t");
                divideList.add(new BasicDBObject("$multiply", new Object[] {"$$a", 50}));
                divideList.add(new BasicDBObject("$cond", condList));
        		secondLet.put("in", new BasicDBObject("$divide", divideList));
        		subIn.put("f", new BasicDBObject("$let", secondLet));
        		
				BasicDBList orMafMatch = new BasicDBList();
				BasicDBList andMafMatch = new BasicDBList();
				andMafMatch.add(new BasicDBObject("g.f", new BasicDBObject("$gte", minmaf)));
				andMafMatch.add(new BasicDBObject("g.f", new BasicDBObject("$lte", maxmaf)));
				orMafMatch.add(new BasicDBObject("$and", andMafMatch));
				andMafMatch = new BasicDBList();
				andMafMatch.add(new BasicDBObject("g.f", new BasicDBObject("$lte", Float.valueOf(100F - minmaf.floatValue()))));
				andMafMatch.add(new BasicDBObject("g.f", new BasicDBObject("$gte", Float.valueOf(100F - maxmaf.floatValue()))));
				orMafMatch.add(new BasicDBObject("$and", andMafMatch));
				mainMatchList.add(new BasicDBObject("$or", orMafMatch));
            }
            
            if (fIsWithoutAbnormalHeterozygosityQuery)
            {	// counts for HZ, HR and HV genotypes
				BasicDBObject filter = new BasicDBObject("input", "$$d");
				filter.put("as", "gt");
				filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "0/1")));
            	subIn.put("hz", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
            	
            	filter = new BasicDBObject("input", "$$d");
				filter.put("as", "gt");
				filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "0/0")));
            	subIn.put("hr", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
				
            	filter = new BasicDBObject("input", "$$d");
				filter.put("as", "gt");
				filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "1/1")));
            	subIn.put("hv", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
            }

            // insert additional $let
            BasicDBObject subVars = in;
    		BasicDBObject subLet = new BasicDBObject("vars", subVars);
    		subLet.put("in", subIn);
            in = new BasicDBObject("$let", subLet);
        }
		
		BasicDBObject let = new BasicDBObject("vars", vars);
		let.put("in", in);
		project.put("g", new BasicDBObject("$let", let));

		if (fMultiRunProject)
			pipeline.add(new BasicDBObject("$group", groupFields));
		if (!project.keySet().isEmpty())
			pipeline.add(new BasicDBObject("$project", project));

		if (cleanOperator != null)
        {
            if (selectedIndividuals.size() >= 1)
            {
				if (fZygosityRegex)	
				{	// query to match specific genotype code with zygosity regex (homozygous var, homozygous ref, heterozygous)
					BasicDBList orSelectedGenotypeRegexAndFieldExistList = new BasicDBList();
					DBObject orFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
					DBObject andFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
				
					for (int j=0; j<selectedIndividuals.size(); j++)
					{
						/* FIXME: we can probably support heterozygous multiple-digit-genotypes using {$not : /^([0-9]+)(\/\1)*$/} */
						if (fOr)
						{	// at least one whatever
							orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("g.d." + j, new BasicDBObject("$regex", cleanOperator)));
						}
						else if (!genotypeCodeToQueryMap.get(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS).startsWith(cleanOperator))
						{	// all homozygous whatever
							orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("g.d." + j, new BasicDBObject(j == 0 ? "$regex" : "$exists", j == 0 ? cleanOperator : false)));
						}
						else 
						{	// all heterozygous
							if (j == 0)
								orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("g.d." + j, new BasicDBObject("$regex", cleanOperator)));
							else
							{
								BasicDBList orList = new BasicDBList();
								DBObject clause1 = new BasicDBObject("g.d." + j, new BasicDBObject("$exists", false));
					    		DBObject clause2 = new BasicDBObject("g.d." + j, new BasicDBObject("$regex", cleanOperator));
								orList.add(clause1);
								orList.add(clause2);
								orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("$or", orList));
							}
						}
					}

					if (fOr)
					{		
						orFinalSelectedGenotypeRegexAndFieldExist.put("$or", orSelectedGenotypeRegexAndFieldExistList);
						mainMatchList.add(orFinalSelectedGenotypeRegexAndFieldExist);
				    }			
					else
					{
						andFinalSelectedGenotypeRegexAndFieldExist.put("$and", orSelectedGenotypeRegexAndFieldExistList);
						mainMatchList.add(andFinalSelectedGenotypeRegexAndFieldExist);
					}
				}
				else if (fIsWithoutAbnormalHeterozygosityQuery)
                {	// only for bi-allelic, diploid data: query that requires every allele present in heterozygous genotypes to be also present in homozygous ones
                    BasicDBList orList = new BasicDBList();
                    orList.add(new BasicDBObject("g.hz", new BasicDBObject("$eq", 0)));
                    BasicDBList andList = new BasicDBList();
                    andList.add(new BasicDBObject("g.hr", new BasicDBObject("$gt", 0)));
                    andList.add(new BasicDBObject("g.hv", new BasicDBObject("$gt", 0)));
                    orList.add(new BasicDBObject("$and", andList));
                    mainMatchList.add(new BasicDBObject("$or", orList));
                }
            }
        }
		
		if (mainMatchList.size() > 0)
			 pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", mainMatchList)));
		
        if (fieldsToReturn.size() == 0)	// reduce output size to the minimum if we are not loading fields for display
        	pipeline.add(new BasicDBObject("$project", new BasicDBObject("_id", "$_id" + (!fMultiRunProject ? "." + VariantRunDataId.FIELDNAME_VARIANT_ID : ""))));

//        System.out.println(pipeline.subList(1, pipeline.size()));
        return pipeline;
    }
		
//	private List<Comparable> buildFullListFromRange(MongoTemplate mongoTemplate, Comparable leftBound, Comparable rightBound) {
//		if (leftBound == null)
//			leftBound = mongoTemplate.findOne(new Query().with(new Sort(Sort.Direction.ASC, "_id")), VariantData.class).getId();
//		if (rightBound == null)
//			rightBound = mongoTemplate.findOne(new Query().with(new Sort(Sort.Direction.DESC, "_id")), VariantData.class).getId();
//
//		ArrayList<Comparable> result = new ArrayList<>();
//		String leftAsString = leftBound.toString(), rightAsString = rightBound.toString();
//		if (ObjectId.isValid(leftAsString) && ObjectId.isValid(rightAsString) && leftAsString.substring(0, 18).equals(rightAsString.substring(0, 18)))
//		{
//			int nCurrentId = Integer.parseInt(leftAsString.substring(18, 24), 16);
//			while (nCurrentId <= Integer.parseInt(rightAsString.substring(18, 24), 16))
//				result.add(new ObjectId(leftAsString.substring(0, 18) + Integer.toHexString(nCurrentId++)));
//		}
//		else
//		{
//			Query q = new Query().with(new Sort(Sort.Direction.ASC, "_id"));
//			q.fields().include("_id");
//			// not finished implementing method (functionality non needed)
//		}
//		return result;
//	}

	public static boolean areObjectIDsConsecutive(ObjectId first, ObjectId second)
	{
//		if (first == null || second == null)
//			return false;

		String firstAsString = first.toHexString(), secondAsString = second.toHexString();
		if (!firstAsString.substring(0, 18).equals(secondAsString.substring(0, 18)))
			return false;
		
		return 1 + Integer.parseInt(firstAsString.substring(18, 24), 16) == Integer.parseInt(secondAsString.substring(18, 24), 16);
	}

	public static BasicDBObject tryAndShrinkIdList(String pathToVariantId, Collection<Comparable> idCollection, int nShrinkThreshold)
	{
		if (idCollection.size() >= 300000)
			try
			{
		//		long b4 = System.currentTimeMillis();
		//		SortedSet<Comparable> idSet = SortedSet.class.isAssignableFrom(idCollection.getClass()) ? (SortedSet<Comparable>) idCollection : new TreeSet<Comparable>(idCollection);
		//		System.out.println("sorting took " + (System.currentTimeMillis() - b4));
				BasicDBList orList = new BasicDBList();
				ArrayList<ObjectId> inIdList = new ArrayList<>(), rangeIdList = new ArrayList<>();
				
				ObjectId previousId = null;
				for (Comparable id : idCollection)
				{
					ObjectId currentId = (ObjectId) id;
					if (previousId == null || areObjectIDsConsecutive(previousId, currentId))
						rangeIdList.add(currentId);
					else
					{
						if (rangeIdList.size() >= nShrinkThreshold)
						{	// replace list with a range
							BasicDBList chunkMatchAndList = new BasicDBList();
							chunkMatchAndList.add(new BasicDBObject(pathToVariantId, new BasicDBObject("$gte", rangeIdList.get(0))));
							chunkMatchAndList.add(new BasicDBObject(pathToVariantId, new BasicDBObject("$lte", rangeIdList.get(rangeIdList.size() - 1))));
							orList.add(new BasicDBObject("$and", chunkMatchAndList));
						}
						else
							inIdList.addAll(rangeIdList);	// range is too small, keep the list
		
						rangeIdList.clear();
						rangeIdList.add(currentId);
					}
					previousId = currentId;
				}
				inIdList.addAll(rangeIdList);
		
				if (inIdList.size() > 0 || orList.size() == 0)
					orList.add(new BasicDBObject(pathToVariantId, new BasicDBObject("$in", inIdList)));
		
				return orList.size() > 1 ? new BasicDBObject("$or", orList) : (BasicDBObject) orList.iterator().next();
			}
			catch (ClassCastException cce)
			{
				if (!cce.getMessage().contains("ObjectId"))
					throw cce;	// otherwise it simply means IDs are of a different type, in which case we can't shrink the collection
			}
//		else
//		{
//			LOG.debug("Didn't shrink id collection (" + idCollection.size() + " records only)");
//		}
		
		return new BasicDBObject(pathToVariantId, new BasicDBObject("$in", idCollection));	// not shrinked
	}

	/**
	 * Cleanup.
	 */
	public void cleanup() {
		variantCursor.close();
		preFilteredIDs = new ArrayList();	// free allocated memory
	}
	
	public static HashMap<String, String> getGenotypeCodeToQueryMap() {
		return genotypeCodeToQueryMap;
	}
	
	public static HashMap<String, String> getGenotypeCodeToDescriptionMap() {
		return genotypeCodeToDescriptionMap;
	}

	public void setMaxAlleleCount(int maxAlleleCount) {
		this.maxAlleleCount = maxAlleleCount;
	}
}
