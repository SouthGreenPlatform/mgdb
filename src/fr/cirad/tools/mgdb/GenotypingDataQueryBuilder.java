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
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.models.GigwaSearchVariantsRequest;
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
	
	/** Whether or not project has effect annotations. */
	private boolean projectHasEffectAnnotations;

	/** The variant cursor. */
	private DBCursor variantCursor; 
	
	/** The gene names. */
	private String geneNames;
	
	/** The variant effects. */
	private String variantEffects;
	
	/** The selected individuals. */
	private List<String>[] selectedIndividuals = new ArrayList[2];
	
	/** The operator. */
	private String[] operator = new String[2];
	
	/** The percentage of individuals for the "all same" filter. */
	private Integer[] mostSameRatio = new Integer[2];
	
	/** The annotation field thresholds. */
	private HashMap<String, Integer>[] annotationFieldThresholds = new HashMap[2];
		
	/** The missing data threshold. */
	private Double[] missingData = new Double[2];
	
	/** The minmaf. */
	private Float[] minmaf = new Float[2];
	
	/** The maxmaf. */
	private Float[] maxmaf = new Float[2];
	
    boolean fDiscriminate = false;
	
	/** The individual index to sample list map. */
	private HashMap<Integer, List<Integer>>[] individualIndexToSampleListMap = new HashMap[2];
	
	/** The n total variant count. */
	private long nTotalVariantCount = 0;
	
	/** The n variants queried at once. */
	private int nNVariantsQueriedAtOnce = 0;	/* will remain so if we are working on the full dataset */
	
	private int nNextCallCount = 0;
	
	private int maxAlleleCount = 0;
	
	private int nGroupCount = 0;
	
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
	static final public String GENOTYPE_CODE_LABEL_NOT_ALL_SAME = "Not all the same";

	/** The Constant GENOTYPE_CODE_LABEL_MOSTLY_SAME. */
	static final public String GENOTYPE_CODE_LABEL_MOSTLY_SAME = "All or mostly the same";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_DIFFERENT. */
	static final public String GENOTYPE_CODE_LABEL_ALL_DIFFERENT = "All different";

	/** The Constant GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT. */
	static final public String GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT = "Not all different";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF = "All Homozygous Ref";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF = "Some Homozygous Ref";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR = "All Homozygous Var";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR = "Some Homozygous Var";

	/** The Constant GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS. */
	static final public String GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS = "All Heterozygous";

	/** The Constant GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS. */
	static final public String GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS = "Some Heterozygous";

	/** The Constant GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY. */
	static final public String GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY = "Without abnormal heterozygosity";

	/** The Constant genotypePatternToDescriptionMap. */
	static final private HashMap<String, String> genotypePatternToDescriptionMap = new LinkedHashMap<String, String>();

	/** The Constant genotypePatternToQueryMap. */
	static final private HashMap<String, String> genotypePatternToQueryMap = new HashMap<String, String>();	

	static
	{
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL, "This will return all variants whithout applying any filters");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_SAME, "This will return variants where not all selected individuals have the same genotype");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_MOSTLY_SAME, "This will return variants where all or most selected individuals have the same genotype");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_DIFFERENT, "This will return variants where none of the selected individuals have the same genotype");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT, "This will return variants where some of the selected individuals have the same genotypes");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF, "This will return variants where selected individuals are all homozygous with the reference allele");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF, "This will return variants where where at least one selected individual is homozygous with the reference allele");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR, "This will return variants where selected individuals are all homozygous with an alternate allele");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR, "This will return variants where at least one selected individual is homozygous with an alternate allele");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS, "This will return variants where selected individuals are all heterozygous");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS, "This will return variants where at least one selected individual is heterozygous");
		genotypePatternToDescriptionMap.put(GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY, "This will exclude variants where some alleles are found only in heterozygous genotypes (only for diploid, bi-allelic data)");
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ALL, null);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_MOSTLY_SAME, "$eq");
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_SAME, "$eq" + GenotypingDataQueryBuilder.AGGREGATION_QUERY_NEGATION_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_DIFFERENT, "$ne");
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_NOT_ALL_DIFFERENT, "$ne" + GenotypingDataQueryBuilder.AGGREGATION_QUERY_NEGATION_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_REF, "^0(/0)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_REF, "^0(/0)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HOMOZYGOUS_VAR, "^([1-9][0-9]*)(/\\1)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HOMOZYGOUS_VAR, "^([1-9][0-9]*)(/\\1)*$"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ALL_HETEROZYGOUS, "([0-9])([0-9])*(/(?!\\1))+([0-9])*"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS, "([0-9])([0-9])*(/(?!\\1))+([0-9])*"/*|^$"*/ + GenotypingDataQueryBuilder.AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX);
		genotypePatternToQueryMap.put(GENOTYPE_CODE_LABEL_WITHOUT_ABNORMAL_HETEROZYGOSITY, GenotypingDataQueryBuilder.AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY);
	}
	
	public GenotypingDataQueryBuilder(GigwaSearchVariantsRequest gsvr, DBCollection tempExportColl) throws Exception
	{
        String info[] = GigwaSearchVariantsRequest.getInfoFromId(gsvr.getVariantSetId(), 2);
        String sModule = info[0];
        int projId = Integer.parseInt(info[1]);

		this.mongoTemplate = MongoTemplateManager.get(sModule);
		this.genotypingProject = mongoTemplate.findById(Integer.valueOf(projId), GenotypingProject.class);
		this.geneNames = gsvr.getGeneName();
		this.variantEffects = gsvr.getVariantEffect();

		Query q = new Query();
       	q.addCriteria(Criteria.where("_id").is(projId));
       	q.addCriteria(Criteria.where(GenotypingProject.FIELDNAME_EFFECT_ANNOTATIONS + ".0").exists(true));
       	this.projectHasEffectAnnotations = mongoTemplate.findOne(q, GenotypingProject.class) != null;

		this.selectedIndividuals[0] = gsvr.getCallSetIds().size() == 0 ? MgdbDao.getIndividualsInDbOrder(sModule, projId) : gsvr.getCallSetIds();
		this.operator[0] = genotypePatternToQueryMap.get(gsvr.getGtPattern());
		this.mostSameRatio[0] = gsvr.getMostSameRatio();
		this.annotationFieldThresholds[0] = gsvr.getAnnotationFieldThresholds();
		this.missingData[0] = gsvr.getMissingData();
		this.minmaf[0] = gsvr.getMinmaf();
		this.maxmaf[0] = gsvr.getMaxmaf();
		this.individualIndexToSampleListMap[0] = new HashMap<Integer, List<Integer>>();	
		int nSampleCount = 0;		
		for (int k=0; k<selectedIndividuals[0].size(); k++)
		{
			List<Integer> sampleIndexes = genotypingProject.getIndividualSampleIndexes(selectedIndividuals[0].get(k));
			individualIndexToSampleListMap[0].put(k, sampleIndexes);
			nSampleCount += sampleIndexes.size();
		}
		
		nGroupCount = countGroupsForWhichToFilterOnGenotypingData(gsvr);
		LOG.debug("Filtering genotypes on " + nGroupCount + " groups");
		if (nGroupCount == 2)
		{
			this.selectedIndividuals[1] = gsvr.getCallSetIds2().size() == 0 ? MgdbDao.getIndividualsInDbOrder(sModule, projId) : gsvr.getCallSetIds2();
			this.operator[1] = genotypePatternToQueryMap.get(gsvr.getGtPattern2());
			this.mostSameRatio[1] = gsvr.getMostSameRatio2();
			this.annotationFieldThresholds[1] = gsvr.getAnnotationFieldThresholds2();
			this.missingData[1] = gsvr.getMissingData2();
			this.minmaf[1] = gsvr.getMinmaf2();
			this.maxmaf[1] = gsvr.getMaxmaf2();
			this.individualIndexToSampleListMap[1] = new HashMap<Integer, List<Integer>>();
			for (int k=0; k<selectedIndividuals[1].size(); k++)
			{
				List<Integer> sampleIndexes = genotypingProject.getIndividualSampleIndexes(selectedIndividuals[1].get(k));
				individualIndexToSampleListMap[1].put(k, sampleIndexes);
				nSampleCount += sampleIndexes.size();
			}
			fDiscriminate = gsvr.isDiscriminate();
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
				        
		List<DBObject> pipeline = new ArrayList<DBObject>();
		BasicDBList initialMatchList = new BasicDBList();
		BasicDBList mainMatchList = new BasicDBList();

		pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", initialMatchList)));

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
		if (projectHasEffectAnnotations && (geneNames.length() > 0 || variantEffects.length() > 0))
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
		
        boolean fGotIndividualsWithMultipleSamples = false;
		boolean[] fZygosityRegex = new boolean[2];
		boolean[] fIsWithoutAbnormalHeterozygosityQuery = new boolean[2];
		boolean[] fNegateMatch = new boolean[2];
		boolean[] fOr = new boolean[2];
		boolean[] fMafApplied = new boolean[2];
		boolean[] fMissingDataApplied = new boolean[2];
        boolean[] fCompareBetweenGenotypes = new boolean[2];
        String[] cleanOperator = new String[2];

        for (int g=0; g<nGroupCount; g++)
        {
    		cleanOperator[g] = operator[g];
    		if (cleanOperator[g] != null)
    		{
    			if (cleanOperator[g].endsWith(AGGREGATION_QUERY_NEGATION_SUFFIX))
    			{
    				fNegateMatch[g] = true;
    				cleanOperator[g] = cleanOperator[g].substring(0, cleanOperator[g].length() - AGGREGATION_QUERY_NEGATION_SUFFIX.length());
    			}
    			else if (cleanOperator[g].endsWith(AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX)) {
    				fZygosityRegex[g] = true;
    				cleanOperator[g] = cleanOperator[g].substring(0, cleanOperator[g].length() - AGGREGATION_QUERY_REGEX_APPLY_TO_ALL_IND_SUFFIX.length());
    			}
    			else if (cleanOperator[g].endsWith(AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX)) {
    				fZygosityRegex[g] = true;
    				fOr[g] = true;
    				cleanOperator[g] = cleanOperator[g].substring(0, cleanOperator[g].length() - AGGREGATION_QUERY_REGEX_APPLY_TO_AT_LEAST_ONE_IND_SUFFIX.length());
    			}
    			else if (cleanOperator[g].equals(AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY)) {
    				fIsWithoutAbnormalHeterozygosityQuery[g] = true;
    				cleanOperator[g] = cleanOperator[g].substring(0, cleanOperator[g].length() - AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY.length());
    			}
    		}

	        int nMaxNumberOfAllelesForOneVariant = maxAlleleCount > 0 ? maxAlleleCount : genotypingProject.getAlleleCounts().last(), nPloidy = genotypingProject.getPloidyLevel();
	        int nNumberOfPossibleGenotypes = (int) (nMaxNumberOfAllelesForOneVariant + MathUtils.factorial(nMaxNumberOfAllelesForOneVariant)/(MathUtils.factorial(nPloidy)*MathUtils.factorial(nMaxNumberOfAllelesForOneVariant-nPloidy)));
	        double maxMissingGenotypeCount = selectedIndividuals[g].size() * missingData[g] / 100;
    		if ("$ne".equals(cleanOperator[g]) && !fNegateMatch[g])
            {
		        if (selectedIndividuals[g].size() - maxMissingGenotypeCount > nNumberOfPossibleGenotypes)
		        {
		        	initialMatchList.add(new BasicDBObject("_id", null));	// return no results
		        	if (nNextCallCount == 1)
			        	LOG.info("Aborting 'all different' filter (more called individuals than possible genotypes in group " + (g + 1) + ")");
		        	return pipeline;
		        }
            }

    		if (!fGotIndividualsWithMultipleSamples)
				for (List<Integer> sampleList : individualIndexToSampleListMap[g].values())
					if (sampleList.size() > 1)
					{
						fGotIndividualsWithMultipleSamples = true;
						break;
					}
						
			fCompareBetweenGenotypes[g] = cleanOperator[g] != null && !fZygosityRegex[g] && !fIsWithoutAbnormalHeterozygosityQuery[g];
	        if ("$ne".equals(cleanOperator[g]) && fNegateMatch[g])
	        {
		        if (selectedIndividuals[g].size() - maxMissingGenotypeCount > nNumberOfPossibleGenotypes)
		        {
		        	fCompareBetweenGenotypes[g] = false;	// we know applying this filter would not affect the query
		        	if (nNextCallCount == 1)
			        	LOG.info("Ignoring 'not all different' filter on group 1 (more called individuals than possible genotypes in group " + (g + 1) + ")");
		        }
	        }
	        
	        fMafApplied[g] = maxmaf[g] != null && maxmaf[g].floatValue() < 50F || minmaf[g] != null && minmaf[g].floatValue() > 0.0F;
	        fMissingDataApplied[g] = missingData[g] != null && missingData[g] < 100;
        }
        
		DBObject groupFields = new BasicDBObject("_id", "$_id." + VariantRunDataId.FIELDNAME_VARIANT_ID); // group multi-run records by variant id
        DBObject project = new BasicDBObject();
        BasicDBObject addFieldsVars = new BasicDBObject();	// used for handling "all same" filter
        BasicDBObject addFieldsIn = new BasicDBObject();	// used for handling "all same" filter
    	BasicDBObject vars = new BasicDBObject();
    	BasicDBObject in = new BasicDBObject();
        BasicDBObject subIn = new BasicDBObject();
        
        for (int g=0; g<nGroupCount; g++)
		{
    		boolean fMostSameSelected = "$eq".equals(cleanOperator[g]) && !fNegateMatch[g];
	        int nMaxNumberOfAllelesForOneVariant = maxAlleleCount > 0 ? maxAlleleCount : genotypingProject.getAlleleCounts().last(), nPloidy = genotypingProject.getPloidyLevel();
	        int nNumberOfPossibleGenotypes = (int) (nMaxNumberOfAllelesForOneVariant + MathUtils.factorial(nMaxNumberOfAllelesForOneVariant)/(MathUtils.factorial(nPloidy)*MathUtils.factorial(nMaxNumberOfAllelesForOneVariant-nPloidy)) + (missingData[g] != null && missingData[g] >= 100/selectedIndividuals[g].size() ? 1 : 0));

        	BasicDBList altAlleleCountList = new BasicDBList();
            BasicDBList missingGenotypeCountList = new BasicDBList();
            BasicDBList distinctGenotypeList = new BasicDBList();
            
			for (int j=0; j<selectedIndividuals[g].size(); j++)
			{
				BasicDBList individualSampleGenotypeList = new BasicDBList();
				BasicDBList conditionsWhereAnnotationFieldValueIsTooLow = new BasicDBList();
				List<Integer> individualSamples = individualIndexToSampleListMap[g].get(j);
				for (int k=0; k<individualSamples.size(); k++)	// this loop is executed only once for single-run projects
		    	{
					Integer individualSample = individualSamples.get(k);
					String pathToGT = individualSample + "." + SampleGenotype.FIELDNAME_GENOTYPECODE;
					groupFields.put(pathToGT.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT));
					individualSampleGenotypeList.add("$" + pathToGT.replaceAll("\\.", "¤"));
	        		
					if (annotationFieldThresholds[g] != null)
						for (String annotation : annotationFieldThresholds[g].keySet())
						{
							Integer threshold = annotationFieldThresholds[g].get(annotation);
							if (threshold == 0)
								continue;

							String pathToAnnotationField = individualSample + "." + SampleGenotype.SECTION_ADDITIONAL_INFO + "." + annotation;
							groupFields.put(pathToAnnotationField.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToAnnotationField));
							
							BasicDBList qualTooLowList = new BasicDBList();
							qualTooLowList.add(fGotIndividualsWithMultipleSamples ? new BasicDBObject("$arrayElemAt", new Object[] {"$" + pathToAnnotationField.replaceAll("\\.", "¤"), 0}) : ("$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToAnnotationField));
							qualTooLowList.add(threshold);
		
							BasicDBObject qualTooLow = new BasicDBObject("$lt", qualTooLowList);
							conditionsWhereAnnotationFieldValueIsTooLow.add(qualTooLow);
						}
	
			        if (k > 0)
						continue;	// the remaining code in this loop must only be executed once
					
			        Object possiblyConstrainedPathToGT = conditionsWhereAnnotationFieldValueIsTooLow.size() == 0 ? "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT : new BasicDBObject("$cond", new Object[] {new BasicDBObject("$or", conditionsWhereAnnotationFieldValueIsTooLow), "", "$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + pathToGT});
	                if (fMafApplied[g])
	                {	// count alternate alleles
	                	BasicDBList condList = new BasicDBList();
	                    String allRefGtCode = genotypingProject.getPloidyLevel() != 1 ? "0/0" : "0";
	                    Object[] arrayOfPossibleValuesWithoutAlternateAllele = new Object[] {fGotIndividualsWithMultipleSamples ? new Object[] {allRefGtCode} : allRefGtCode, fGotIndividualsWithMultipleSamples ? new Object[] {""} : "", fGotIndividualsWithMultipleSamples ? new Object[] {null} : null};
	                    condList.add(new BasicDBObject("$in", new Object[] {fGotIndividualsWithMultipleSamples ? ("$$u" + g + "_" + j) : (possiblyConstrainedPathToGT), arrayOfPossibleValuesWithoutAlternateAllele}));
	                    condList.add(0);
	                    if (genotypingProject.getPloidyLevel() == 1)
	                        condList.add(2);
	                    else
	                    	condList.add(new BasicDBObject("$subtract", new Object[] {2, new BasicDBObject("$cmp",  new Object[] {fGotIndividualsWithMultipleSamples ? new Object[] {"1/1"} : "1/1", fGotIndividualsWithMultipleSamples ? ("$$u" + g + "_" + j) : (possiblyConstrainedPathToGT)})}));
	                    altAlleleCountList.add(new BasicDBObject("$cond", condList));
	                }
	
	                if ((fMissingDataApplied[g] || fMafApplied[g] || (fCompareBetweenGenotypes[g]) && !fMostSameSelected))
	                {	// count missing genotypes
	                	if (fGotIndividualsWithMultipleSamples)
	                		missingGenotypeCountList.add(new BasicDBObject("$abs", new BasicDBObject("$cmp", new Object[] {new BasicDBObject("$size", "$$u" + g + "_" + j), 1})));
	                	else
	                	{
	                		BasicDBObject missingGtCalculation = new BasicDBObject("$add", new Object[] {1, new BasicDBObject("$cmp", new Object[] {"", new BasicDBObject("$ifNull", new Object[] {possiblyConstrainedPathToGT, ""})})});
	       					missingGenotypeCountList.add(missingGtCalculation);
	                	}
	                }
	                
	                if (fCompareBetweenGenotypes[g] || fZygosityRegex[g] || fIsWithoutAbnormalHeterozygosityQuery[g] || fMostSameSelected)
	                {	// build list of distinct non-missing genotypes
	                	if (fGotIndividualsWithMultipleSamples)
	                	{
		                	BasicDBList condList = new BasicDBList();
		                    condList.add(new BasicDBObject("$eq", new Object[] {1, new BasicDBObject("$size", "$$u" + g + "_" + j)}));
		                    condList.add("$$u" + g + "_" + j);
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
					
	
					if (conditionsWhereAnnotationFieldValueIsTooLow.size() == 0)
						vars.put("u" + g + "_" + j, filteredGenotypeUnion);
					else
						vars.put("u" + g + "_" + j, new BasicDBObject("$cond", new Object[] { new BasicDBObject("$and", conditionsWhereAnnotationFieldValueIsTooLow), new Object[0], filteredGenotypeUnion}));
				}
			}

			if (altAlleleCountList.size() > 0)
				in.put("a" + g, new BasicDBObject("$add", altAlleleCountList));	// number of alternate alleles in selected population

			if (missingGenotypeCountList.size() > 0)
	        	in.put("m" + g, new BasicDBObject("$add", missingGenotypeCountList));	//  number of missing genotypes in selected population
			
			if (fCompareBetweenGenotypes[g] && !fMostSameSelected)
			{
				if (fGotIndividualsWithMultipleSamples)
					in.put("dc" + g, new BasicDBObject("$size", new BasicDBObject("$setUnion", distinctGenotypeList)));	//  number of distinct non-missing genotypes in selected population (not all same, all different, not all different)
				else
				{
					BasicDBObject filter = new BasicDBObject("input", new BasicDBObject("$setUnion", distinctGenotypeList));
					filter.put("as", "gt");
					filter.put("cond", new BasicDBObject("$ne", Arrays.asList(new BasicDBObject("$ifNull", new Object[] {"$$gt", ""}), "")));
					in.put("dc" + g, new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
				}
			}
			else if (fZygosityRegex[g] || fIsWithoutAbnormalHeterozygosityQuery[g] || fMostSameSelected)
			{	//  distinct non-missing genotypes in selected population (zygosity comparison)
				if (fGotIndividualsWithMultipleSamples)
				{
					in.put("d" + g, new BasicDBObject("$setUnion", distinctGenotypeList));
					if (fMostSameSelected)
						in.put("gt" + g, new BasicDBObject("$concatArrays", distinctGenotypeList));	//  complete list of genotypes in selected population (all same)
				}
				else
				{
					BasicDBObject filter = new BasicDBObject("input", new BasicDBObject("$setUnion", distinctGenotypeList));
					filter.put("as", "gt");
					filter.put("cond", new BasicDBObject("$ne", Arrays.asList(new BasicDBObject("$ifNull", new Object[] {"$$gt", ""}), "")));
					in.put("d" + g, new BasicDBObject("$filter", filter));
					if (fMostSameSelected)
						in.put("gt" + g, new BasicDBObject("$concatArrays", distinctGenotypeList));	//  complete list of genotypes in selected population (all same)
				}
			}
			
			if (fMissingDataApplied[g])
				mainMatchList.add(new BasicDBObject("r.m" + g, new BasicDBObject("$lte", selectedIndividuals[g].size() * missingData[g] / 100)));
				
			if (fMissingDataApplied[g] || fMafApplied[g] || fCompareBetweenGenotypes[g] || fIsWithoutAbnormalHeterozygosityQuery[g] || fMostSameSelected)
	        {	// we need to calculate extra fields via an additional $let operator
	            // keep previously computed fields
	            if (fMissingDataApplied[g] || fMafApplied[g] || (fCompareBetweenGenotypes[g] && !fMostSameSelected))
	            	subIn.put("m" + g, "$$m" + g);
	            if (fMafApplied[g])
	            	subIn.put("a" + g, "$$a" + g);
	            if (fZygosityRegex[g] || fMostSameSelected)
	            	subIn.put("d" + g, "$$d" + g);
	            if (fCompareBetweenGenotypes[g] && !fMostSameSelected)
	            	subIn.put("dc" + g, "$$dc" + g);

	            if (fCompareBetweenGenotypes[g] && !fMostSameSelected)
	            {	// dm = d + m
	            	 subIn.put("dm" + g, new BasicDBObject("$add", new Object[] {"$$dc" + g, "$$m" + g}));
	            	 
            		 mainMatchList.add(new BasicDBObject("r.m" + g, new BasicDBObject("$lt", selectedIndividuals[g].size() - 1)));	// if only one individual's genotype is not treated as missing then the filter makes no more sense
	            	 if ("$eq".equals(cleanOperator[g]) && fNegateMatch[g])
	           			 mainMatchList.add(new BasicDBObject("r.dc" + g, new BasicDBObject("$ne" /*not all same*/, 1)));
	            	 else if ("$ne".equals(cleanOperator[g]))
	            		 mainMatchList.add(new BasicDBObject("r.dm" + g, new BasicDBObject(fNegateMatch[g] ? "$lt" /*not all different*/ : "$eq" /*all different*/, selectedIndividuals[g].size())));
	            	 else
	            		 LOG.error("Invalid operator: " + operator);
	            }
	            
	            if (fMafApplied[g])
	            {	// allele frequency
	        		BasicDBObject secondLet = new BasicDBObject("vars", new BasicDBObject("t" + g, new BasicDBObject("$subtract", new Object[] {selectedIndividuals[g].size(), "$$m" + g})));
	            	BasicDBList condList = new BasicDBList(), divideList = new BasicDBList();
	                condList.add(new BasicDBObject("$eq", new Object[] {"$$t" + g, 0}));
	                condList.add(null);
	                condList.add("$$t" + g);
	                divideList.add(new BasicDBObject("$multiply", new Object[] {"$$a" + g, 50}));
	                divideList.add(new BasicDBObject("$cond", condList));
	        		secondLet.put("in", new BasicDBObject("$divide", divideList));
	        		subIn.put("f" + g, new BasicDBObject("$let", secondLet));
	        		
					BasicDBList orMafMatch = new BasicDBList();
					BasicDBList andMafMatch = new BasicDBList();
					andMafMatch.add(new BasicDBObject("r.f" + g, new BasicDBObject("$gte", minmaf[g])));
					andMafMatch.add(new BasicDBObject("r.f" + g, new BasicDBObject("$lte", maxmaf[g])));
					orMafMatch.add(new BasicDBObject("$and", andMafMatch));
					andMafMatch = new BasicDBList();
					andMafMatch.add(new BasicDBObject("r.f" + g, new BasicDBObject("$lte", Float.valueOf(100F - minmaf[g].floatValue()))));
					andMafMatch.add(new BasicDBObject("r.f" + g, new BasicDBObject("$gte", Float.valueOf(100F - maxmaf[g].floatValue()))));
					orMafMatch.add(new BasicDBObject("$and", andMafMatch));
					mainMatchList.add(new BasicDBObject("$or", orMafMatch));
	            }
	            
	            if (fIsWithoutAbnormalHeterozygosityQuery[g])
	            {	// counts for HZ, HR and HV genotypes
					BasicDBObject filter = new BasicDBObject("input", "$$d" + g);
					filter.put("as", "gt");
					filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "0/1")));
	            	subIn.put("hz", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
	            	
	            	filter = new BasicDBObject("input", "$$d" + g);
					filter.put("as", "gt");
					filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "0/0")));
	            	subIn.put("hr", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
					
	            	filter = new BasicDBObject("input", "$$d" + g);
					filter.put("as", "gt");
					filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$gt", "1/1")));
	            	subIn.put("hv", new BasicDBObject("$size", new BasicDBObject("$filter", filter)));
	            }
	        }

			if (cleanOperator[g] != null)
	        {
	            if (selectedIndividuals[g].size() >= 1)
	            {
					if (fZygosityRegex[g])	
					{	// query to match specific genotype code with zygosity regex (homozygous var, homozygous ref, heterozygous)
						BasicDBList orSelectedGenotypeRegexAndFieldExistList = new BasicDBList();
						DBObject orFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
						DBObject andFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
					
						for (int j=0; j<selectedIndividuals[g].size(); j++)
						{
							/* FIXME: we can probably support heterozygous multiple-digit-genotypes using {$not : /^([0-9]+)(\/\1)*$/} */
							if (fOr[g])
							{	// at least one whatever
								orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("r.d" + g + "." + j, new BasicDBObject("$regex", cleanOperator[g])));
							}
							else if (!genotypePatternToQueryMap.get(GENOTYPE_CODE_LABEL_ATL_ONE_HETEROZYGOUS).startsWith(cleanOperator[g]))
							{	// all homozygous whatever
								orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("r.d" + g + "." + j, new BasicDBObject(j == 0 ? "$regex" : "$exists", j == 0 ? cleanOperator[g] : false)));
							}
							else 
							{	// all heterozygous
								if (j == 0)
									orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("r.d" + g + "." + j, new BasicDBObject("$regex", cleanOperator[g])));
								else
								{
									BasicDBList orList = new BasicDBList();
									DBObject clause1 = new BasicDBObject("r.d" + g + "." + j, new BasicDBObject("$exists", false));
						    		DBObject clause2 = new BasicDBObject("r.d" + g + "." + j, new BasicDBObject("$regex", cleanOperator[g]));
									orList.add(clause1);
									orList.add(clause2);
									orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("$or", orList));
								}
							}
						}

						if (fOr[g])
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
					else if (fIsWithoutAbnormalHeterozygosityQuery[g])
	                {	// only for bi-allelic, diploid data: query that requires every allele present in heterozygous genotypes to be also present in homozygous ones
	                    BasicDBList orList = new BasicDBList();
	                    orList.add(new BasicDBObject("r.hz", new BasicDBObject("$eq", 0)));
	                    BasicDBList andList = new BasicDBList();
	                    andList.add(new BasicDBObject("r.hv", new BasicDBObject("$gt", 0)));
	                    orList.add(new BasicDBObject("$and", andList));
	                    mainMatchList.add(new BasicDBObject("$or", orList));
	                }
					else if (fMostSameSelected)
		            {
		                BasicDBList concatArrayListForMostSame = new BasicDBList();
		                for (int nPossibleGtIndex = 0; nPossibleGtIndex < nNumberOfPossibleGenotypes; nPossibleGtIndex++)
		                {
		                	BasicDBObject filter = new BasicDBObject("input", "$$gt" + g);
		                	filter.put("as", "g");
		                	filter.put("cond", new BasicDBObject("$eq", Arrays.asList("$$g", new BasicDBObject("$arrayElemAt", Arrays.asList("$$d" + g, nPossibleGtIndex)))));
		                	BasicDBList condForPossibleGt = new BasicDBList();
		                	condForPossibleGt.add(new BasicDBObject("$lte", Arrays.asList(new BasicDBObject("$size", "$$d" + g), nPossibleGtIndex)));
		                	condForPossibleGt.add(new Object[0]);
		                	condForPossibleGt.add(Arrays.asList(new BasicDBObject("$size", new BasicDBObject("$filter", filter))));
		                	
		                	concatArrayListForMostSame.add(new BasicDBObject("$cond", condForPossibleGt));
		                }
		                subIn.put("c" + g, new BasicDBObject("$concatArrays", concatArrayListForMostSame));
		                
		                addFieldsVars.put("dgc" + g, new BasicDBObject("$max", "$r.c" + g));	// dominant genotype count
		                Object minimumDominantGenotypeCount = mostSameRatio[g] * selectedIndividuals[g].size() / 100f;
		                if (fMissingDataApplied[g])	// we may need to decrease that by the number of allowed missing genotypes
		                	minimumDominantGenotypeCount = new BasicDBObject("$subtract", Arrays.asList(minimumDominantGenotypeCount, "$r.m" + g));
		                addFieldsIn.put("ed" + g, new BasicDBObject("$gte", Arrays.asList("$$dgc" + g, minimumDominantGenotypeCount)));	// flag telling whether or not we have enough dominant genotypes to reach the required ratio
		                if (fDiscriminate && g == 1)
		                {
		                	BasicDBObject dominantGt0 = new BasicDBObject("$arrayElemAt", Arrays.asList("$r.d" + 0, new BasicDBObject("$indexOfArray", Arrays.asList("$r.c" + 0, "$$dgc" + 0))));
		                	BasicDBObject dominantGt1 = new BasicDBObject("$arrayElemAt", Arrays.asList("$r.d" + g, new BasicDBObject("$indexOfArray", Arrays.asList("$r.c" + g, "$$dgc" + g))));
		                	addFieldsIn.put("sd", new BasicDBObject("$eq", Arrays.asList(dominantGt0, dominantGt1)));	// tells whether dominant genotypes are the same in both groups
		                	
		                	mainMatchList.add(new BasicDBObject("r2.sd", false));
		                }

		                mainMatchList.add(new BasicDBObject("r2.ed" + g, true));
		            }
	            }
	        }
		}
        
        if (subIn.size() > 0)
        { // insert additional $let
	        BasicDBObject subVars = in;
			BasicDBObject subLet = new BasicDBObject("vars", subVars);
			subLet.put("in", subIn);
			in = new BasicDBObject("$let", subLet);
        }

		BasicDBObject let = new BasicDBObject("vars", vars);
		let.put("in", in);
		project.put("r", new BasicDBObject("$let", let));			
		
		if (fGotIndividualsWithMultipleSamples)
			pipeline.add(new BasicDBObject("$group", groupFields));
		if (!project.keySet().isEmpty())
			pipeline.add(new BasicDBObject("$project", project));
		
		if (addFieldsIn.size() > 0)
		{
			BasicDBObject addFieldsLet = new BasicDBObject("vars", addFieldsVars);
			addFieldsLet.put("in", addFieldsIn);
			pipeline.add(new BasicDBObject("$addFields", new BasicDBObject("r2", new BasicDBObject("$let", addFieldsLet))));
		}
				
		if (mainMatchList.size() > 0)
			 pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", mainMatchList)));

        pipeline.add(new BasicDBObject("$project", new BasicDBObject("_id", "$_id" + (!fGotIndividualsWithMultipleSamples ? "." + VariantRunDataId.FIELDNAME_VARIANT_ID : ""))));

//        if (nNextCallCount == 1)
//        {
//        	try { System.out.println(new ObjectMapper().defaultPrettyPrintingWriter().writeValueAsString(pipeline.subList(1, pipeline.size()))); }
//        	catch (Exception ignored) {}
//        }
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
	
	public static HashMap<String, String> getGenotypePatternToQueryMap() {
		return genotypePatternToQueryMap;
	}
	
	public static HashMap<String, String> getGenotypePatternToDescriptionMap() {
		return genotypePatternToDescriptionMap;
	}

	public void setMaxAlleleCount(int maxAlleleCount) {
		this.maxAlleleCount = maxAlleleCount;
	}
	
	static public int countGroupsForWhichToFilterOnGenotypingData(GigwaSearchVariantsRequest gsvr)
    {
        String info[] = GigwaSearchVariantsRequest.getInfoFromId(gsvr.getVariantSetId(), 2);
        String sModule = info[0];

    	int groupCount = 0;
    	if (!gsvr.getGtPattern().equals(GENOTYPE_CODE_LABEL_ALL) || gsvr.getAnnotationFieldThresholds().size() >= 1 || gsvr.getMissingData() < 100 || gsvr.getMinmaf() > 0 || gsvr.getMaxmaf() < 50)
    		groupCount++;
    	if (!gsvr.getGtPattern2().equals(GENOTYPE_CODE_LABEL_ALL) || gsvr.getAnnotationFieldThresholds2().size() >= 1 || gsvr.getMissingData2() < 100 || gsvr.getMinmaf2() > 0 || gsvr.getMaxmaf2() < 50)
    		groupCount++;
    	
    	if (groupCount == 0 && MongoTemplateManager.get(sModule).count(null, GenotypingProject.class) != 1 || gsvr.getGeneName().length() > 0 || gsvr.getVariantEffect().length() > 0)
    		groupCount = 1;	// needed at least for filtering on annotation data

    	return groupCount;
    }
}
