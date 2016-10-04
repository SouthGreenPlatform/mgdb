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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.mongo.MongoTemplateManager;

// TODO: Auto-generated Javadoc
/**
 * The Class GenotypingDataQueryBuilder.
 */
public class GenotypingDataQueryBuilder implements Iterator<List<DBObject>>
{
	
	/** The Constant LOG. */
	protected static final Logger LOG = Logger.getLogger(GenotypingDataQueryBuilder.class);
	
	/** The Constant MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE. */
	static final private int MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE = 1000000;
	
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

	/** The mongo template. */
	private MongoTemplate mongoTemplate;
	
	/** The genotyping project. */
	private GenotypingProject genotypingProject;
	
	/** The individual index to sample list map. */
	private HashMap<Integer, Collection<Integer>> individualIndexToSampleListMap;
	
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
	
	/** The n n variants queried at once. */
	private int nNVariantsQueriedAtOnce = 0;	/* will remain so if we are working on the full dataset */
	
	/** The current tagged variant. */
	private Comparable currentTaggedVariant = null;
	
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
		this.individualIndexToSampleListMap = new HashMap<Integer, Collection<Integer>>();
		
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
			this.variantCursor = taggedVarColl.find().addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		}
		else
		{
	    	// we may need to split the query into several parts if the number of input variants is large (otherwise the query may exceed 16Mb)
			this.nNVariantsQueriedAtOnce = MAX_NUMBER_OF_GENOTYPES_TO_QUERY_AT_ONCE / (nSampleCount * (NUMBER_OF_SIMULTANEOUS_QUERY_THREADS / 2));
			this.variantCursor = tempExportColl.find().addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		}
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
		boolean fZygosityRegex = false;
		boolean fIsRealisticGenotypeQuery = false;
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
				fIsRealisticGenotypeQuery = true;
				cleanOperator = cleanOperator.substring(0, cleanOperator.length() - AGGREGATION_QUERY_WITHOUT_ABNORMAL_HETEROZYGOSITY.length());
			}
		}
		        
		List<DBObject> pipeline = new ArrayList<DBObject>();
		BasicDBList initialMatchList = new BasicDBList();
        if (mongoTemplate.count(null, GenotypingProject.class) != 1)
			initialMatchList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID, genotypingProject.getId()));
    	
        if (nNVariantsQueriedAtOnce == 0)
        {	// splitting using tagged variants because we have no filter on variant features
        	BasicDBList chunkMatchAndList = new BasicDBList();
			if (currentTaggedVariant != null)
				chunkMatchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$gt", ObjectId.isValid(currentTaggedVariant.toString()) ? new ObjectId(currentTaggedVariant.toString()) : currentTaggedVariant)));
			currentTaggedVariant = variantCursor.hasNext() ? (Comparable) variantCursor.next().get("_id") : null;
			if (currentTaggedVariant != null)
				chunkMatchAndList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$lte", ObjectId.isValid(currentTaggedVariant.toString()) ? new ObjectId(currentTaggedVariant.toString()) : currentTaggedVariant)));
			initialMatchList.addAll(chunkMatchAndList);
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
	    	
	    	// if (predefinedVariantList != null && predefinedVariantList.size() > 0)
	        if (aVariantSubList.size() > 0)
				initialMatchList.add(new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$in", aVariantSubList)));
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
					geneNameDBO = new BasicDBObject("$in", MgdbDao.split(geneNames, ","));
				initialMatchList.add(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, geneNameDBO));
			}
			if (variantEffects.length() > 0){
				initialMatchList.add(new BasicDBObject(VariantRunData.SECTION_ADDITIONAL_INFO + "." + VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, new BasicDBObject("$in", MgdbDao.split(variantEffects, ","))));
                        }
                }
		pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", initialMatchList)));
		
        boolean fMultiRunProject = genotypingProject.getRuns().size() > 1;
        boolean fNeedProjectStage = fMultiRunProject || fieldsToReturn.size() > 0 || (genotypeQualityThreshold != null && genotypeQualityThreshold > 1) || (readDepthThreshold != null && readDepthThreshold > 1) || (cleanOperator != null && !fZygosityRegex);
		
        DBObject project = new BasicDBObject();        
        for (int k = 0; k < selectedIndividuals.size(); k++)
            for (Integer individualSample : individualIndexToSampleListMap.get(k))
            {
				String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + individualSample + "." + SampleGenotype.FIELDNAME_GENOTYPECODE;
				BasicDBList conditionsWhereGtIsAssimilatedToMissing = new BasicDBList();
				if (genotypeQualityThreshold != null && genotypeQualityThreshold > 1)
				{
					BasicDBList qualTooLowList = new BasicDBList();
					qualTooLowList.add("$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + individualSample + "." + SampleGenotype.SECTION_ADDITIONAL_INFO + "." + VariantData.GT_FIELD_GQ);
					qualTooLowList.add(genotypeQualityThreshold);

					BasicDBObject qualTooLow = new BasicDBObject("$lt", qualTooLowList);
					conditionsWhereGtIsAssimilatedToMissing.add(qualTooLow);
				}
				if (readDepthThreshold != null && readDepthThreshold > 1)
				{
					BasicDBList depthTooLowList = new BasicDBList();
					depthTooLowList.add("$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + individualSample + "." + SampleGenotype.SECTION_ADDITIONAL_INFO + "." + VariantData.GT_FIELD_DP);
					depthTooLowList.add(readDepthThreshold);

					BasicDBObject depthTooLow = new BasicDBObject("$lt", depthTooLowList);
					conditionsWhereGtIsAssimilatedToMissing.add(depthTooLow);
				}

				BasicDBList condComparisonList = new BasicDBList();
				condComparisonList.add(new BasicDBObject("$or", conditionsWhereGtIsAssimilatedToMissing));
				condComparisonList.add("");
				condComparisonList.add("$" + pathToGT);
                
                project.put(fMultiRunProject ? pathToGT.replaceAll("\\.", "¤") : pathToGT, conditionsWhereGtIsAssimilatedToMissing.size() == 0 ? 1 : new BasicDBObject("$cond", condComparisonList));
            }
        
		String separator = fMultiRunProject ? "¤" : ".";
        if (fNeedProjectStage)
        {	// we will need to use the $project stage, either to return specific fields (not simply counting but actually getting records), or because we need to compute new fields on the fly for specific filters
			for (String field : fieldsToReturn)
				if (field != null && field.length() > 0)
					project.put(field.replaceAll("\\.", "¤"), "$" + field);	        
        }
        
    	// Group records by variant id
		DBObject groupFields = new BasicDBObject("_id", "$_id." + VariantRunDataId.FIELDNAME_VARIANT_ID);
    	BasicDBList uniformityList = new BasicDBList();
		for (int j=0; j<selectedIndividuals.size(); j++)
		{
			Integer firstSample = null;
			for (Integer individualSample : individualIndexToSampleListMap.get(j))
	    	{
				String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + individualSample + "." + SampleGenotype.FIELDNAME_GENOTYPECODE;
				groupFields.put(pathToGT.replaceAll("\\.", "¤"), new BasicDBObject("$addToSet", "$" + pathToGT));
				
    			// Check uniformity between genotypes related to a same individual
				if (firstSample == null)
					firstSample = individualSample;
				else
				{
					DBObject comparisonDBObject = new BasicDBObject();
					comparisonDBObject.put("$eq", new String[] {"$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + firstSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE, "$" + pathToGT.replaceAll("\\.", "¤")});
					uniformityList.add(comparisonDBObject);
				}
	    	}
		}
		if (uniformityList.size() > 0)
			project.put("u", new BasicDBObject("$and", uniformityList));
		
		if (fMultiRunProject)
			pipeline.add(new BasicDBObject("$group", groupFields));
		if (fNeedProjectStage && !project.keySet().isEmpty())
			pipeline.add(new BasicDBObject("$project", project));

		if (cleanOperator != null)
        {
            if (selectedIndividuals.size() >= 1)
            {
          		BasicDBList genotypeMatchList = new BasicDBList();
    			if (uniformityList.size() > 0)
    				genotypeMatchList.add(new BasicDBObject("u", true));
            	
 				// Query to match specific genotype code with zygosity regex (homozygous var, homozygous ref, heterozygous)
				if (fZygosityRegex)	
				{
					BasicDBList orSelectedGenotypeRegexAndFieldExistList = new BasicDBList();
					DBObject orFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
					DBObject andFinalSelectedGenotypeRegexAndFieldExist = new BasicDBObject();
				
					for (int j=0; j<selectedIndividuals.size(); j++)
					{
						for (Integer individualSample : individualIndexToSampleListMap.get(j))
				    	{
							String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + individualSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE;
							BasicDBList orList = new BasicDBList();
							DBObject clause1 = new BasicDBObject(pathToGT, new BasicDBObject("$exists", false));
				    		DBObject clause2 = new BasicDBObject(pathToGT, new BasicDBObject("$regex", cleanOperator));
							orList.add(clause1);
							orList.add(clause2);
							orSelectedGenotypeRegexAndFieldExistList.add(new BasicDBObject("$or", orList));				
				    	}
					}
										
					if (fOr)
					{		
						orFinalSelectedGenotypeRegexAndFieldExist.put("$or", orSelectedGenotypeRegexAndFieldExistList);
						genotypeMatchList.add(orFinalSelectedGenotypeRegexAndFieldExist);
				    }			
					else
					{
						andFinalSelectedGenotypeRegexAndFieldExist.put("$and", orSelectedGenotypeRegexAndFieldExistList);
						genotypeMatchList.add(andFinalSelectedGenotypeRegexAndFieldExist);
					}
				}
				else if (fIsRealisticGenotypeQuery)
                {	// only for bi-allelic: query that requires every allele present in heterozygous genotypes to be also present in homozygous ones
                    ArrayList<BasicDBObject> totalHR = new ArrayList<BasicDBObject>();
                    ArrayList<BasicDBObject> totalHV = new ArrayList<BasicDBObject>();
                    ArrayList<BasicDBObject> totalHZ = new ArrayList<BasicDBObject>();
                    for(int j = 0; j < selectedIndividuals.size(); j++)
                    {
                        for (Integer individualSample : individualIndexToSampleListMap.get(j))
                        {
							/* Step to project needed fields :
							/* assign the value 1 to field where genotype equals 0/0 or 1/1 or 0/1, otherwise 0. Field are called like this : 'project::individual' + respectively HR/HV/HZ
							 * assign empty string to individuals that contains missing data : Field are called like this : 'project::individual'
							 * assign empty string to individuals that contains DP value less than threshold : Field are called like this : 'project::individualDP'
							 */
							String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + individualSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE;

							BasicDBList condList1 = new BasicDBList();
							condList1.add(new BasicDBObject("$eq", new Object[] { "$" + pathToGT, fMultiRunProject ? new Object[] {"0/0"} : "0/0" } ));
							condList1.add(1);
							condList1.add(0);
							BasicDBList condList2 = new BasicDBList();
							condList2.add(new BasicDBObject("$eq", new Object[] { "$" + pathToGT, fMultiRunProject ? new Object[] {"1/1"} : "1/1" } ));
							condList2.add(1);
							condList2.add(0);
							BasicDBList condList3 = new BasicDBList();
							condList3.add(new BasicDBObject("$eq", new Object[] { "$" + pathToGT, fMultiRunProject ? new Object[] {"0/1"} : "0/1" } ));
							condList3.add(1);
							condList3.add(0);
							
							totalHR.add(new BasicDBObject("$cond", condList1));
							totalHV.add(new BasicDBObject("$cond", condList2));
							totalHZ.add(new BasicDBObject("$cond", condList3));
                        }
                    }

                    project.put("HR", new BasicDBObject("$add", totalHR));
                    project.put("HV", new BasicDBObject("$add", totalHV));
                    project.put("HZ", new BasicDBObject("$add", totalHZ));

					/* 
					 * Match step to get individuals which respond to different case.
					 * We don't want individuals that have only genotype 1/1 and 0/1 or 0/0 and 0/1.
					 * -/- Example -\- 
					 * To match case 1 "at least all kind of genotypes are present " :
					 * { "$match" : { "$or" : [ { "$and" : [ { "HR" : { "$gt" : 0}} , 
					 * { "HV" : { "$gt" : 0}} , { "HZ" : { "$gt" : 0}}]}]}}
					 */
					
                    BasicDBList orList = new BasicDBList();
                    BasicDBList andListCase1 = new BasicDBList();
                    andListCase1.add(new BasicDBObject("HR", new BasicDBObject("$gt", 0)));
                    andListCase1.add(new BasicDBObject("HV", new BasicDBObject("$gt", 0)));
                    andListCase1.add(new BasicDBObject("HZ", new BasicDBObject("$gt", 0)));
                    orList.add(new BasicDBObject("$and", andListCase1));
                    BasicDBList andListCase2 = new BasicDBList();
                    andListCase2.add(new BasicDBObject("HR", new BasicDBObject("$gt", 0)));
                    andListCase2.add(new BasicDBObject("HV", new BasicDBObject("$eq", 0)));
                    andListCase2.add(new BasicDBObject("HZ", new BasicDBObject("$eq", 0)));
                    orList.add(new BasicDBObject("$and", andListCase2));
                    BasicDBList andListCase3 = new BasicDBList();
                    andListCase3.add(new BasicDBObject("HR", new BasicDBObject("$eq", 0)));
                    andListCase3.add(new BasicDBObject("HV", new BasicDBObject("$gt", 0)));
                    andListCase3.add(new BasicDBObject("HZ", new BasicDBObject("$eq", 0)));
                    orList.add(new BasicDBObject("$and", andListCase3));
                    BasicDBList andListCase4 = new BasicDBList();
                    andListCase4.add(new BasicDBObject("HR", new BasicDBObject("$gt", 0)));
                    andListCase4.add(new BasicDBObject("HV", new BasicDBObject("$gt", 0)));
                    andListCase4.add(new BasicDBObject("HZ", new BasicDBObject("$eq", 0)));
                    orList.add(new BasicDBObject("$and", andListCase4));
                    genotypeMatchList.add(new BasicDBObject("$or", orList));
                }
				else
				{	// Query to compare at genotypes of individuals (All same, All different, Not all same, Not all different)
			        ArrayList<DBObject> comparisonList = new ArrayList<DBObject>();

					for (int i=0; i<selectedIndividuals.size(); i++)
						for (Integer firstIndividualSample : individualIndexToSampleListMap.get(i))
							for (int j=("$eq".equals(cleanOperator) ? (selectedIndividuals.size()-1) : (i+1)); j<selectedIndividuals.size(); j++)
								for (Integer secondIndividualSample : individualIndexToSampleListMap.get(j))
									if (i != j)
									{	/* do we need to make sure each genotype actually exists?!? */
										String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + secondIndividualSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE;
										DBObject comparisonDBObject = new BasicDBObject();
										comparisonDBObject.put(cleanOperator, new String[] {"$" + VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + firstIndividualSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE, "$" + pathToGT});
										project.put("c" + i + "_" + j, comparisonDBObject);
										BasicDBObject dbo = new BasicDBObject("c" + i + "_" + j, fNegateMatch ? false : true);
										if (!comparisonList.contains(dbo))
											comparisonList.add(dbo);
									}
					genotypeMatchList.add(new BasicDBObject(fNegateMatch ? "$or" : "$and", comparisonList));
				}
				pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", genotypeMatchList)));
            }
        }

        boolean fMafRequested = maxmaf != null && maxmaf.floatValue() < 50F || minmaf != null && minmaf.floatValue() > 0.0F;
        if ((missingData != null && missingData < 100) || fMafRequested)
        {	// do some allele counting
            DBObject mafAndMissingDataProject = new BasicDBObject();
            BasicDBList calledGenotypeTotalCountList = new BasicDBList();
            BasicDBList mafTotalCountList = new BasicDBList();
            double nTotalSampleCount = 0;
            for(int j = 0; j < selectedIndividuals.size(); j++)
            {
                for (Integer individualSample : individualIndexToSampleListMap.get(j))
                {
                	nTotalSampleCount++;
                	String pathToGT = VariantRunData.FIELDNAME_SAMPLEGENOTYPES + separator + individualSample + separator + SampleGenotype.FIELDNAME_GENOTYPECODE;
                	
                    calledGenotypeTotalCountList.add(new BasicDBObject("$cmp", new Object[] {"$" + pathToGT, fMultiRunProject ? new Object[] {""} : ""}));
                    if (fMafRequested)
                    {
                        BasicDBList macConditionList = new BasicDBList();
                        String allRefGtCode = genotypingProject.getPloidyLevel() != 1 ? "0/0" : "0";
                        macConditionList.add(new BasicDBObject("$eq", new Object[] {"$" + pathToGT, fMultiRunProject ? new Object[] {allRefGtCode} : allRefGtCode}));
                        macConditionList.add(0);
                        if (genotypingProject.getPloidyLevel() == 1)
                            macConditionList.add(2);
                        else
                        	macConditionList.add(new BasicDBObject("$add", new Object[] {1, new BasicDBObject("$cmp",  new Object[] {"$" + pathToGT, fMultiRunProject ? new Object[] {"0/1"} : "0/1"})}));
                        mafTotalCountList.add(new BasicDBObject("$cond", macConditionList));
                    }
                }
            }

            mafAndMissingDataProject.put("t", new BasicDBObject("$add", calledGenotypeTotalCountList));            
           	pipeline.add(new BasicDBObject("$project", mafAndMissingDataProject));

            BasicDBList mafAndMissingDataDBList = new BasicDBList();            
            if (missingData != null && missingData < 100)
            {
                double minimumGenotypeCount = nTotalSampleCount - (nTotalSampleCount * missingData) / 100;
//                LOG.debug("Requiring " + minimumGenotypeCount + " existing genotypes out of " + nTotalSampleCount);
                mafAndMissingDataDBList.add(new BasicDBObject("t", new BasicDBObject("$gte", minimumGenotypeCount)));
            }
            
            if (fMafRequested)
            {
                mafAndMissingDataProject.put("m", new BasicDBObject("$add", mafTotalCountList));
                
            	// avoid dividing by zero
                BasicDBList totalAlleleCount = new BasicDBList();
                totalAlleleCount.add(new BasicDBObject("$eq", ((Object) (new Object[] { "$t", 0 }))));
                totalAlleleCount.add(null);
                totalAlleleCount.add("$t");
                
                BasicDBObject projectFieldConditionMaf = new BasicDBObject("f", new BasicDBObject("$divide", (new Object[] { new BasicDBObject("$multiply", (new Object[] { "$m", 50 })), new BasicDBObject("$cond", totalAlleleCount) })));
                if (missingData != null && missingData < 100)
                    projectFieldConditionMaf.put("t", 1);

                pipeline.add(new BasicDBObject("$project", projectFieldConditionMaf));
                BasicDBList orMafMatch = new BasicDBList();
                BasicDBList andMafMatch = new BasicDBList();
                andMafMatch.add(new BasicDBObject("f", new BasicDBObject("$gte", minmaf)));
                andMafMatch.add(new BasicDBObject("f", new BasicDBObject("$lte", maxmaf)));
                orMafMatch.add(new BasicDBObject("$and", andMafMatch));
                andMafMatch = new BasicDBList();
                andMafMatch.add(new BasicDBObject("f", new BasicDBObject("$lte", Float.valueOf(100F - minmaf.floatValue()))));
                andMafMatch.add(new BasicDBObject("f", new BasicDBObject("$gte", Float.valueOf(100F - maxmaf.floatValue()))));
                orMafMatch.add(new BasicDBObject("$and", andMafMatch));
                mafAndMissingDataDBList.add(new BasicDBObject("$or", orMafMatch));
            }
            pipeline.add(new BasicDBObject("$match", new BasicDBObject("$and", mafAndMissingDataDBList)));
        }

        if (fieldsToReturn.size() == 0)	// reduce output size to the minimum if we are not loading fields for display
        	pipeline.add(new BasicDBObject("$project", new BasicDBObject("_id", "$_id" + (!fMultiRunProject ? "." + VariantRunDataId.FIELDNAME_VARIANT_ID : ""))));

        return pipeline;
    }

	/**
	 * Cleanup.
	 */
	public void cleanup() {
		variantCursor.close();
	}
}
