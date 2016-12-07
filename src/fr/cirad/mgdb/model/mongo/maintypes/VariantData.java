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
package fr.cirad.mgdb.model.mongo.maintypes;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.SampleId;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;

// TODO: Auto-generated Javadoc
/**
 * The Class VariantData.
 */
@Document(collection = "variants")
@TypeAlias("VD")
public class VariantData
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
	/** The Constant FIELDNAME_VERSION. */
	public final static String FIELDNAME_VERSION = "v";
	
	/** The Constant FIELDNAME_ANALYSIS_METHODS. */
	public final static String FIELDNAME_ANALYSIS_METHODS = "am";
	
	/** The Constant FIELDNAME_SYNONYMS. */
	public final static String FIELDNAME_SYNONYMS = "sy";
	
	/** The Constant FIELDNAME_KNOWN_ALLELE_LIST. */
	public final static String FIELDNAME_KNOWN_ALLELE_LIST = "ka";
	
	/** The Constant FIELDNAME_TYPE. */
	public final static String FIELDNAME_TYPE = "ty";
	
	/** The Constant FIELDNAME_REFERENCE_POSITION. */
	public final static String FIELDNAME_REFERENCE_POSITION = "rp";
	
	/** The Constant FIELDNAME_PROJECT_DATA. */
	public final static String FIELDNAME_PROJECT_DATA = "pj";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA = "il";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_NCBI. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_NCBI = "nc";
	
	/** The Constant FIELDNAME_SYNONYM_TYPE_ID_INTERNAL. */
	public final static String FIELDNAME_SYNONYM_TYPE_ID_INTERNAL = "in";
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";

	/** The Constant FIELD_PHREDSCALEDQUAL. */
	public static final String FIELD_PHREDSCALEDQUAL = "qual";
	
	/** The Constant FIELD_SOURCE. */
	public static final String FIELD_SOURCE = "name";
	
	/** The Constant FIELD_FILTERS. */
	public static final String FIELD_FILTERS = "filt";
	
	/** The Constant FIELD_FULLYDECODED. */
	public static final String FIELD_FULLYDECODED = "fullDecod";
	
	/** The Constant FIELDVAL_SOURCE_MISSING. */
	public static final String FIELDVAL_SOURCE_MISSING = "Unknown";
	
	/** The Constant GT_FIELD_GQ. */
	public static final String GT_FIELD_GQ = "GQ";
	
	/** The Constant GT_FIELD_DP. */
	public static final String GT_FIELD_DP = "DP";
	
	/** The Constant GT_FIELD_AD. */
	public static final String GT_FIELD_AD = "AD";
	
	/** The Constant GT_FIELD_PL. */
	public static final String GT_FIELD_PL = "PL";
	
	/** The Constant GT_FIELD_PHASED_GT. */
	public static final String GT_FIELD_PHASED_GT = "phGT";
	
	/** The Constant GT_FIELD_PHASED_ID. */
	public static final String GT_FIELD_PHASED_ID = "phID";

	/** The Constant GT_FIELDVAL_AL_MISSING. */
	public static final String GT_FIELDVAL_AL_MISSING = ".";
	
	/** The Constant GT_FIELDVAL_ID_MISSING. */
	public static final String GT_FIELDVAL_ID_MISSING = ".";
	
	/** The id. */
	@Id
	private Comparable id;
	
	/** The version. */
	@Version
	@Field(FIELDNAME_VERSION)
    private Long version;
	
	/** The type. */
	@Indexed
	@Field(FIELDNAME_TYPE)
	private String type;

	/** The reference position. */
	@Field(FIELDNAME_REFERENCE_POSITION)
	ReferencePosition referencePosition = null;
	
	/** The synonyms. */
	@Field(FIELDNAME_SYNONYMS)
	private TreeMap<String /*synonym type*/, TreeSet<Comparable>> synonyms = new TreeMap<String, TreeSet<Comparable>>();

	/** The list of analysis methods. */
	@Field(FIELDNAME_ANALYSIS_METHODS)
	private List<String> analysisMethods = null;

	/** The known allele list. */
	@Field(FIELDNAME_KNOWN_ALLELE_LIST)
	private List<String> knownAlleleList = new ArrayList<String>();

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Comparable> additionalInfo = null;


	/**
	 * Instantiates a new variant data.
	 */
	public VariantData() {
		super();
	}
	
	/**
	 * Instantiates a new variant data.
	 *
	 * @param id the id
	 */
	public VariantData(Comparable id) {
		super();
		this.id = id;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public Comparable getId() {
		return id;
	}

	/**
	 * Gets the version.
	 *
	 * @return the version
	 */
	public Long getVersion() {
		return version;
	}

	/**
	 * Sets the version.
	 *
	 * @param version the new version
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	/**
	 * Gets the synonyms.
	 *
	 * @return the synonyms
	 */
	public TreeMap<String, TreeSet<Comparable>> getSynonyms() {
		return synonyms;
	}

	/**
	 * Sets the synonyms.
	 *
	 * @param synonyms the synonyms
	 */
	public void setSynonyms(TreeMap<String, TreeSet<Comparable>> synonyms) {
		this.synonyms = synonyms;
	}
	
	public List<String> getAnalysisMethods() {
		return analysisMethods;
	}

	public void setAnalysisMethods(List<String> analysisMethods) {
		this.analysisMethods = analysisMethods;
	}

	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * Sets the type.
	 *
	 * @param type the new type
	 */
	public void setType(String type) {
		this.type = type.intern();
	}

	/**
	 * Gets the reference position.
	 *
	 * @return the reference position
	 */
	public ReferencePosition getReferencePosition() {
		return referencePosition;
	}

	/**
	 * Sets the reference position.
	 *
	 * @param referencePosition the new reference position
	 */
	public void setReferencePosition(ReferencePosition referencePosition) {
		this.referencePosition = referencePosition;
	}
	
	/**
	 * Gets the known allele list.
	 *
	 * @return the known allele list
	 */
	public List<String> getKnownAlleleList() {
		return knownAlleleList;
	}

	/**
	 * Sets the known allele list.
	 *
	 * @param knownAlleleList the new known allele list
	 */
	public void setKnownAlleleList(List<String> knownAlleleList) {
		this.knownAlleleList = knownAlleleList;
		for (String allele : this.knownAlleleList)
			allele.intern();
	}
	
	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Comparable> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new HashMap<String, Comparable>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(HashMap<String, Comparable> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * Static get alleles from genotype code.
	 *
	 * @param alleleList the allele list
	 * @param code the code
	 * @return the list
	 * @throws Exception the exception
	 */
	static public List<String> staticGetAllelesFromGenotypeCode(List<String> alleleList, String code) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();
		if (code.length() > 0)
		{
			for (String alleleCodeIndex : Helper.split(code.replaceAll("\\|", "/"), "/"))
			{
				int nAlleleCodeIndex = Integer.parseInt(alleleCodeIndex);
				if (alleleList.size() > nAlleleCodeIndex)
					result.add(alleleList.get(nAlleleCodeIndex));
				else
					throw new Exception("Variant has no allele number " + nAlleleCodeIndex);
			}
		}
		return result;
	}

	/**
	 * Gets the alleles from genotype code.
	 *
	 * @param code the code
	 * @return the alleles from genotype code
	 * @throws Exception the exception
	 */
	public List<String> getAllelesFromGenotypeCode(String code) throws Exception
	{
		try
		{
			return staticGetAllelesFromGenotypeCode(knownAlleleList, code);
		}
		catch (Exception e)
		{
			throw new Exception("Variant ID: " + getId(), e);
		}
	}
	
	/**
	 * Rebuild vcf format genotype.
	 *
	 * @param alternates the alternates
	 * @param genotypeAlleles the genotype alleles
	 * @param genotypeAllele the genotype allele
	 * @param keepCurrentPhasingInfo the keep current phasing info
	 * @return the string
	 * @throws Exception the exception
	 */
	public static String rebuildVcfFormatGenotype(List<Allele> alternates, List<Allele> genotypeAlleles, String genotypeAllele, boolean keepCurrentPhasingInfo) throws Exception
	{
		String result = "";
		List<Allele> orderedGenotypeAlleles = new ArrayList<Allele>();
		orderedGenotypeAlleles.addAll(genotypeAlleles);
		if(!keepCurrentPhasingInfo){
			Collections.sort(orderedGenotypeAlleles);
		
		mainLoop: for (Allele gtA : orderedGenotypeAlleles)
			if (gtA.isReference())
				result += (result.length() == 0 ? "" : "/") + 0;
			else
			{
				for (int i=0; i<alternates.size(); i++)
				{
					Allele altA = alternates.get(i);
					if (altA.equals(gtA))
					{
						result += (result.length() == 0 ? "" : "/") + (i+1);
						continue mainLoop;						
					}
				}
				if (!GT_FIELDVAL_AL_MISSING.equals(gtA.getBaseString()))
					throw new Exception("Unable to find allele '" + gtA.getBaseString() + "' in alternate list");
			}
		}
		else{
			mainLoop: for (Allele gtA : orderedGenotypeAlleles)
				if (gtA.isReference())
					result += (result.length() == 0 ? "" : genotypeAllele.contains("|") ? "|" : "/") + 0;
				else
				{
					for (int i=0; i<alternates.size(); i++)
					{
						Allele altA = alternates.get(i);
						if (altA.equals(gtA))
						{
							result += (result.length() == 0 ? "" : genotypeAllele.contains("|") ? "|" : "/") + (i+1);
							continue mainLoop;						
						}
					}
					if (!GT_FIELDVAL_AL_MISSING.equals(gtA.getBaseString()))
						throw new Exception("Unable to find allele '" + gtA.getBaseString() + "' in alternate list");
				}
		}
		return result;
	}
		
	/**
	 * To variant context.
	 *
	 * @param runs the runs
	 * @param exportVariantIDs the export variant i ds
	 * @param sampleIDToIndividualIdMap the sample id to individual id map
	 * @param previousPhasingIds the previous phasing ids
	 * @param nMinimumGenotypeQuality the n minimum genotype quality
	 * @param nMinimumReadDepth the n minimum read depth
	 * @param warningFileWriter the warning file writer
	 * @param synonym the synonym
	 * @return the variant context
	 * @throws Exception the exception
	 */
	public VariantContext toVariantContext(Collection<VariantRunData> runs, boolean exportVariantIDs, LinkedHashMap<SampleId, String> sampleIDToIndividualIdMap, HashMap<SampleId, Comparable> previousPhasingIds, int nMinimumGenotypeQuality, int nMinimumReadDepth, FileWriter warningFileWriter, Comparable synonym) throws Exception
	{
		ArrayList<Genotype> genotypes = new ArrayList<Genotype>();
		String sRefAllele = knownAlleleList.size() == 0 ? "" : knownAlleleList.get(0);

		ArrayList<Allele> variantAlleles = new ArrayList<Allele>();
		variantAlleles.add(Allele.create(sRefAllele, true));
		
		// collect all genotypes for all individuals
		Map<String/*individual*/, HashMap<String/*genotype code*/, List<SampleId>>> individualSamplesByGenotype = new LinkedHashMap<String, HashMap<String, List<SampleId>>>();
		
		HashMap<SampleId, SampleGenotype> sampleGenotypes = new HashMap<SampleId, SampleGenotype>();
		List<VariantRunData> runsWhereDataWasFound = new ArrayList<VariantRunData>();
		List<String> individualList = new ArrayList<String>();
		for (SampleId spId : sampleIDToIndividualIdMap.keySet())
		{
			if (runs == null || runs.size() == 0)
				continue;
			
			Integer sampleIndex = spId.getSampleIndex();
			
			for (VariantRunData run : runs)
			{
				SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sampleIndex);
				if (sampleGenotype == null)
					continue;	// run contains no data for this sample
				
				// keep track of SampleGenotype and Run so we can have access to additional info later on
				sampleGenotypes.put(spId, sampleGenotype);
				if (!runsWhereDataWasFound.contains(run))
					runsWhereDataWasFound.add(run);
				
				String gtCode = /*isPhased ? (String) sampleGenotype.getAdditionalInfo().get(GT_FIELD_PHASED_GT) : */sampleGenotype.getCode();
				String individualId = sampleIDToIndividualIdMap.get(spId);
				if (!individualList.contains(individualId))
					individualList.add(individualId);
				HashMap<String, List<SampleId>> storedIndividualGenotypes = individualSamplesByGenotype.get(individualId);
				if (storedIndividualGenotypes == null) {
					storedIndividualGenotypes = new HashMap<String, List<SampleId>>();
					individualSamplesByGenotype.put(individualId, storedIndividualGenotypes);
				}
				List<SampleId> samplesWithGivenGenotype = storedIndividualGenotypes.get(gtCode);
				if (samplesWithGivenGenotype == null)
				{
					samplesWithGivenGenotype = new ArrayList<SampleId>();
					storedIndividualGenotypes.put(gtCode, samplesWithGivenGenotype);
				}
				samplesWithGivenGenotype.add(spId);
			}
		}
			
		individualLoop : for (String individualId : individualList)
		{
			HashMap<String, List<SampleId>> samplesWithGivenGenotype = individualSamplesByGenotype.get(individualId);
			HashMap<Object, Integer> genotypeCounts = new HashMap<Object, Integer>(); // will help us to keep track of missing genotypes
				
			int highestGenotypeCount = 0;
			String mostFrequentGenotype = null;
			if (genotypes != null && samplesWithGivenGenotype != null)
				for (String gtCode : samplesWithGivenGenotype.keySet())
				{
					if (gtCode.length() == 0)
						continue; /* skip missing genotypes */

					int gtCount = samplesWithGivenGenotype.get(gtCode).size();
					if (gtCount > highestGenotypeCount) {
						highestGenotypeCount = gtCount;
						mostFrequentGenotype = gtCode;
					}
					genotypeCounts.put(gtCode, gtCount);
				}
			
			if (mostFrequentGenotype == null)
				continue;	// no genotype for this individual
			
			if (warningFileWriter != null && genotypeCounts.size() > 1)
				warningFileWriter.write("- Dissimilar genotypes found for variant " + (synonym == null ? getId() : synonym) + ", individual " + individualId + ". Exporting most frequent: " + mostFrequentGenotype + "\n");
			
			SampleId spId = samplesWithGivenGenotype.get(mostFrequentGenotype).get(0);	// any will do
			SampleGenotype sampleGenotype = sampleGenotypes.get(spId);
			
			Comparable currentPhId = sampleGenotype.getAdditionalInfo().get(GT_FIELD_PHASED_ID);
			
			boolean isPhased = currentPhId != null && currentPhId.equals(previousPhasingIds.get(spId));

			List<String> alleles = /*mostFrequentGenotype == null ? new ArrayList<String>() :*/ getAllelesFromGenotypeCode(isPhased ? (String) sampleGenotype.getAdditionalInfo().get(GT_FIELD_PHASED_GT) : mostFrequentGenotype);
			ArrayList<Allele> individualAlleles = new ArrayList<Allele>();
			previousPhasingIds.put(spId, currentPhId == null ? getId() : currentPhId);
			if (alleles.size() == 0)
				continue;	/* skip this sample because there is no genotype for it */
			
			boolean fAllAllelesNoCall = true;
			for (String allele : alleles)
				if (allele.length() > 0)
				{
					fAllAllelesNoCall = false;
					break;
				}
			for (String sAllele : alleles)
			{
				Allele allele = Allele.create(sAllele.length() == 0 ? (fAllAllelesNoCall ? Allele.NO_CALL_STRING : "<DEL>") : sAllele, sRefAllele.equals(sAllele));
				if (!allele.isNoCall() && !variantAlleles.contains(allele))
					variantAlleles.add(allele);
				individualAlleles.add(allele);
			}

			GenotypeBuilder gb = new GenotypeBuilder(individualId, individualAlleles);
			if (individualAlleles.size() > 0)
			{
				gb.phased(isPhased);
				String genotypeFilters = (String) sampleGenotype.getAdditionalInfo().get(FIELD_FILTERS);
				if (genotypeFilters != null && genotypeFilters.length() > 0)
					gb.filter(genotypeFilters);
				
				for (String key : sampleGenotype.getAdditionalInfo().keySet())
					if (GT_FIELD_GQ.equals(key))
					{
						Integer gq = (Integer) sampleGenotype.getAdditionalInfo().get(key);
						if (gq != null && gq < nMinimumGenotypeQuality)
							continue individualLoop;
						if (gq != null)
							gb.GQ(gq.intValue());
						else
							gb.noGQ();
					}
					else if (GT_FIELD_DP.equals(key))
					{
						Integer dp = (Integer) sampleGenotype.getAdditionalInfo().get(key);
						if (dp != null && dp < nMinimumReadDepth)
							continue individualLoop;
						if (dp != null)
							gb.DP(dp.intValue());
						else
							gb.noDP();
					}
					else if (GT_FIELD_AD.equals(key))
					{
						String ad = (String) sampleGenotype.getAdditionalInfo().get(key);
						if (ad != null)
							gb.AD(MgdbDao.csvToIntArray(ad));
						else
							gb.noAD();
					}
					else if (GT_FIELD_PL.equals(key))
					{
						String pl = (String) sampleGenotype.getAdditionalInfo().get(key);
						if (pl != null)
							gb.PL(MgdbDao.csvToIntArray(pl));
						else
							gb.noPL();
					}
					else if (!key.equals(VariantData.GT_FIELD_PHASED_GT) && !key.equals(VariantData.GT_FIELD_PHASED_ID) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME)) // exclude some internally created fields that we don't want to export
						gb.attribute(key, sampleGenotype.getAdditionalInfo().get(key)); // looks like we have an extended attribute
			}
			genotypes.add(gb.make());
		}

		VariantRunData run = runsWhereDataWasFound.size() == 1 ? runsWhereDataWasFound.get(0) : null;	// if there is not exactly one run involved then we do not export meta-data
		String source = run == null ? null : (String) run.getAdditionalInfo().get(FIELD_SOURCE);

		Long start = referencePosition == null ? null : referencePosition.getStartSite(), stop = referencePosition == null ? null : (referencePosition.getEndSite() == null ? start : referencePosition.getEndSite());
		String chr = referencePosition == null ? null : referencePosition.getSequence();
		VariantContextBuilder vcb = new VariantContextBuilder(source != null ? source : FIELDVAL_SOURCE_MISSING, chr != null ? chr : "", start != null ? start : 0, stop != null ? stop : 0, variantAlleles);
		if (exportVariantIDs)
			vcb.id((synonym == null ? getId() : synonym).toString());
		vcb.genotypes(genotypes);
		
		if (run != null)
		{
			Boolean fullDecod = (Boolean) run.getAdditionalInfo().get(FIELD_FULLYDECODED);
			vcb.fullyDecoded(fullDecod != null && fullDecod);
	
			String filters = (String) run.getAdditionalInfo().get(FIELD_FILTERS);
			if (filters != null)
				vcb.filters(filters.split(","));
			else
				vcb.filters(VCFConstants.UNFILTERED);
			
			Number qual = (Number) run.getAdditionalInfo().get(FIELD_PHREDSCALEDQUAL);
			if (qual != null)
				vcb.log10PError(qual.doubleValue() / -10.0D);
			
			List<String> alreadyTreatedAdditionalInfoFields = Arrays.asList(new String[] {FIELD_SOURCE, FIELD_FULLYDECODED, FIELD_FILTERS, FIELD_PHREDSCALEDQUAL});
			for (String attrName : run.getAdditionalInfo().keySet())
				if (!VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME.equals(attrName) && !VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE.equals(attrName) && !alreadyTreatedAdditionalInfoFields.contains(attrName))
					vcb.attribute(attrName, run.getAdditionalInfo().get(attrName));
		}
		VariantContext vc = vcb.make();
		return vc;
	}
	
    @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantData))
			return false;
		
		return getId().equals(((VariantData)o).getId());
	}
}