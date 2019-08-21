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
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

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

	/** The analysis methods. */
	@Field(FIELDNAME_ANALYSIS_METHODS)
	private TreeSet<String> analysisMethods = null;

	/** The known allele list. */
	@Field(FIELDNAME_KNOWN_ALLELE_LIST)
	private List<String> knownAlleleList = new ArrayList<String>();

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Comparable> additionalInfo = null;

	static public int[] fixAdFieldValue(int[] importedAD, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
    {
    	List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
    				.map(Allele.class::cast)
    				.map(allele -> allele.getBaseString()).collect(Collectors.toList());
    	
    	if (importedAllelesAsStrings.isEmpty())
    		importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
    	
    	if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
    	{
//    		System.out.println("AD: no fix needed for " + Helper.arrayToCsv(", ", importedAD));
    		return importedAD;
    	}

    	HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
    	for (int i=0; i<importedAlleles.size(); i++)
    	{
    		String allele = importedAllelesAsStrings.get(i);
    		int knownAlleleIndex = knownAlleles.indexOf(allele);
    		if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
    			knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
    	}
    	int[] adToStore = new int[knownAlleles.size()];
    	for (int i=0; i<adToStore.length; i++)
    	{
    		Integer importedAlleleIndex = knownAlleleToImportedAlleleIndexMap.get(i);
    		adToStore[i] = importedAlleleIndex == null ? 0 : importedAD[importedAlleleIndex];
    	}
//		System.out.println("AD: " + Helper.arrayToCsv(", ", importedAD) + " -> " + Helper.arrayToCsv(", ", adToStore));

    	return adToStore;
    }

	static public int[] fixPlFieldValue(int[] importedPL, int ploidy, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
	{
    	List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
				.map(Allele.class::cast)
				.map(allele -> allele.getBaseString()).collect(Collectors.toList());
    	
    	if (importedAllelesAsStrings.isEmpty())
    		importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
	
    	if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
    	{
//    		System.out.println("PL: no fix needed for " + Helper.arrayToCsv(", ", importedPL));
    		return importedPL;
    	}
    	
    	HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
    	for (int i=0; i<importedAlleles.size(); i++)
    	{
    		String allele = importedAllelesAsStrings.get(i);
    		int knownAlleleIndex = knownAlleles.indexOf(allele);
    		if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
    			knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
    	}
    	
    	int[] plToStore = new int[bcf_ap2g(knownAlleles.size(), ploidy)];
    	for (int i=0; i<plToStore.length; i++)
    	{
    		int[] genotype = bcf_ip2g(i, ploidy);
    		for (int j=0; j<genotype.length; j++)	// convert genotype to match the provided allele ordering
    		{
    			Integer importedAllele = knownAlleleToImportedAlleleIndexMap.get(genotype[j]);
    			if (importedAllele == null)
    			{
    				genotype = null;
    				break;	// if any allele is not part of the imported ones then the whole genotype is not represented
    			}
    			else
    				genotype[j] = importedAllele;
    		}
    		if (genotype != null)
    			Arrays.sort(genotype);
    		
    		plToStore[i] = genotype == null ? Integer.MAX_VALUE : importedPL[(int) bcf_g2i(genotype, ploidy)];
    	}
//    	System.out.println("PL: " + Helper.arrayToCsv(", ", importedPL) + " -> " + Helper.arrayToCsv(", ", plToStore));
    	
		return plToStore;
	}
	
	/**
	 * Gets number of genotypes from number of alleles and ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int bcf_ap2g(int no_allele, int no_ploidy)
	{
		if (no_ploidy==1 || no_allele<=1)
	        return no_allele;
	    else if (no_ploidy==2)
	        return (((no_allele+1)*(no_allele))>>1);
	    else
	        return (int) Helper.choose(no_ploidy+no_allele-1, no_allele-1);
	}
		
	/**
	 * Gets index of a genotype of n ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int bcf_g2i(int[] g, int n)
	{
	    if (n==1)
	        return g[0];
	    if (n==2)
	        return g[0] + (((g[1]+1)*(g[1]))>>1);
	    else
	    {
	    	int index = 0;
	        for (int i=0; i<n; ++i)
	            index += bcf_ap2g(g[i], i+1);
	        return index;
	    }
	}
	
	/**
	 * Gets genotype from genotype index and ploidy.
	 * Translated from original C++ code that was part of the project https://github.com/atks/vt
	 */
	static public int[] bcf_ip2g(int genotype_index, int no_ploidy)
	{
	    int[] genotype = new int[no_ploidy];
	    int pth = no_ploidy;
	    int max_allele_index = genotype_index;
	    int leftover_genotype_index = genotype_index;
	    while (pth>0)
	    {
	        for (int allele_index=0; allele_index <= max_allele_index; ++allele_index)
	        {
	            double i = Helper.choose(pth+allele_index-1, pth);
	            if (i>=leftover_genotype_index || allele_index==max_allele_index)
	            {
	                if (i>leftover_genotype_index)
	                	--allele_index;
	                leftover_genotype_index -= Helper.choose(pth+allele_index-1, pth);
	                --pth;
	                max_allele_index = allele_index;
	                genotype[pth] = allele_index;
	                break;                
	            }
	        }
	    }
	    return genotype;
	}
  
//  static public List<Integer> getAlleles(final int PLindex, final int ploidy) {
//      if ( ploidy == 2 ) { // diploid
//          final GenotypeLikelihoodsAllelePair pair = getAllelePair(PLindex);
//          return Arrays.asList(pair.alleleIndex1, pair.alleleIndex2);
//      } else { // non-diploid
//          if (!anyploidPloidyToPLIndexToAlleleIndices.containsKey(ploidy))
//              throw new IllegalStateException("Must initialize the cache of allele anyploid indices for ploidy " + ploidy);
//
//          if (PLindex < 0 || PLindex >= anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).size()) {
//              final String msg = "The PL index " + PLindex + " does not exist for " + ploidy + " ploidy, " +
//                      (PLindex < 0 ? "cannot have a negative value." : "initialized the cache of allele anyploid indices with the incorrect number of alternate alleles.");
//              throw new IllegalStateException(msg);
//          }
//
//          return anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).get(PLindex);
//      }
//}
	
//	static public int likelihoodGtIndex(int j, int k)
//    {
//    	return (k*(k+1)/2)+j;
//    }
    
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
	
	public TreeSet<String> getAnalysisMethods() {
		return analysisMethods;
	}

	public void setAnalysisMethods(TreeSet<String> analysisMethods) {
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
			throw new Exception("Variant ID: " + getId() + " - " + e.getMessage());
		}
	}
	
	/**
	 * Rebuild vcf format genotype.
	 *
	 * @param alternates the alternates
	 * @param genotypeAlleles the genotype alleles
	 * @param fPhased whether or not the genotype is phased
	 * @param keepCurrentPhasingInfo the keep current phasing info
	 * @return the string
	 * @throws Exception the exception
	 */
	public static String rebuildVcfFormatGenotype(List<String> knownAlleleList, List<String> genotypeAlleles, boolean fPhased, boolean keepCurrentPhasingInfo) throws Exception
	{
		String result = "";
		List<String> orderedGenotypeAlleles = new ArrayList<String>();
		orderedGenotypeAlleles.addAll(genotypeAlleles);
		mainLoop: for (String gtA : orderedGenotypeAlleles)
		{
			String separator = keepCurrentPhasingInfo && fPhased ? "|" : "/";
			for (int i=0; i<knownAlleleList.size(); i++)
			{
				String allele = knownAlleleList.get(i);
				if (allele.equals(gtA))
				{
					result += (result.length() == 0 ? "" : separator) + i;
					continue mainLoop;						
				}
			}
			if (!GT_FIELDVAL_AL_MISSING.equals(gtA))
				throw new Exception("Unable to find allele '" + gtA + "' in alternate list");
		}

		return result;
	}
		
	/**
	 * To variant context.
	 *
	 * @param runs the runs
	 * @param exportVariantIDs the export variant i ds
	 * @param sampleIDToIndividualIdMap global sample ID to individual ID map
	 * @param individuals1 individual IDs for group 1
	 * @param individuals2 individual IDs for group 2
	 * @param previousPhasingIds the previous phasing ids
	 * @param annotationFieldThresholds1 the annotation field thresholds for group 1
	 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
	 * @param warningFileWriter the warning file writer
	 * @param synonym the synonym
	 * @return the variant context
	 * @throws Exception the exception
	 */
	public VariantContext toVariantContext(Collection<VariantRunData> runs, boolean exportVariantIDs, Map<SampleId, String> sampleIDToIndividualIdMap, Collection<String> individuals1, Collection<String> individuals2, HashMap<SampleId, Comparable> previousPhasingIds, HashMap<String, Integer> annotationFieldThresholds1, HashMap<String, Integer> annotationFieldThresholds2, FileWriter warningFileWriter, Comparable synonym) throws Exception
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
			
		individualLoop : for (String individualName : individualList)
		{
			HashMap<String, List<SampleId>> samplesWithGivenGenotype = individualSamplesByGenotype.get(individualName);
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
				warningFileWriter.write("- Dissimilar genotypes found for variant " + (synonym == null ? getId() : synonym) + ", individual " + individualName + ". Exporting most frequent: " + mostFrequentGenotype + "\n");
			
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

			GenotypeBuilder gb = new GenotypeBuilder(individualName, individualAlleles);
			if (individualAlleles.size() > 0)
			{
				gb.phased(isPhased);
				String genotypeFilters = (String) sampleGenotype.getAdditionalInfo().get(FIELD_FILTERS);
				if (genotypeFilters != null && genotypeFilters.length() > 0)
					gb.filter(genotypeFilters);
								
				List<String> alleleListAtImportTimeIfDifferentFromNow = null;
				for (String key : sampleGenotype.getAdditionalInfo().keySet())
				{
					if (!gtPassesVcfAnnotationFilters(individualName, sampleGenotype, individuals1, annotationFieldThresholds1, individuals2, annotationFieldThresholds2))
						continue individualLoop;	// skip genotype

					if (VCFConstants.GENOTYPE_ALLELE_DEPTHS.equals(key))
					{
						String ad = (String) sampleGenotype.getAdditionalInfo().get(key);
						if (ad != null)
						{
							int[] adArray = Helper.csvToIntArray(ad);
							if (knownAlleleList.size() > adArray.length)
							{
								alleleListAtImportTimeIfDifferentFromNow = knownAlleleList.subList(0, adArray.length);
								adArray = VariantData.fixAdFieldValue(adArray, alleleListAtImportTimeIfDifferentFromNow, knownAlleleList);
							}
							gb.AD(adArray);
						}
					}
					else if (VCFConstants.DEPTH_KEY.equals(key) || VCFConstants.GENOTYPE_QUALITY_KEY.equals(key))
					{
						Integer value = (Integer) sampleGenotype.getAdditionalInfo().get(key);
						if (value != null)
						{
							if (VCFConstants.DEPTH_KEY.equals(key))
								gb.DP(value);
							else
								gb.GQ(value);
						}
					}
					else if (VCFConstants.GENOTYPE_PL_KEY.equals(key) || VCFConstants.GENOTYPE_LIKELIHOODS_KEY.equals(key))
					{
						String fieldVal = (String) sampleGenotype.getAdditionalInfo().get(key);
						if (fieldVal != null)
						{
							int[] plArray = VCFConstants.GENOTYPE_PL_KEY.equals(key) ? Helper.csvToIntArray(fieldVal) : GenotypeLikelihoods.fromGLField(fieldVal).getAsPLs();
							if (alleleListAtImportTimeIfDifferentFromNow != null)
								plArray = VariantData.fixPlFieldValue(plArray, individualAlleles.size(), alleleListAtImportTimeIfDifferentFromNow, knownAlleleList);
							gb.PL(plArray);
						}
					}
					else if (!key.equals(VariantData.GT_FIELD_PHASED_GT) && !key.equals(VariantData.GT_FIELD_PHASED_ID) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME)) // exclude some internally created fields that we don't want to export
						gb.attribute(key, sampleGenotype.getAdditionalInfo().get(key)); // looks like we have an extended attribute
				}					
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
	
	// tells whether applied filters imply to treat this genotype as missing data
    public static boolean gtPassesVcfAnnotationFilters(String individualName, SampleGenotype sampleGenotype, Collection<String> individuals1, HashMap<String, Integer> annotationFieldThresholds, Collection<String> individuals2, HashMap<String, Integer> annotationFieldThresholds2)
    {
		List<HashMap<String, Integer>> thresholdsToCheck = new ArrayList<HashMap<String, Integer>>();
		if (individuals1.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds);
		if (individuals2.contains(individualName))
			thresholdsToCheck.add(annotationFieldThresholds2);
		
		for (HashMap<String, Integer> someThresholdsToCheck : thresholdsToCheck)
			for (String annotationField : someThresholdsToCheck.keySet())
			{
				Integer annotationValue = null;
				try
				{
					annotationValue = (Integer) sampleGenotype.getAdditionalInfo().get(annotationField);
				}
				catch (Exception ignored)
				{}
				if (annotationValue != null && annotationValue < someThresholdsToCheck.get(annotationField))
					return false;
			}
		return true;
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