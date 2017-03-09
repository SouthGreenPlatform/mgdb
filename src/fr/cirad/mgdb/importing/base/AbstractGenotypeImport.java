package fr.cirad.mgdb.importing.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.mongo.MongoTemplateManager;

public class AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);
	
	protected static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();
		
		if (idAndSynonyms != null)
			for (String name : idAndSynonyms)
				result.add(name.toUpperCase());

		if (sSeq != null && nStartPos != null)
			result.add(sType + "¤" + sSeq + "¤" + nStartPos);

		if (result.size() == 0)
			throw new Exception("Not enough info provided to build identification strings");
		
		return result;
	}
	
	protected static HashMap<String, Comparable> buildSynonymToIdMapForExistingVariants(MongoTemplate mongoTemplate) throws Exception
	{
        HashMap<String, Comparable> existingVariantIDs = new HashMap<String, Comparable>();
        if (mongoTemplate.count(null, VariantData.class) > 0)
        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
            long beforeReadingAllVariants = System.currentTimeMillis();
            Query query = new Query();
            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
            Iterator<VariantData> variantIterator = mongoTemplate.find(query, VariantData.class).iterator();
			while (variantIterator.hasNext())
			{
				VariantData vd = variantIterator.next();
				ReferencePosition chrPos = vd.getReferencePosition();
//				if (chrPos == null)
//				{	// no position data available
//					variantIterator = null;
//					LOG.warn("No position data available in existing variants");
//					continue;
//				}
				ArrayList<String> idAndSynonyms = new ArrayList<>();
				idAndSynonyms.add(vd.getId().toString());
				for (Collection<Comparable> syns : vd.getSynonyms().values())
					for (Comparable syn : syns)
						idAndSynonyms.add(syn.toString());

				for (String variantDescForPos : getIdentificationStrings(vd.getType(), chrPos == null ? null : chrPos.getSequence(), chrPos == null ? null : chrPos.getStartSite(), idAndSynonyms))
					existingVariantIDs.put(variantDescForPos, vd.getId());
			}
            if (existingVariantIDs.size() > 0)
                LOG.info(mongoTemplate.count(null, VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
        }
        return existingVariantIDs;
	}
	
	public boolean doesDatabaseSupportImportingUnknownVariants(String sModule)
	{
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		long variantCount = mongoTemplate.count(null, VariantData.class);
		String firstId = null, lastId = null;
		Query query = new Query(Criteria.where("_id").not().regex("^\\*.*"));
        query.with(new Sort("_id"));
        query.fields().include("_id");
        VariantData firstVariant = mongoTemplate.findOne(query, VariantData.class);
		if (firstVariant != null)
			firstId = firstVariant.getId().toString();
		query.with(new Sort(Sort.Direction.DESC, "_id"));
		VariantData lastVariant = mongoTemplate.findOne(query, VariantData.class);
		if (lastVariant != null)
			lastId = lastVariant.getId().toString();	

		boolean fLooksLikePreprocessedVariantList = firstId != null && lastId != null && firstId.endsWith("001") && lastId.endsWith("" + variantCount);
		LOG.debug("Database " + sModule + " does " + (fLooksLikePreprocessedVariantList ? "not " : "") + "support importing unknown variants");
		return !fLooksLikePreprocessedVariantList;
	}		
}
