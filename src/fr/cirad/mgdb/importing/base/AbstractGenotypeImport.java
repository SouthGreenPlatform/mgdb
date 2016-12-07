package fr.cirad.mgdb.importing.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;

public class AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);
	
	protected static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
	{
		ArrayList<String> result = new ArrayList<String>();
		
		if (sSeq != null && nStartPos != null)
			result.add(sType + "¤" + sSeq + "¤" + nStartPos);
		
		if (idAndSynonyms != null)
			for (String name : idAndSynonyms)
				result.add(name.toUpperCase());
		
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
}
