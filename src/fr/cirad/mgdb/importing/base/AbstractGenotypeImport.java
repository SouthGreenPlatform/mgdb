package fr.cirad.mgdb.importing.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

public class AbstractGenotypeImport {
	
	private static final Logger LOG = Logger.getLogger(AbstractGenotypeImport.class);

	private static final String COLLECTION_NAME_SYNONYM_MAPPINGS = "synonymMappings";
	
	/** String representing nucleotides considered as valid */
	protected static HashSet<String> validNucleotides = new HashSet<>(Arrays.asList(new String[] {"a", "A", "t", "T", "g", "G", "c", "C"}));
	
	public static ArrayList<String> getIdentificationStrings(String sType, String sSeq, Long nStartPos, Collection<String> idAndSynonyms) throws Exception
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

//	public static void buildSynonymMappings(MongoTemplate mongoTemplate) throws Exception
//	{
//		DBCollection collection = mongoTemplate.getCollection(COLLECTION_NAME_SYNONYM_MAPPINGS);
//		collection.drop();
//
//		long variantCount = mongoTemplate.count(null, VariantData.class);
//        if (variantCount > 0)
//        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
//            long beforeReadingAllVariants = System.currentTimeMillis();
//            Query query = new Query();
//            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
//            DBCursor variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject(), query.getFieldsObject());
//            int i = 0, nDups = 0;
//			while (variantIterator.hasNext())
//			{
//				DBObject vd = variantIterator.next();
//				boolean fGotChrPos = vd.get(VariantData.FIELDNAME_REFERENCE_POSITION) != null;
//				ArrayList<String> idAndSynonyms = new ArrayList<>();
//				idAndSynonyms.add(vd.get("_id").toString());
//				DBObject synonymsByType = (DBObject) vd.get(VariantData.FIELDNAME_SYNONYMS);
//				for (String synonymType : synonymsByType.keySet())
//					for (Object syn : (BasicDBList) synonymsByType.get(synonymType))
//						idAndSynonyms.add((String) syn.toString());
//
//				for (String variantDescForPos : getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), !fGotChrPos ? null : (String) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE), !fGotChrPos ? null : (long) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE), idAndSynonyms))
//				{
//					BasicDBObject synonymMapping = new BasicDBObject("_id", variantDescForPos);
//					synonymMapping.put("id", vd.get("_id"));
//					try
//					{
//						collection.insert(synonymMapping);
//					}
//					catch (DuplicateKeyException dke)
//					{
//						nDups++;
//						LOG.error(dke.getMessage());
//					}
//				}
//				
//				float nProgressPercentage = ++i * 100f / variantCount;
//				if (nProgressPercentage % 10 == 0)
//					LOG.debug("buildSynonymMappings: " + nProgressPercentage + "%");
//			}
//            LOG.info(mongoTemplate.count(null, VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
//            if (nDups > 0)
//            	LOG.warn(nDups + " duplicates found in database " + mongoTemplate.getDb().getName());
//        }
//	}
	
	protected static HashMap<String, Comparable> buildSynonymToIdMapForExistingVariants(MongoTemplate mongoTemplate, boolean fIncludeRandomObjectIDs) throws Exception
	{
        HashMap<String, Comparable> existingVariantIDs = new HashMap<String, Comparable>();
		long variantCount = mongoTemplate.count(null, VariantData.class);
        if (variantCount > 0)
        {	// there are already variants in the database: build a list of all existing variants, finding them by ID is by far most efficient
            long beforeReadingAllVariants = System.currentTimeMillis();
            Query query = new Query();
            query.fields().include("_id").include(VariantData.FIELDNAME_REFERENCE_POSITION).include(VariantData.FIELDNAME_TYPE).include(VariantData.FIELDNAME_SYNONYMS);
            DBCursor variantIterator = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(query.getQueryObject(), query.getFieldsObject());
			while (variantIterator.hasNext())
			{
				DBObject vd = variantIterator.next();
				String variantIdAsString = vd.get("_id").toString();
				boolean fGotChrPos = vd.get(VariantData.FIELDNAME_REFERENCE_POSITION) != null;
				ArrayList<String> idAndSynonyms = new ArrayList<>();
				if (fIncludeRandomObjectIDs || !ObjectId.isValid(variantIdAsString))	// most of the time we avoid taking into account randomly generated IDs
					idAndSynonyms.add(variantIdAsString);
				DBObject synonymsByType = (DBObject) vd.get(VariantData.FIELDNAME_SYNONYMS);
				for (String synonymType : synonymsByType.keySet())
					for (Object syn : (BasicDBList) synonymsByType.get(synonymType))
						idAndSynonyms.add((String) syn.toString());

				for (String variantDescForPos : getIdentificationStrings((String) vd.get(VariantData.FIELDNAME_TYPE), !fGotChrPos ? null : (String) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE), !fGotChrPos ? null : (long) Helper.readPossiblyNestedField(vd, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE), idAndSynonyms))
					existingVariantIDs.put(variantDescForPos, (Comparable) vd.get("_id"));
			}
            LOG.info(mongoTemplate.count(null, VariantData.class) + " VariantData record IDs were scanned in " + (System.currentTimeMillis() - beforeReadingAllVariants) / 1000 + "s");
        }
        return existingVariantIDs;
	}
	
	public boolean doesDatabaseSupportImportingUnknownVariants(String sModule)
	{
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
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

		boolean fLooksLikePreprocessedVariantList = firstId != null && firstId.endsWith("001") && mongoTemplate.count(new Query(Criteria.where("_id").not().regex("^\\*?" + StringUtils.getCommonPrefix(new String[] {firstId, lastId}) + ".*")), VariantData.class) == 0;
		LOG.debug("Database " + sModule + " does " + (fLooksLikePreprocessedVariantList ? "not " : "") + "support importing unknown variants");
		return !fLooksLikePreprocessedVariantList;
	}		
}
