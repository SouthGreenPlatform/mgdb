package fr.cirad.mgdb.importing.base;

import java.util.ArrayList;
import java.util.Collection;

public class AbstractGenotypeImport {
	
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
}
