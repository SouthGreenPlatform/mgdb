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
package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
//import java.io.FileWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
//import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
//import java.util.Scanner;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class VariantSynonymImport.
 */
public class InitialVariantImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(InitialVariantImport.class);
	
	/** The Constant twoDecimalNF. */
	static private final NumberFormat twoDecimalNF = NumberFormat.getInstance();

	final static private List<String> deprecatedSNPs = Arrays.asList("ARS-BFGL-NGS-89367","ARS-USMARC-Parent-DQ647186-rs29014143","Hapmap58054-rs29014143","Hapmap33892-BES6_Contig314_677","ARS-USMARC-Parent-DQ485413-no-rs","ARS-USMARC-Parent-DQ846689-rs29011985","UA-IFASA-1922","ARS-USMARC-Parent-DQ837646-rs29012894","Hapmap57799-rs29012894","ARS-USMARC-Parent-DQ846691-rs29019814","Hapmap35881-SCAFFOLD20653_10639","ARS-USMARC-Parent-EF042090-no-rs","Hapmap35077-BES9_Contig405_919","ARS-USMARC-Parent-DQ888313-no-rs","Hapmap34041-BES1_Contig298_838","Hapmap40441-BTA-54131","LTF","ARS-USMARC-Parent-DQ990834-rs29013727","BovineHD4000000095","Hapmap53362-rs29013727","ARS-USMARC-Parent-EF026086-rs29013660","Hapmap36071-SCAFFOLD106623_11509","ARS-USMARC-Parent-EF042091-rs29014974","Hapmap36794-SCAFFOLD186736_5402","CHS1","LYST","ARS-USMARC-Parent-AY849380-no-rs","BovineHD4000000009");

	static
	{
		twoDecimalNF.setMaximumFractionDigits(2);
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
//		enrichExistingVariantFile_priorityToIlluminaName();
		
//		generateVariantImportFile();
		
		insertVariantsAndSynonyms(args);
	}

	private static void generateVariantImportFile() throws Exception {
		/* Insert chip list from external file */
		HashMap<String, HashSet<String>> illuminaIdToChipMap = new HashMap<String, HashSet<String>>();
		Scanner sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/SNPchimp_exhaustive_with_rs.tsv"));
		sc.nextLine();
		while (sc.hasNextLine())
		{
			String[] splitLine = sc.nextLine().split("\t");
			HashSet<String> chipsForId = illuminaIdToChipMap.get(splitLine[4]);
			if (chipsForId == null)
			{
				chipsForId = new HashSet<String>();
				illuminaIdToChipMap.put(splitLine[4], chipsForId);
			}
			chipsForId.add(splitLine[0]);
		}
		
		sc.close();
		sc = new Scanner(new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/bos_ExhaustiveSnpList_StandardFormat.tsv"));
		FileWriter fw = new FileWriter("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/NEW_bos_ExhaustiveSnpList_StandardFormat_wChips.tsv");
		fw.write(sc.nextLine() + "\tchip\n");
		while (sc.hasNextLine())
		{
			String sLine = sc.nextLine();
			String[] splitLine = sLine.split("\t");
			HashSet<String> chipsForId = illuminaIdToChipMap.get(splitLine[2].split(";")[0]);
			fw.write(sLine + "\t" + StringUtils.join(chipsForId, ";") + "\n");
		}
		sc.close();
		fw.close();
	}
	
	private static void enrichExistingVariantFile_priorityToIlluminaName() throws Exception {
//		You want to import this output after executing this method, in order to keep the allele ordering you had before
//		db.variants_old.find({}, {"_id":1, "ka":1}).forEach(function(doc) {
//			  db.variants.update({"_id" : doc._id}, {$set : {"ka" : doc.ka}});
//			});		
		
		File newSnpFile = new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/list_SNP_Bovine50K_v3_rs_widde.tsv");
		File existingWiddeSnpFile = new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/DEDUPL_bos_ExhaustiveSnpList_StandardFormat_wChips.tsv");
		String outputWiddeSnpFile = "/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/DEDUPL_bos_ExhaustiveSnpList_StandardFormat_withIlluminaV3.tsv";


		// parse new SNP file and store its contents in HashMaps (we expect each Illumina ID to appear only once, one per line)
		HashMap<String /*chr:n*/, List /*there might be several lines for a same position*/<String[/*chip + rs + Illumina + pos*/]>> providedPosToDetailsMap = new HashMap<>();
		HashMap<String /*Illumina ID*/, String[/*chip + rs + Illumina + pos*/]> providedIlluminaToDetailsMap = new HashMap<>();
		Scanner sc = new Scanner(newSnpFile);
		while (sc.hasNextLine())
		{
			String sLine = sc.nextLine();
			String[] splitLine = sLine.split("\t");
			String pos = splitLine[5];

			providedIlluminaToDetailsMap.put(splitLine[2], new String[] {splitLine[6], splitLine[3], splitLine[2], splitLine[5].endsWith(":0") ? "0:0": splitLine[5]});

			List<String[]> newDetailListForVariant = providedPosToDetailsMap.get(pos);
			if (newDetailListForVariant == null) {
				newDetailListForVariant = new ArrayList<>();
				providedPosToDetailsMap.put(pos, newDetailListForVariant);
			}
			newDetailListForVariant.add(new String[] {splitLine[6], splitLine[3], splitLine[2], splitLine[5].endsWith(":0") ? "0:0": splitLine[5]});
		}
		sc.close();


		// read existing Widde SNP file and re-output each (possibly enriched) line into new Widde SNP file
		sc = new Scanner(existingWiddeSnpFile);
		FileWriter fw = new FileWriter(outputWiddeSnpFile);
		fw.write(sc.nextLine() + "\n");
		String sLine = null;
		int nCommonVariantCount = 0, nCorrectedPositions = 0;
		LinkedHashMap<String, Integer> knownSnpDuplicates = new LinkedHashMap<>(), newSnpDuplicates = new LinkedHashMap<>();
		HashSet<String> duplicateIlluminaIDsWrittenOut = new HashSet<>(), knownDeprecated = new HashSet<>(), newDeprecated = new HashSet<>();
		while (sc.hasNextLine())
		{
			sLine = sc.nextLine();
			String[] splitLine = sLine.split("\t");
//			String pos = splitLine[5];
		
			List<String[]> newDataForVariant = new ArrayList<>();
				
			// first try and find it by Illumina ID
			for (String anIlluminaID : Helper.split(splitLine[2], ";")) {
				String[] illuminaDetails = providedIlluminaToDetailsMap.get(anIlluminaID);
				if (illuminaDetails != null) {	// there is an entry for this Illumina ID in the new provided file
					if (!illuminaDetails[3].endsWith(":0"))	{ // see if we got Illumina synonyms according to positions 
						List<String[]> detailsByPos = providedPosToDetailsMap.get(illuminaDetails[3]);
						if (detailsByPos != null)
							for (String[] aDetailsRow : detailsByPos)
								newDataForVariant.add(aDetailsRow);
					}

					if (newDataForVariant.isEmpty())
						newDataForVariant.add(illuminaDetails);
					break;
				}
			}
			
            if (newDataForVariant.isEmpty() && !splitLine[5].endsWith(":0"))	{ // no match found by Illumina ID, let's try by position if it has one
                newDataForVariant = providedPosToDetailsMap.get(splitLine[5]);
//                if (newDataForVariant != null)
//                        System.out.println("matched by pos: " + pos + " / " + splitLine[2]);
            }
            
//            boolean fIluminaIdAlreadyWrittenOut = false;
            for (String anIlluId : splitLine[2].split(";"))
				if (duplicateIlluminaIDsWrittenOut.contains(anIlluId)) {
//					fIluminaIdAlreadyWrittenOut = true;
					System.err.println("Duplicate: " + anIlluId + " ; make sure it comes out deprecated");
					break;
				}

			if (newDataForVariant != null && newDataForVariant.size() > 0) {
				if (newDataForVariant.size() > 0) {
					nCommonVariantCount++;
					if (newDataForVariant.size() > 1)
						knownSnpDuplicates.put(splitLine[5], newDataForVariant.size());
				}

				for (String[] values : newDataForVariant) {
					providedIlluminaToDetailsMap.remove(values[2]);	// we matched it so we won't append it as a new SNP
					
					if (".".equals(splitLine[2]))
						splitLine[2] = "";
					if (!Helper.split(splitLine[2], ";").contains(values[2]))
						splitLine[2] += (splitLine[2].isEmpty() ? "" : ";") + values[2];	// append Ilumina ID
					if (".".equals(splitLine[3]))
						splitLine[3] = "";
					if (values[1].length() > 0 && !Helper.split(splitLine[3], ";").contains(values[1]))
						splitLine[3] += (splitLine[3].isEmpty() ? "" : ";") + values[1];	// append rs ID
					if (!Helper.split(splitLine[6], ";").contains(values[0]) && !splitLine[6].contains(";" + values[0]))
						splitLine[6] += ";" + values[0];	// append chip
					if (!splitLine[5].equals(values[3])) {
//						System.err.println("Correcting position from " + splitLine[5] + " to " + values[3] + " for " + splitLine[2]);
						nCorrectedPositions++;
						splitLine[5] = values[3];
					}
//					if (newDataForVariant.size() > 1)
						duplicateIlluminaIDsWrittenOut.add(values[2]);
//						System.err.println(values[2]);
				}
				
				sLine = StringUtils.join(splitLine, "\t");
			}
			boolean fDeprecated = CollectionUtils.intersection(deprecatedSNPs, Helper.split(splitLine[2], ";")).size() > 0;
			if (fDeprecated)
				knownDeprecated.add(splitLine[2]);
			fw.write((!sLine.startsWith("*") && fDeprecated ? "*" : "") + sLine + "\n");	// not matched: known SNP is not part of the provided set
		}
		
		String sLastIdInOriginalFile = sLine.split("\t")[0], varIdPrefix = sLastIdInOriginalFile.substring(0, 3);
		int nIdIndex = Integer.parseInt(sLastIdInOriginalFile.substring(3)), nNewSnpCount = 0, nNewUnpositionedSnpCount = 0;


		// append newly encountered SNPs to the end of the file
		HashSet<String> alreadyWrittenOutIlluminaIDs = new HashSet<>();
		mainLoop : for (String[] values : providedIlluminaToDetailsMap.values()) {
			String illuIDs = "", rsIDs = "", chips = "";
			if (!values[3].endsWith(":0"))	{ // see if we got Illumina synonyms according to positions
				List<String[]> detailsByPos = providedPosToDetailsMap.get(values[3]);
				if (detailsByPos != null) {
					if (detailsByPos.size() > 1 && !alreadyWrittenOutIlluminaIDs.contains(detailsByPos.get(0)[2]))
						newSnpDuplicates.put(values[3], detailsByPos.size());

					for (String[] aDetailsRow : detailsByPos) {
						if (alreadyWrittenOutIlluminaIDs.contains(aDetailsRow[2]))
							continue mainLoop;	// already written when we encountered its synonym
						
						if (!Helper.split(illuIDs, ";").contains(aDetailsRow[2])) {
							illuIDs += (illuIDs.isEmpty() ? "" : ";") + aDetailsRow[2];
							alreadyWrittenOutIlluminaIDs.add(aDetailsRow[2]);
						}
						if (!Helper.split(rsIDs, ";").contains(aDetailsRow[1]))
							rsIDs += (rsIDs.isEmpty() ? "" : ";") + aDetailsRow[1];
						if (!Helper.split(chips, ";").contains(aDetailsRow[0]))
							chips += (chips.isEmpty() ? "" : ";") + aDetailsRow[0];
					}
				}
			}
			else {
				illuIDs = values[2];
				rsIDs = values[1];
				chips = values[0];
				nNewUnpositionedSnpCount++;
			}
			
			boolean fDeprecated = CollectionUtils.intersection(deprecatedSNPs, Helper.split(illuIDs, ";")).size() > 0;
			if (fDeprecated) {
				newDeprecated.add(illuIDs);
				System.err.println("Not adding deprecated variant: " + illuIDs);
			}
			else
				fw.write((varIdPrefix + String.format("%09d", ++nNewSnpCount + nIdIndex)) + "\tSNP\t" + illuIDs + "\t" + rsIDs + "\t\t" + values[3] + "\t" + chips + "\n");
		}
		sc.close();
		fw.close();
		
		System.out.println(nCommonVariantCount + " existing variants");
		System.out.println((nNewSnpCount - nNewUnpositionedSnpCount) + " new positioned variants");
		if (nNewUnpositionedSnpCount > 0)
			System.out.println(nNewUnpositionedSnpCount + " new unpositioned variants");
		if (knownSnpDuplicates.size() > 0)
			System.out.println(knownSnpDuplicates.size() + " duplicates for known variants: " + knownSnpDuplicates);
		if (newSnpDuplicates.size() > 0)
			System.out.println(newSnpDuplicates.size() + " duplicates for new variants: " + newSnpDuplicates);
		if (knownDeprecated.size() > 0)
			System.out.println(knownDeprecated.size() + " deprecated known variants: " + knownDeprecated);
		if (newDeprecated.size() > 0)
			System.out.println(newDeprecated.size() + " deprecated new variants were skipped: " + newDeprecated);
		if (nCorrectedPositions > 0)
			System.out.println(nCorrectedPositions + " variant positions corrected");
		
//		Pour chaque synonyme associé à plusieurs variants de la base, affichage du nombre de variants auquel des génotypes sont rattachés (résultat : 1 pour la plupart, parfois 0)
//
//		var cursor = db.variants.aggregate([
//			{$unwind:"$sy.il"},
//			{$group:{_id:"$sy.il", count:{$sum:1}, bta_id:{$addToSet:"$_id"}}},
//			{$match:{"count":{$gt:1}}},
//			{$sort:{"count":-1}}
//		], {allowDiskUse:true});
//
//		while (cursor.hasNext()) {
//			var obj = cursor.next();
////			print("\n" + obj["_id"]);
//			var nVariantsWithGenotypeCount = 0;
//			for (var i=0; i<obj["bta_id"].length; i++) {
//			    var count = db.variantRunData.count({"_id.vi":obj["bta_id"][i]});
//		    	if (count > 0)
//					nVariantsWithGenotypeCount++;
//		  }
//		  print(obj["_id"] + " : " + nVariantsWithGenotypeCount);
//		}
//
//		----------------------------------------------------------
//
//		Affichage de la liste des variants pouvant être éliminés du fait que leur synonyme Illumina est associé à un autre variant ET qu'ils n'ont pas de génotypes associés (si aucun des variants avec un synonyme donné n'a de génotypes associés on garde tout de même le premier de la liste)
//
//		var cursor = db.variants.aggregate([
//			{$unwind:"$sy.il"},
//			{$group:{_id:"$sy.il", count:{$sum:1}, bta_id:{$addToSet:"$_id"}}},
//			{$match:{"count":{$gt:1}}},
//			{$sort:{"count":-1}}
//		], {allowDiskUse:true});
//
//		while (cursor.hasNext()) {
//			var obj = cursor.next();
//			var nVariantsWithGenotypeCount = 0;
//			var unusedVariants = new Array();
//			for (var i=0; i<obj["bta_id"].length; i++) {
//			    var count = db.variantRunData.count({"_id.vi":obj["bta_id"][i]});
//		    	if (count > 0)
//					nVariantsWithGenotypeCount++;
//				else
//					unusedVariants.push(obj["bta_id"][i]);
//		  }
//		  if (nVariantsWithGenotypeCount == 1)
//		  	print(unusedVariants);
//		  else if (nVariantsWithGenotypeCount == 0) {
//		    print(/*unusedVariants + " -> " + */unusedVariants.splice(1));
//		  }
//		}
//
//		RESULTAT :  BTA000779887,BTA000779995,BTA000779888,BTA000779996,BTA000779890,BTA000779998,BTA000779891,BTA000779999,BTA000779892,BTA000780000,BTA000779893,BTA000780001,BTA000779894,BTA000780002,BTA000779895,BTA000780003,BTA000779897,BTA000780005,BTA000779899,BTA000780007,BTA000779901,BTA000780009,BTA000779905,BTA000780013,BTA000779907,BTA000780015,BTA000779909,BTA000780017,BTA000779911,BTA000780020,BTA000779913,BTA000780022,BTA000779917,BTA000780026,BTA000779919,BTA000780028,BTA000779921,BTA000780030,BTA000779923,BTA000780032,BTA000779925,BTA000780034,BTA000779927,BTA000780036,BTA000779929,BTA000780038,BTA000779930,BTA000780039,BTA000779931,BTA000780040,BTA000779932,BTA000780041,BTA000779933,BTA000780042,BTA000779934,BTA000780043,BTA000779935,BTA000780044,BTA000779937,BTA000780046,BTA000779939,BTA000780047,BTA000779941,BTA000780049,BTA000779942,BTA000780050,BTA000779943,BTA000780051,BTA000779944,BTA000780052,BTA000779945,BTA000780053,BTA000779946,BTA000780054,BTA000779948,BTA000780056,BTA000779949,BTA000780057,BTA000779950,BTA000780058,BTA000779951,BTA000780059,BTA000779952,BTA000780060,BTA000779953,BTA000780061,BTA000779954,BTA000780062,BTA000779955,BTA000780063,BTA000779956,BTA000780064,BTA000779957,BTA000780065,BTA000779959,BTA000780067,BTA000779960,BTA000780068,BTA000779961,BTA000780069,BTA000779962,BTA000780070,BTA000779963,BTA000780071,BTA000779964,BTA000780072,BTA000779966,BTA000780074,BTA000779967,BTA000780075,BTA000779968,BTA000780077,BTA000779969,BTA000780078,BTA000779970,BTA000780079,BTA000779971,BTA000780080,BTA000779972,BTA000780081,BTA000779973,BTA000780082,BTA000779974,BTA000780083,BTA000779975,BTA000780084,BTA000779976,BTA000780085,BTA000780131,BTA000780004,BTA000780006,BTA000780133,BTA000780008,BTA000780135,BTA000780137,BTA000780012,BTA000780014,BTA000780139,BTA000780141,BTA000780016,BTA000780019,BTA000780143,BTA000780145,BTA000780021,BTA000780025,BTA000780147,BTA000780027,BTA000780149,BTA000780029,BTA000780151,BTA000780031,BTA000780153,BTA000780033,BTA000780155,BTA000780157,BTA000780035,BTA000780037,BTA000780159,BTA000779977,BTA000780089,BTA000779978,BTA000780090,BTA000779979,BTA000780091,BTA000779980,BTA000780092,BTA000779981,BTA000780093,BTA000779982,BTA000780094,BTA000779983,BTA000780095,BTA000779985,BTA000780097,BTA000779986,BTA000780098,BTA000779987,BTA000780099,BTA000779988,BTA000780100,BTA000779989,BTA000780101,BTA000779990,BTA000780102,BTA000779991,BTA000780104,BTA000779992,BTA000780120,BTA000779993,BTA000780121,BTA000779994,BTA000780122,BTA000779889,BTA000779903,BTA000779915,BTA000779936,BTA000779938,BTA000779940,BTA000779947,BTA000779958,BTA000779965,BTA000780086,BTA000780087,BTA000780088,BTA000780010,BTA000780023,BTA000779984,BTA000780103,BTA000780107,BTA000780108,BTA000780110,BTA000780111,BTA000780113,BTA000780114,BTA000780115,BTA000780117,BTA000780118,BTA000780119
	}
	

	private static void enrichExistingVariantFile_priorityToPosition() throws Exception {
//		You want to import this output after executing this method, in order to keep the allele ordering you had before
//		db.variants_old.find({}, {"_id":1, "ka":1}, {"_id":1, "ka":1}).forEach(function(doc) {
//			  db.variants.update({"_id" : doc._id}, {$set : {"ka" : doc.ka}});
//			});
		
		File newSnpFile = new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/list_SNP_Bovine50K_v3_rs_widde.tsv");
		File existingWiddeSnpFile = new File("/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/DEDUPL_bos_ExhaustiveSnpList_StandardFormat_wChips.tsv");
		String outputWiddeSnpFile = "/media/sempere/Seagate Expansion Drive/CIRH433_backup/D/data/intertryp/WIDDE/cattle/DEDUPL_bos_ExhaustiveSnpList_StandardFormat_withIlluminaV3.tsv";
		
		// parse new SNP file and store its contents in HashMaps
		int nProvidedPositionedVariantCount = 0;
		HashMap<String /*chr:n*/, List /*there might be several lines for a same position*/<String[/*chip + synonyms*/]>> providedPosToDetailsMap = new HashMap<>();
		HashMap<String /*Illumina ID*/, String[/*chip + synonyms*/]> providedUnpositionedDataIlluminaToDetailsMap = new HashMap<>();
		Scanner sc = new Scanner(newSnpFile);
		while (sc.hasNextLine())
		{
			String sLine = sc.nextLine();
			String[] splitLine = sLine.split("\t");
			String pos = splitLine[5];
			if (pos.endsWith(":0"))
				providedUnpositionedDataIlluminaToDetailsMap.put(splitLine[2], new String[] {splitLine[6], splitLine[3], splitLine[2]});
			else {
				List<String[]> newDetailListForVariant = providedPosToDetailsMap.get(pos);
				if (newDetailListForVariant == null) {
					newDetailListForVariant = new ArrayList<>();
					providedPosToDetailsMap.put(pos, newDetailListForVariant);
				}
				newDetailListForVariant.add(new String[] {splitLine[6], splitLine[3], splitLine[2]});
			}
		}
		sc.close();

		// read existing Widde SNP file and re-output each (possibly enriched) line into new Widde SNP file
		sc = new Scanner(existingWiddeSnpFile);
		FileWriter fw = new FileWriter(outputWiddeSnpFile);
		fw.write(sc.nextLine()/* + "\tchip"*/ + "\n");
		String sLine = null;
		int nCommonVariantCount = 0;
		while (sc.hasNextLine())
		{
			sLine = sc.nextLine();
			String[] splitLine = sLine.split("\t");
			String pos = splitLine[5];
			List<String[]> newDataForVariant = providedPosToDetailsMap.get(pos);
			if (newDataForVariant == null) {	// no match found by position, let's try by Illumina ID in case it's on the UNK chromosome
				newDataForVariant = new ArrayList<>();
				for (String anID : Helper.split(splitLine[2], ";"))
					if (providedUnpositionedDataIlluminaToDetailsMap.containsKey(anID)) {
						newDataForVariant.add(providedUnpositionedDataIlluminaToDetailsMap.get(anID));
						providedUnpositionedDataIlluminaToDetailsMap.remove(anID);	// we matched it so we won't append it as a new SNP
						break;
					}
			}

			if (newDataForVariant.size() == 0)
				fw.write(sLine + "\n");
			else {
				if (newDataForVariant.size() > 1)
					System.out.println(pos + " -> " + newDataForVariant.size());	// know position provided several times in new SNP file
				for (String[] values : newDataForVariant) {
					nCommonVariantCount++;
					if (!Helper.split(splitLine[2], ";").contains(values[2]))
						splitLine[2] += ";" + values[2];	// append Ilumina ID
					if (values[1].length() > 0 && !Helper.split(splitLine[3], ";").contains(values[1]))
						splitLine[3] += ";" + values[1];	// append rs ID
					if (!Helper.split(splitLine[6], ";").contains(values[0]) && !splitLine[6].contains(";" + values[0]))
						splitLine[6] += ";" + values[0];	// append chip
				}
				fw.write(StringUtils.join(splitLine, "\t") + "\n");
				providedPosToDetailsMap.remove(pos);	// if we matched it we won't append it as a new SNP
			}
		}
		
		// append newly discovered positioned SNPs to the end of the file
		String sLastIdInOriginalFile = sLine.split("\t")[0], varIdPrefix = sLastIdInOriginalFile.substring(0, 3);
		int nIdIndex = Integer.parseInt(sLastIdInOriginalFile.substring(3));
		for (String pos : providedPosToDetailsMap.keySet()) {
			List<String[]> newDataForVariant = providedPosToDetailsMap.get(pos);
			nProvidedPositionedVariantCount += newDataForVariant.size();
			if (newDataForVariant.size() > 1)
				System.err.println(pos + " -> " + newDataForVariant.size());	// new position provided several times in new SNP file
			for (String[] values : newDataForVariant)
				fw.write((varIdPrefix + String.format("%09d", ++nIdIndex)) + "\tSNP\t" + values[2] + "\t" + values[1] + "\t\t" + pos + "\t" + values[0] + "\n");
		}
		
		// append newly discovered unpositioned SNPs to the end of the file
		for (String[] values : providedUnpositionedDataIlluminaToDetailsMap.values())
			fw.write((varIdPrefix + String.format("%09d", ++nIdIndex)) + "\tSNP\t" + values[2] + "\t" + values[1] + "\t\t0:0\t" + values[0] + "\n");
		sc.close();
		fw.close();
		
		System.out.println(nCommonVariantCount + " existing variants");
		System.out.println(providedUnpositionedDataIlluminaToDetailsMap.size() + " new unpositioned variants");
		System.out.println(nProvidedPositionedVariantCount + " new positioned variants");
	}

	/**
	 * Insert reference positions.
	 *
	 * @param args the args
	 * @throws Exception the exception
	 */
	public static void insertVariantsAndSynonyms(String[] args) throws Exception
	{
		if (args.length < 2)
			throw new Exception("You must pass 2 parameters as arguments: DATASOURCE name, exhaustive variant list TSV file. This TSV file is expected to be formatted as follows: id, chr:pos, colon-separated list of containing chips, zero or more colon-separated lists of synonyms (their type being defined in the header)");

		File chipInfoFile = new File(args[1]);
		if (!chipInfoFile.exists() || chipInfoFile.isDirectory())
			throw new Exception("Data file does not exist: " + chipInfoFile.getAbsolutePath());
		
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(args[0]);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(args[0]);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + args[0] + "' is not supported!");
			}

			if (mongoTemplate.count(null, VariantData.class) > 0)
				throw new Exception("There are already some variants in this database!");
			
			long before = System.currentTimeMillis();

			BufferedReader in = new BufferedReader(new FileReader(chipInfoFile));
			try
			{
				String sLine = in.readLine();	// read header
				if (sLine != null)
					sLine = sLine.trim();
				List<String> header = splitByComaSpaceOrTab(sLine);
				sLine = in.readLine();
				if (sLine != null)
					sLine = sLine.trim();
				
				long count = 0;
				int nNumberOfVariantsToSaveAtOnce = 20000;
				ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
				List<String> fieldsExceptSynonyms = Arrays.asList(new String[] {"id", "type", "pos", "chip"}); 
				do
				{
					if (sLine.length() > 0)
					{
						List<String> cells = Helper.split(sLine, "\t");
						VariantData variant = new VariantData(cells.get(header.indexOf("id")));
						variant.setType(cells.get(header.indexOf("type")));
						String[] seqAndPos = cells.get(header.indexOf("pos")).split(":");
						if (!seqAndPos[0].equals("0"))
							variant.setReferencePosition(new ReferencePosition(seqAndPos[0], Long.parseLong(seqAndPos[1])));
						
						if (!variant.getId().toString().startsWith("*"))	// otherwise it's a deprecated variant that we don't want to appear
						{
							String chipList = cells.get(header.indexOf("chip"));
							if (chipList.length() > 0)
							{
								TreeSet<String> analysisMethods = new TreeSet<String>();
									for (String chip : chipList.split(";"))
										analysisMethods.add(chip);
								variant.setAnalysisMethods(analysisMethods);
							}
						}

						for (int i=0; i<header.size(); i++)
						{
							if (fieldsExceptSynonyms.contains(header.get(i)))
								continue;

							String syns = cells.get(i);
							if (syns.length() > 0)
							{
								TreeSet<Comparable> synSet = new TreeSet<Comparable>();
								for (String syn : syns.split(";"))
									if (!syn.equals("."))
										synSet.add(syn);
								variant.getSynonyms().put(header.get(i), synSet);
							}
						}
						unsavedVariants.add(variant);
												
						if (count % nNumberOfVariantsToSaveAtOnce == 0)
						{
							mongoTemplate.insert(unsavedVariants, VariantData.class);
							unsavedVariants.clear();
							if (count > 0)
							{
								String info = count + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
								LOG.debug(info);
							}
						}
						
						count++;
					}
					sLine = in.readLine();
					if (sLine != null)
						sLine = sLine.trim();
				}
				while (sLine != null);
				
				if (unsavedVariants.size() > 0)
				{
					mongoTemplate.insert(unsavedVariants, VariantData.class);
					unsavedVariants.clear();
				}
				LOG.info("InitialVariantImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
			}
			finally
			{
				in.close();			
			}
		}
		finally
		{
			if (ctx != null)
				ctx.close();
		}
	}

	/**
	 * Split by coma space or tab.
	 *
	 * @param s the s
	 * @return the list
	 */
	private static List<String> splitByComaSpaceOrTab(String s)
	{
		return Helper.split(s, s.contains(",") ? "," : (s.contains(" ") ? " " : "\t"));
	}
}