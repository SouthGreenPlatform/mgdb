/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 <South Green>
 *     
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See <http://www.gnu.org/licenses/gpl-3.0.html> for details about
 * GNU General Public License V3.
 *******************************************************************************/
package fr.cirad.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.mongodb.DBObject;

// TODO: Auto-generated Javadoc
/**
 * The Class Helper.
 */
public class Helper 
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(Helper.class);
	
	/** The md. */
	static MessageDigest md = null;
	
	static
	{
		try
		{
			md = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e)
		{
			LOG.error("Unable to find MD5 algorithm", e);
		}
	}
		
	/**
	 * Convert string array to number array.
	 *
	 * @param stringArray the string array
	 * @return the number[]
	 */
	public static Number[] convertStringArrayToNumberArray(String[] stringArray)
	{
		if (stringArray == null || stringArray.length == 0)
			return new Number[0];
	
		Number[] result = new Number[stringArray.length];
		for (int i=0; i<stringArray.length; i++)
			result[i] = Double.parseDouble(stringArray[i]);
		return result;
	}

	/**
	 * Array to csv.
	 *
	 * @param separator the separator
	 * @param array the array
	 * @return the string
	 */
	public static String arrayToCsv(String separator, int[] array)
	{
		if (array == null)
			return null;
		
		StringBuffer result = new StringBuffer("");
		for (int val : array)
			result.append((result.length() == 0 ? "" : separator) + val);
		return result.toString();
	}
	
	/**
	 * Array to csv.
	 *
	 * @param separator the separator
	 * @param array the array
	 * @return the string
	 */
	public static String arrayToCsv(String separator, Collection<?> array)
	{
		if (array == null)
			return null;
		
		StringBuffer result = new StringBuffer("");
		for (Object val : array)
			result.append((result.length() == 0 ? "" : separator) + val);
		return result.toString();
	}
	
	/**
	 * Csv to int array.
	 *
	 * @param csvString the csv string
	 * @return the int[]
	 */
	public static int[] csvToIntArray(String csvString)
	{
		if (csvString == null)
			return new int[0];
		
		String[] splittedString = csvString.split(",");
		int[] result = new int[splittedString.length];
		for (int i=0; i<result.length; i++)
			result[i] = Integer.parseInt(splittedString[i]);
		return result;
	}
	
	/**
	 * Split.
	 *
	 * @param stringToSplit the string to split
	 * @param delimiter the delimiter
	 * @return the list
	 */
	public static List<String> split(String stringToSplit, String delimiter)
	{
        List<String> splittedString = new ArrayList<String>();
		if (stringToSplit != null)
		{
	        int pos = 0, end;
	        while ((end = stringToSplit.indexOf(delimiter, pos)) >= 0) {
	        	splittedString.add(stringToSplit.substring(pos, end));
	            pos = end + delimiter.length();
	        }
	        splittedString.add(stringToSplit.substring(pos));
		}
        return splittedString;
	}
	
	/**
	 * Extract columns from csv.
	 *
	 * @param f the file
	 * @param sSeparator the separator
	 * @param returnedColumnIndexes the returned column indexes
	 * @param nNHeaderLinesToSkip the n n header lines to skip
	 * @return the list
	 * @throws Exception the exception
	 */
	public static List<List<String>> extractColumnsFromCsv(File f, String sSeparator, int[] returnedColumnIndexes, int nNHeaderLinesToSkip) throws Exception
	{
		List<List<String>> result = new ArrayList<List<String>>();
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(f));
			for (int i=0; i<nNHeaderLinesToSkip; i++)
				 br.readLine();	// skip this line
			String sLine = br.readLine();
			if (sLine != null)
				sLine = sLine.trim();
			int nLineCount = 0;
			do
			{
				List<String> splittedLine = split(sLine, sSeparator);
				if (returnedColumnIndexes == null)
					result.add(splittedLine);	// we return the while line
				else
					// we return only some cells in the line
					for (int j=0; j<returnedColumnIndexes.length; j++)
						if (splittedLine.size() < returnedColumnIndexes[j] - 1)
							throw new Exception("Unable to find column n." + returnedColumnIndexes[j] + " in line: " + sLine);
						else
						{
							if (nLineCount == 0)
								result.add(new ArrayList<String>());
							result.get(j).add(splittedLine.get(returnedColumnIndexes[j]));
						}
	
				sLine = br.readLine();
				if (sLine != null)
					sLine = sLine.trim();
				nLineCount++;
			}
			while (sLine != null);
		}
		finally
		{
			if (br != null)
				br.close();
		}
		return result;
	}
	
	/**
	 * Gets the count for key.
	 *
	 * @param keyToCountMap the key to count map
	 * @param sampleId the sample id
	 * @return the count for key
	 */
	public static int getCountForKey(HashMap<Object, Integer> keyToCountMap, Object sampleId)
	{
		Integer gtCount = keyToCountMap.get(sampleId);
		if (gtCount == null)
			gtCount = 0;
		return gtCount;
	}	

	/**
	 * Convert to m d5.
	 *
	 * @param string the string
	 * @return the string
	 */
	public static String convertToMD5(String string)
	{
		if (md == null)
			return string;

        byte[] messageDigest = md.digest(string.getBytes());
        BigInteger number = new BigInteger(1, messageDigest);
        String md5String = number.toString(16);
        // Now we need to zero pad it if you actually want the full 32 chars.
        while (md5String.length() < 32)
        	md5String = "0" + md5String;
        return md5String;
	}
	
	/**
	 * Null to empty string.
	 *
	 * @param s the s
	 * @return the object
	 */
	public static Object nullToEmptyString(Object s)
	{
		return s == null ? "" : s;
	}
	
	/**
	 * Read possibly nested field.
	 *
	 * @param record the record
	 * @param fieldPath the field path
	 * @return the object
	 */
	public static Object readPossiblyNestedField(DBObject record, String fieldPath)
	{
		DBObject slidingRecord = record;
		String[] splitFieldName = fieldPath.split("\\.");
		Object o = null, result;
		for (String s : splitFieldName)
		{
			o = slidingRecord.get(s);
			if (o != null && DBObject.class.isAssignableFrom(o.getClass()))
				slidingRecord = ((DBObject)o);
		}
		if (o != null && List.class.isAssignableFrom(o.getClass()))
		{
			result = new ArrayList<Object>();
			for (Object o2 : ((List) o))
				if (o2 != null && List.class.isAssignableFrom(o2.getClass()))
					((ArrayList<Object>) result).addAll(((List) o2));
				else
					((ArrayList<Object>) result).add(o2);
			result = StringUtils.join(((ArrayList<Object>) result), "; ");
		}
		else
			result = o;						

		if (result == null)
			result = "";
			
		return result;
	}

}
