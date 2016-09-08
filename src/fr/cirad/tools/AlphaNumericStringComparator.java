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
package fr.cirad.tools;

import java.util.Comparator;

// TODO: Auto-generated Javadoc
/**
 * The Class AlphaNumericStringComparator.
 */
public class AlphaNumericStringComparator implements Comparator<String> {

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(String a, String b) {
        char[] aChars = a.toCharArray();
        char[] bChars = b.toCharArray();
        int aIndex = 0;
        int bIndex = 0;
        while (true) {
            if (aIndex >= aChars.length) {
                if (bIndex >= bChars.length) {
                    return 0;
                } else {
                    return -1;
                }
            } else if (bIndex >= bChars.length) {
                return 1;
            }
            char aChar = aChars[aIndex];
            char bChar = bChars[bIndex];
            if (isDigit(aChar) && isDigit(bChar)) {
                int aIndexTo = aIndex + 1;
                while (aIndexTo < aChars.length && isDigit(aChars[aIndexTo])) {
                    aIndexTo++;
                }
                int bIndexTo = bIndex + 1;
                while (bIndexTo < bChars.length && isDigit(bChars[bIndexTo])) {
                    bIndexTo++;
                }
                int aNumber = Integer.parseInt(new String(aChars, aIndex, aIndexTo - aIndex));
                int bNumber = Integer.parseInt(new String(bChars, bIndex, bIndexTo - bIndex));
                if (aNumber < bNumber) {
                    return -1;
                } else if (aNumber > bNumber) {
                    return 1;
                }
                aIndex = aIndexTo;
                bIndex = bIndexTo;
            } else {
                if (aChar < bChar) {
                    return -1;
                } else if (aChar > bChar) {
                    return 1;
                }
                aIndex++;
                bIndex++;
            }
        }
    }

    /**
     * Checks if is digit.
     *
     * @param aChar the a char
     * @return true, if is digit
     */
    private boolean isDigit(char aChar) {
        return aChar >= '0' && aChar <= '9';
    }
}


