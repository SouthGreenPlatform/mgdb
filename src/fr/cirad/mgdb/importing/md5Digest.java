/**
 * ***********************************************************************
 * GIGWA - Genotype Investigator for Genome Wide Analyses
 * Copyright (C) 2016 <CIRAD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 * ***********************************************************************
 */
package fr.cirad.mgdb.importing;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

/**
 * extends DigestInputStream and override read() method to avoid computing some
 * char in the checksum
 *
 * @author petel
 */
public class md5Digest extends DigestInputStream {

    private boolean on = true;
    private final int spaceChar = (int) '\n';
    private final int chevChar = (int) '>';

    public md5Digest(InputStream in, MessageDigest md) {
        super(in, md);
    }

    @Override
    public int read() throws IOException {

        int ch = in.read();

        // do not compute '\n' and '>' char in the checksum 
        if (on && ch != -1 && ch != spaceChar && ch != chevChar) {
            digest.update((byte) ch);
        }
        return ch;

    }

}
