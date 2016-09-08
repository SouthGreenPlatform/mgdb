/**
 * *****************************************************************************
 * MGDB - Mongo Genotype DataBase
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
 * *****************************************************************************
 */
package fr.cirad.mgdb.importing;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.Sequence;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

// TODO: Auto-generated Javadoc
/**
 * The Class SequenceImport.
 */
public class SequenceImport {

    /**
     * Download a fasta reference file from an url and unzip it ex :
     * "ftp://ftp.ensembl.org/pub/release-84/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.chromosome.22.fa.gz"
     *
     * @param urlPath
     * @return
     */
    public static File getFastaFileFromURL(String urlPath) {
        File file = null;
        InputStream input;
        FileOutputStream writeFile;

        try {

            URL url = new URL(urlPath);
            URLConnection connection = url.openConnection();

            input = connection.getInputStream();

            String fileName = url.getFile().substring(url.getFile().lastIndexOf('/') + 1);

            // download and decompress if it's a gz file 
            if (fileName.substring(fileName.length() - 2, fileName.length()).equals("gz")) {
                fileName = fileName.substring(0, fileName.length() - 3);

                // OS independant filePath? 
                String basePath = System.getProperty("user.home");
                writeFile = new FileOutputStream(basePath + "/" + fileName);
                // TODO compute checksum on whole file for integrity ? 
                GZIPInputStream gis = new GZIPInputStream(input);

                byte[] buffer = new byte[1024];
                int read;

                while ((read = gis.read(buffer)) > 0) {
                    writeFile.write(buffer, 0, read);
                }
                writeFile.flush();
                writeFile.close();
                input.close();
                gis.close();

                file = new File(basePath + "/" + fileName);
            } else {
                LOG.warn("Unsupported media type: support only .gz file for the moment");
            }

        } catch (Exception ex) {

            LOG.warn("Failed : " + ex);
        } finally {

        }
        return file;
    }

    /**
     * get checksum and sequence length from a fasta file
     *
     * @param file
     * @param headerList
     * @return Map<String, String> with
     * @throws java.io.FileNotFoundException
     * @throws java.security.NoSuchAlgorithmException
     */
    public static Map<String, String> getSeqInfo(File file, List<String> headerList) throws FileNotFoundException, IOException, NoSuchAlgorithmException {

        InputStream is;
        Map<String, String> seqInfo = new LinkedHashMap<>();
        is = new BufferedInputStream(new FileInputStream(file));

        while (getMD5andLength(seqInfo, is, headerList)) {
            // each iteration, a sequence is computed
        }
        is.close();

        return seqInfo;
    }

    /**
     * compute md5 and length for a sequence
     *
     * @param seqInfo
     * @param is
     * @param headerList
     * @return the following char (-1 if end of file or '>' if there is another
     * sequence)
     * @throws java.io.IOException
     * @throws java.security.NoSuchAlgorithmException
     */
    public static boolean getMD5andLength(Map<String, String> seqInfo, InputStream is, List<String> headerList) throws IOException, NoSuchAlgorithmException {

        int spaceCode = (int) '\n';
        int chevronCode = (int) '>';
        DigestInputStream dis = null;
        int c = -1;
        int count = 0;
        int lineLength = 0;
        int localLength = 0;
        int total = 0;
        boolean goOn = false;
        MessageDigest md;
        String header = "";
        try {

            while ((c = is.read()) != spaceCode) {
                // skip the header and do not include it in checksum 
                header += (char) c;
            }
            headerList.add(header);

            // start to compute md5
            md = MessageDigest.getInstance("MD5");
            dis = new md5Digest(is, md);
            while ((c = dis.read()) != spaceCode) {
                // get the 2nd line length 
                lineLength++;
            }
            // count this line 
            count++;

            while ((c = dis.read()) != chevronCode && c != -1) {
                localLength++;
                if (c == spaceCode) {

                    // line is shorter than the other 
                    if (localLength < lineLength) {
                        localLength--; // do not count the '\n' 
                    } else {
                        count++;  // count the lines 
                        localLength = 0;
                    }
                }
            }
            total = lineLength * count + localLength;

            goOn = c != -1; // true if c ='>'

        } catch (IOException | NoSuchAlgorithmException ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            byte[] hash = dis.getMessageDigest().digest();
            seqInfo.put(Helper.bytesToHex(hash), Integer.toString(total));
        }

        return goOn;
    }

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(SequenceImport.class);

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("You must pass 3 parameters as arguments: DATASOURCE name, FASTA reference-file, 3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)!");
        }

        String sModule = args[0];

        GenericXmlApplicationContext ctx = null;
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
        if (mongoTemplate == null) {	// we are probably being invoked offline
            try {
                ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
            } catch (BeanDefinitionStoreException fnfe) {
                LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
                ctx = new GenericXmlApplicationContext("applicationContext.xml");
            }

            MongoTemplateManager.initialize(ctx);
            mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {
                throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }
        }

        String seqCollName = MongoTemplateManager.getMongoCollectionName(Sequence.class);

        if ("2".equals(args[2])) {	// empty project's sequence data before importing
            if (mongoTemplate.collectionExists(seqCollName)) {
                mongoTemplate.dropCollection(seqCollName);
                LOG.info("Collection " + seqCollName + " dropped.");
            }
        } else if ("0".equals(args[2])) {
            // do nothing
        } else {
            throw new Exception("3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)");
        }

        List<String> listHeader = new ArrayList<>();
        Map<String, String> seqInfo;

        String fileName = args[1];
        if (fileName.substring(0, 3).equals("ftp") | fileName.substring(0, 4).equals("http")) {

            // import from an URL 
            File file = SequenceImport.getFastaFileFromURL(fileName);
            seqInfo = SequenceImport.getSeqInfo(file, listHeader);
        } else {
            // import from a local file 
            seqInfo = SequenceImport.getSeqInfo(new File(fileName), listHeader);
        }

        LOG.info("Importing fasta file");
        int rowIndex = 0;

        for (Entry<String, String> entry : seqInfo.entrySet()) {

            String sequenceId = listHeader.get(rowIndex);
            // sequenceId looks like :  "10 dna:chromosome chromosome:GRCh38:10:1:133797422:1 REF"
            // sequence name is 10 in this example
            String name = sequenceId.split("\\s+")[0].replace(">", ""); // split on whitespace and delete '>' 

            String checksum = entry.getKey();
            String length = entry.getValue();
            // do not store the sequence since we don't need it ? 
            mongoTemplate.save(new Sequence(name, "", Long.valueOf(length), checksum, fileName), seqCollName);
            LOG.info(length + " records added to collection " + seqCollName);
            rowIndex++;
            if (rowIndex % 1000 == 0) {
                LOG.info(rowIndex + " ");
            }
        }

        // TODO drop fasta file ? 
        if (ctx != null) {
            ctx.close();
        }
    }

}
