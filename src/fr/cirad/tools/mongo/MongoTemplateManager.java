/**
 * *****************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 <CIRAD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
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
package fr.cirad.tools.mongo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;

import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Helper;

/**
 * The Class MongoTemplateManager.
 */
@Component
public class MongoTemplateManager implements ApplicationContextAware {

    /**
     * The Constant LOG.
     */
    static private final Logger LOG = Logger.getLogger(MongoTemplateManager.class);

    /**
     * The application context.
     */
    static private ApplicationContext applicationContext;

    /**
     * The template map.
     */
    static private Map<String, MongoTemplate> templateMap = new TreeMap<>();
    
    /**
     * The species map.
     */
    static private Map<String, String> speciesMap = new TreeMap<>();

    /**
     * The public databases.
     */
    static private Set<String> publicDatabases = new TreeSet<>();

    /**
     * The hidden databases.
     */
    static private List<String> hiddenDatabases = new ArrayList<>();

    /**
     * The mongo clients.
     */
    static private Map<String, MongoClient> mongoClients = new HashMap<>();

    /**
     * The resource.
     */
    static private String resource = "datasources";

    /**
     * The expiry prefix.
     */
    static private String EXPIRY_PREFIX = "_ExpiresOn_";

    /**
     * The temp export prefix.
     */
    static public String TEMP_COLL_PREFIX = "tmpVar_";

    /**
     * The dot replacement string.
     */
    static private String DOT_REPLACEMENT_STRING = "\\[dot\\]";

    /**
     * store ontology terms
     */
    static private Map<String, String> ontologyMap;

    static private int sessionTimeoutInSeconds = 3600;	// defaults to one hour

    /**
     * The app config.
     */
    @Autowired private AppConfig appConfig;

    /**
     * The resource control.
     */
    private static final Control resourceControl = new ResourceBundle.Control() {
        @Override
        public boolean needsReload(String baseName, java.util.Locale locale, String format, ClassLoader loader, ResourceBundle bundle, long loadTime) {
            return true;
        }

        @Override
        public long getTimeToLive(String baseName, java.util.Locale locale) {
            return 0;
        }
    };

    /* (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
    public void setApplicationContext(ApplicationContext ac) throws BeansException {
        initialize(ac);
        String serverCleanupCSV = appConfig.dbServerCleanup();
        try {
            sessionTimeoutInSeconds = appConfig.sessionTimeout();
        } catch (java.lang.NumberFormatException ignored) {
        }	// keep the default value
        List<String> authorizedCleanupServers = serverCleanupCSV == null ? null : Arrays.asList(serverCleanupCSV.split(","));

        // we do this cleanup here because it only happens when the webapp is truly being (re)started (not when the reload button has been clicked)
        for (String sModule : templateMap.keySet()) {

            MongoTemplate mongoTemplate = templateMap.get(sModule);
            String connectPoint = mongoTemplate.getDb().getMongo().getConnectPoint();
            if (authorizedCleanupServers == null || authorizedCleanupServers.contains(connectPoint)) {
                for (String collName : mongoTemplate.getCollectionNames()) {
                    if (collName.startsWith(TEMP_COLL_PREFIX)) {
                        mongoTemplate.dropCollection(collName);
                        LOG.debug("Dropped collection " + collName + " in module " + sModule);
                    }
                }
            }
        }
    }

    public static Map<String, MongoTemplate> getTemplateMap() {
        return templateMap;
    }

    /**
     * Initialize.
     *
     * @param ac the ac
     * @throws BeansException the beans exception
     */
    static public void initialize(ApplicationContext ac) throws BeansException {
        applicationContext = ac;
        while (applicationContext.getParent() != null) /* we want the root application-context */ {
            applicationContext = applicationContext.getParent();
        }

        loadDataSources();
    }
    
    static public void clearExpiredDatabases() {
        try
        {
            ResourceBundle bundle = ResourceBundle.getBundle(resource, resourceControl);
            Enumeration<String> bundleKeys = bundle.getKeys();

            while (bundleKeys.hasMoreElements()) {
				String key = bundleKeys.nextElement();
				String[] datasourceInfo = bundle.getString(key).split(",");
				
				if (datasourceInfo.length < 2) {
				    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
				    continue;
				}
				
				if (datasourceInfo[1].contains(EXPIRY_PREFIX)) {
				    long expiryDate = Long.valueOf((datasourceInfo[1].substring(datasourceInfo[1].lastIndexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length())));
				    if (System.currentTimeMillis() > expiryDate) {
				        if (removeDataSource(key, true))
				        	LOG.info("Removed expired datasource entry: " + key + " and temporary database: " + datasourceInfo[1]);
				    }
				}

            }
        }
        catch (MissingResourceException mre)
        {
            LOG.error("Unable to find file " + resource + ".properties, you may need to adjust your classpath", mre);
        }
    }

    /**
     * Load data sources.
     */
    static public void loadDataSources() {
        templateMap.clear();
        mongoClients.clear();
        publicDatabases.clear();
        hiddenDatabases.clear();
        try {
            ResourceBundle bundle = ResourceBundle.getBundle(resource, resourceControl);
            Map<String, Mongo> mongoHosts = applicationContext.getBeansOfType(Mongo.class);

            for (String sHost : mongoHosts.keySet())
	            try
	            {
	                Mongo host = mongoHosts.get(sHost);
	                ServerAddress serverAddress = new ServerAddress(host.getAddress().getHost(), host.getAddress().getPort());
	                UserCredentials uc = null;
	                try {
	                    uc = applicationContext.getBean(sHost + "Credentials", UserCredentials.class);
	                } catch (NoSuchBeanDefinitionException nsbde) {
	                    LOG.warn("No user credentials configured for host " + sHost + "! You might want to create a bean UserCredentials named " + sHost + "Credentials");
	                }
	                MongoClient client = uc != null ? new MongoClient(serverAddress, Arrays.asList(MongoCredential.createCredential(uc.getUsername(), "admin", uc.getPassword().toCharArray()))) : new MongoClient(serverAddress);
	                mongoClients.put(sHost, client);
	            }
	            catch (MongoTimeoutException mte)
	            {
	                LOG.warn("Unable to connect to host " + sHost, mte);
	            }
            Enumeration<String> bundleKeys = bundle.getKeys();

            while (bundleKeys.hasMoreElements()) {

                String key = bundleKeys.nextElement();
                String[] datasourceInfo = bundle.getString(key).split(",");

                if (datasourceInfo.length < 2) {
                    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
                    continue;
                }

                boolean fHidden = key.endsWith("*"), fPublic = key.startsWith("*");
                String cleanKey = key.replaceAll("\\*", "");
                if (cleanKey.length() == 0)
                {
                	LOG.warn("Skipping unnamed datasource");
                	continue;
                }

                if (templateMap.containsKey(cleanKey)) {
                    LOG.error("Datasource " + cleanKey + " already exists!");
                    continue;
                }

                try
                {
                	if (datasourceInfo.length > 2)
                		speciesMap.put(cleanKey, datasourceInfo[2]);
                    templateMap.put(cleanKey, createMongoTemplate(datasourceInfo[0], datasourceInfo[1]));
                    if (fPublic) {
                        publicDatabases.add(cleanKey);
                    }
                    if (fHidden) {
                        hiddenDatabases.add(cleanKey);
                    }
                    LOG.info("Datasource " + cleanKey + " loaded as " + (fPublic ? "public" : "private") + " and " + (fHidden ? "hidden" : "exposed"));

//                    if (datasourceInfo[1].contains(EXPIRY_PREFIX)) {
//                        long expiryDate = Long.valueOf((datasourceInfo[1].substring(datasourceInfo[1].lastIndexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length())));
//                        if (System.currentTimeMillis() > expiryDate) {
//
//                            removeDataSource(key, true);
//                            LOG.info("Removed expired datasource entry: " + key);
//                            LOG.info("Dropped expired temporary database: " + datasourceInfo[1]);
//                        }
//                    }

                }
                catch (UnknownHostException e)
                {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey + " (no such host)");
                }
                catch (Exception e)
                {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey, e);
                }
            }
        } catch (MissingResourceException mre) {
            LOG.error("Unable to find file " + resource + ".properties, you may need to adjust your classpath", mre);
        }
    }

    /**
     * Creates the mongo template.
     *
     * @param sHost the host
     * @param sDbName the db name
     * @return the mongo template
     * @throws Exception the exception
     */
    static public MongoTemplate createMongoTemplate(String sHost, String sDbName) throws Exception {
        MongoClient client = mongoClients.get(sHost);
        if (client == null) {
            throw new UnknownHostException("Unknown host: " + sHost);
        }

//		UserCredentials uc = mongoCredentials.get(sHost);
//		if (uc != null)
//			client.getCredentialsList().add(MongoCredential.createCredential(uc.getUsername(), "admin", uc.getPassword().toCharArray()));
        SimpleMongoDbFactory factory = new SimpleMongoDbFactory(client, sDbName);
        MongoTemplate mongoTemplate = new MongoTemplate(factory);
        ((MappingMongoConverter) mongoTemplate.getConverter()).setMapKeyDotReplacement(DOT_REPLACEMENT_STRING);
		mongoTemplate.getDb().command(new BasicDBObject("profile", 0));

        return mongoTemplate;
    }

    public enum ModuleAction {
    	CREATE, UPDATE_STATUS, DELETE;
    }
    
    /**
     * Saves or updates a data source.
     *
     * @param action the action to perform on the module
     * @param sModule the module, with a leading * if public and/or a trailing * if hidden
     * @param public flag telling whether or not the module shall be public, ignored for deletion
	 * @param hidden flag telling whether or not the module shall be hidden, ignored for deletion
     * @param sHost the host, only used for creation
     * @param sSpeciesName scientific name of the species, optional, ignored for deletion
     * @param expiryDate the expiry date, only used for creation
     * @throws Exception the exception
     */
    synchronized static public boolean saveOrUpdateDataSource(ModuleAction action, String sModule, boolean fPublic, boolean fHidden, String sHost, String sSpeciesName, Long expiryDate) throws Exception
    {	// as long as we keep all write operations in a single synchronized method, we should be safe
    	if (get(sModule) == null)
    	{
    		if (!action.equals(ModuleAction.CREATE))
    			throw new Exception("Module " + sModule + " does not exist!");
    	}
    	else if (action.equals(ModuleAction.CREATE))
    		throw new Exception("Module " + sModule + " already exists!");
    	
    	FileOutputStream fos = null;
        File f = new ClassPathResource("/" + resource + ".properties").getFile();
    	FileReader fileReader = new FileReader(f);
        Properties properties = new Properties();
        properties.load(fileReader);
        
    	try
    	{
    		if (action.equals(ModuleAction.DELETE))
    		{
    	        String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!properties.containsKey(sModuleKey))
                {
                	LOG.warn("Module could not be found in datasource.properties: " + sModule);
                	return false;
                }
                properties.remove(sModuleKey);
                fos = new FileOutputStream(f);
                properties.store(fos, null);
                return true;
    		}
	        else if (action.equals(ModuleAction.CREATE))
	        {
	            int nRetries = 0;
		        while (nRetries < 100)
		        {
		            String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
		            String sDbName = "mgdb_" + sModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX + expiryDate));
		            MongoTemplate mongoTemplate = createMongoTemplate(sHost, sDbName);
		            if (mongoTemplate.getCollectionNames().size() > 0)
		                nRetries++;	// DB already exists, let's try with a different DB name
		            else
		            {
		                if (properties.containsKey(sModule) || properties.containsKey("*" + sModule) || properties.containsKey(sModule + "*") || properties.containsKey("*" + sModule + "*"))
		                {
		                	LOG.warn("Tried to create a module that already exists in datasource.properties: " + sModule);
		                	return false;
		                }
		                String sModuleKey = (fPublic ? "*" : "") + sModule + (fHidden ? "*" : "");
		                properties.put(sModuleKey, sHost + "," + sDbName + (sSpeciesName == null ? "" : ("," + sSpeciesName)));
		                fos = new FileOutputStream(f);
		                properties.store(fos, null);

		                templateMap.put(sModule, mongoTemplate);
		                if (fPublic)
		                    publicDatabases.add(sModule);
		                if (fHidden)
		                    hiddenDatabases.add(sModule);
		                return true;
		            }
		        }
		        throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
	        }
	        else if (action.equals(ModuleAction.UPDATE_STATUS))
	        {
	        	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!properties.containsKey(sModuleKey))
                {
                	LOG.warn("Tried to update a module that could not be found in datasource.properties: " + sModule);
                	return false;
                }
                String[] propValues = ((String) properties.get(sModuleKey)).split(",");
                properties.remove(sModuleKey);
                properties.put((fPublic ? "*" : "") + sModule + (fHidden ? "*" : ""), propValues[0] + "," + propValues[1] + (sSpeciesName == null ? "" : ("," + sSpeciesName)));
                fos = new FileOutputStream(f);
                properties.store(fos, null);
                
                if (fPublic)
                    publicDatabases.add(sModule);
                else
                	publicDatabases.remove(sModule);
                if (fHidden)
                    hiddenDatabases.add(sModule);
                else
                	hiddenDatabases.remove(sModule);
	        	return true;
	        }
	        else
	        	throw new Exception("Unknown ModuleAction: " + action);
        }
    	catch (IOException ex)
    	{
            LOG.warn("Failed to update datasource.properties for action " + action + " on " + sModule, ex);
            return false;
        }
    	finally
    	{
            try 
            {
           		fileReader.close();
            	if (fos != null)
            		fos.close();
            } 
            catch (IOException ex)
            {
                LOG.debug("Failed to close FileReader", ex);
            }
        }
    }

    /**
     * Removes the data source.
     *
     * @param sModule the module
     * @param fAlsoDropDatabase whether or not to also drop database
     */
    static public boolean removeDataSource(String sModule, boolean fAlsoDropDatabase)
    {
        try
        {
            String key = sModule.replaceAll("\\*", "");
        	saveOrUpdateDataSource(ModuleAction.DELETE, key, false, false, null, null, null);	// only this unique synchronized method may write to file safely

            if (fAlsoDropDatabase)
                templateMap.get(key).getDb().dropDatabase();
            templateMap.remove(key);
            publicDatabases.remove(key);
            hiddenDatabases.remove(key);
            return true;
        }
        catch (Exception ex)
        {
            LOG.warn("Failed to remove " + sModule + " datasource.properties", ex);
            return false;
        }
    }

    /**
     * fill the ontology map
     *
     * @param newOntologyMap
     */
    public static void setOntologyMap(Map<String, String> newOntologyMap) {
        ontologyMap = newOntologyMap;
    }

    /**
     * getter for ontology map
     *
     * @return
     */
    public static Map<String, String> getOntologyMap() {
        return ontologyMap;
    }

    /**
     * Gets the host names.
     *
     * @return the host names
     */
    static public Set<String> getHostNames() {
        return mongoClients.keySet();
    }

    /**
     * Gets the.
     *
     * @param module the module
     * @return the mongo template
     */
    static public MongoTemplate get(String module) {
        return templateMap.get(module);
    }

    /**
     * Gets the public database names.
     *
     * @return the public database names
     */
    static public Collection<String> getPublicDatabases() {
        return publicDatabases;
    }

    static public void dropAllTempColls(String token) {
    	if (token == null)
    		return;
    	
        DBCollection tmpColl;
        String tempCollName = MongoTemplateManager.TEMP_COLL_PREFIX + Helper.convertToMD5(token);
        for (String module : MongoTemplateManager.getTemplateMap().keySet()) {
            // drop all temp collections associated to this token
            tmpColl = templateMap.get(module).getCollection(tempCollName);
            tmpColl.drop();
        }
    }

    /**
     * Gets the available modules.
     *
     * @return the available modules
     */
    static public Collection<String> getAvailableModules() {
        return templateMap.keySet();
    }

    /**
     * Checks if is module public.
     *
     * @param sModule the module
     * @return true, if is module public
     */
    static public boolean isModulePublic(String sModule) {
        return publicDatabases.contains(sModule);
    }

    /**
     * Checks if is module hidden.
     *
     * @param sModule the module
     * @return true, if is module hidden
     */
    static public boolean isModuleHidden(String sModule) {
        return hiddenDatabases.contains(sModule);
    }

//	public void saveRunsIntoProjectRecords()
//	{
//		for (String module : getAvailableModules())
//		{
//			MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
//			for (GenotypingProject proj : mongoTemplate.findAll(GenotypingProject.class))
//				if (proj.getRuns().size() == 0)
//				{
//					boolean fRunAdded = false;
//					for (String run : (List<String>) mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(VariantData.class)).distinct(VariantData.FIELDNAME_PROJECT_DATA + "." + proj.getId() + "." + Run.RUNNAME))
//						if (!proj.getRuns().contains(run))
//						{
//							proj.getRuns().add(run);
//							LOG.info("run " + run + " added to project " + proj.getName() + " in module " + module);
//							fRunAdded = true;
//						}
//					if (fRunAdded)
//						mongoTemplate.save(proj);
//				}
//		}
//	}
    /**
     * Gets the mongo collection name.
     *
     * @param clazz the class
     * @return the mongo collection name
     */
    public static String getMongoCollectionName(Class clazz) {
        Document document = (Document) clazz.getAnnotation(Document.class);
        if (document != null) {
            return document.collection();
        }
        return clazz.getSimpleName();
    }

    /**
     * Close.
     */
    @PreDestroy
    static public void close() {
        for (MongoTemplate mongoTemplate : templateMap.values()) {
            mongoTemplate.getDb().getMongo().close();
        }
    }

	public static String getSpecies(String database) {
		return speciesMap.get(database);
	}

	public static String getModuleHost(String sModule) {
		ResourceBundle bundle = ResourceBundle.getBundle(resource, resourceControl);
		Enumeration<String> bundleKeys = bundle.getKeys();
		while (bundleKeys.hasMoreElements()) {
			String key = bundleKeys.nextElement();

			if (sModule.equals(key.replaceAll("\\*", ""))) {
				String[] datasourceInfo = bundle.getString(key).split(",");
				return datasourceInfo[0];
			}
		}
		return null;
	}
}
