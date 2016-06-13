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
 * See <http://www.gnu.org/licenses/gpl-3.0.html> for details about
 * GNU Affero General Public License V3.
 *******************************************************************************/
package fr.cirad.tools.mongo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import fr.cirad.tools.AppConfig;

/**
 * The Class MongoTemplateManager.
 */
@Component
public class MongoTemplateManager implements ApplicationContextAware
{

	/** The Constant LOG. */
	static private final Logger LOG = Logger.getLogger(MongoTemplateManager.class);

	/** The application context. */
	static private ApplicationContext applicationContext;

	/** The template map. */
	static private Map<String, MongoTemplate> templateMap = new TreeMap<String, MongoTemplate>();

	/** The public databases. */
	static private List<String> publicDatabases = new ArrayList<String>();

	/** The hidden databases. */
	static private List<String> hiddenDatabases = new ArrayList<String>();

	/** The mongo clients. */
	static private Map<String, MongoClient> mongoClients = new HashMap<String, MongoClient>();

/** The resource. */
//	static private Map<String, UserCredentials> mongoCredentials = new HashMap<String, UserCredentials>();
	static private String resource = "datasources";

	/** The expiry prefix. */
	static private String EXPIRY_PREFIX = "_ExpiresOn_";

	/** The temp export prefix. */
	static public String TEMP_EXPORT_PREFIX = "tmpVar_";

	/** The dot replacement string. */
	static private String DOT_REPLACEMENT_STRING = "\\[dot\\]";	/* for VCF header keys */

	/** The app config. */
	@Autowired private AppConfig appConfig;

	/** The resource control. */
	private static Control resourceControl = new ResourceBundle.Control() {
		public boolean needsReload(String baseName, java.util.Locale locale, String format, ClassLoader loader, ResourceBundle bundle, long loadTime)
		{
			return true;
		}

		public long getTimeToLive(String baseName, java.util.Locale locale)
		{
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
		List<String> authorizedCleanupServers = serverCleanupCSV == null ? null : Arrays.asList(serverCleanupCSV.split(","));

		// we do this cleanup here because it only happens when the webapp is truly being (re)started (not when the reload button has been clicked)
		for (String sModule : templateMap.keySet())
		{
			MongoTemplate mongoTemplate = templateMap.get(sModule);
			String connectPoint = mongoTemplate.getDb().getMongo().getConnectPoint();
			if (authorizedCleanupServers == null || authorizedCleanupServers.contains(connectPoint))
				for (String collName : mongoTemplate.getCollectionNames())
					if (collName.startsWith(TEMP_EXPORT_PREFIX))
					{
						mongoTemplate.dropCollection(collName);
						LOG.debug("Dropped collection " + collName + " in module " + sModule);
					}
		}
	}

	/**
	 * Initialize.
	 *
	 * @param ac the ac
	 * @throws BeansException the beans exception
	 */
	static public void initialize(ApplicationContext ac) throws BeansException {
		applicationContext = ac;
		while (applicationContext.getParent() != null)	/* we want the root application-context */
			applicationContext = applicationContext.getParent();

		loadDataSources();
	}

	/**
	 * Load data sources.
	 */
	static private void loadDataSources()
	{
		templateMap.clear();
		try
		{
			ResourceBundle bundle = ResourceBundle.getBundle(resource, resourceControl);
			Map<String, Mongo> mongoHosts = applicationContext.getBeansOfType(Mongo.class);
			for (String sHost : mongoHosts.keySet())
			{
				Mongo host = mongoHosts.get(sHost);
				ServerAddress serverAddress = new ServerAddress(host.getAddress().getHost(), host.getAddress().getPort());
				UserCredentials uc = null;
				try
				{
					uc = applicationContext.getBean(sHost + "Credentials", UserCredentials.class);
				}
				catch (NoSuchBeanDefinitionException nsbde)
				{
					LOG.warn("No user credentials configured for host " + sHost + "! You might want to create a bean UserCredentials named " + sHost + "Credentials");
				}
				MongoClient client = uc != null ? new MongoClient(serverAddress, Arrays.asList(MongoCredential.createCredential(uc.getUsername(), "admin", uc.getPassword().toCharArray()))) : new MongoClient(serverAddress);
				mongoClients.put(sHost, client);
			}
			Enumeration<String> bundleKeys = bundle.getKeys();
			while (bundleKeys.hasMoreElements())
			{
				String key = bundleKeys.nextElement();
				String[] datasourceInfo = bundle.getString(key).split(",");

				if (datasourceInfo.length < 2)
				{
					LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
					continue;
				}

				boolean fHidden = key.endsWith("*"), fPublic = key.startsWith("*");
				String cleanKey = key.replaceAll("\\*", "");

				if (templateMap.containsKey(cleanKey))
				{
					LOG.error("Datasource " + cleanKey + " already exists!");
					continue;
				}

				try
				{
					templateMap.put(cleanKey, createMongoTemplate(cleanKey, datasourceInfo[0], datasourceInfo[1]));
					if (fPublic)
						publicDatabases.add(cleanKey);
					if (fHidden)
						hiddenDatabases.add(cleanKey);
					LOG.info("Datasource " + cleanKey + " loaded as " + (fPublic ? "public" : "private") + " and " + (fHidden ? "hidden" : "exposed"));

					if (datasourceInfo[1].contains(EXPIRY_PREFIX))
					{
						long expiryDate = Long.valueOf((datasourceInfo[1].substring(datasourceInfo[1].lastIndexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length())));
						if (System.currentTimeMillis() > expiryDate)
						{
							try
							{
								removeDataSource(key, true);
								LOG.info("Removed expired datasource entry: " + key);
								continue;
							}
							catch (IOException e)
							{
								LOG.error("Unable to remove expired datasource entry: " + key, e);
							}
							LOG.info("Dropped expired temporary database: " + datasourceInfo[1]);
						}
					}

				}
				catch (Exception e)
				{
					LOG.warn("Unable to create MongoTemplate for module " + cleanKey, e);
				}
			}
		}
		catch (MissingResourceException mre)
		{
			LOG.error("Unable to find file " + resource + ".properties, you may need to adjust your classpath", mre);
		}
	}

	/**
	 * Creates the mongo template.
	 *
	 * @param sModule the module
	 * @param sHost the host
	 * @param sDbName the db name
	 * @return the mongo template
	 * @throws Exception the exception
	 */
	static public MongoTemplate createMongoTemplate(String sModule, String sHost, String sDbName) throws Exception
	{
		MongoClient client = mongoClients.get(sHost);
		if (client == null)
			throw new Exception("Unknown host: " + sHost);

//		UserCredentials uc = mongoCredentials.get(sHost);
//		if (uc != null)
//			client.getCredentialsList().add(MongoCredential.createCredential(uc.getUsername(), "admin", uc.getPassword().toCharArray()));

		SimpleMongoDbFactory factory = new SimpleMongoDbFactory(client, sDbName);
		MongoTemplate mongoTemplate = new MongoTemplate(factory);
		((MappingMongoConverter) mongoTemplate.getConverter()).setMapKeyDotReplacement(DOT_REPLACEMENT_STRING);

		return mongoTemplate;
	}

	/**
	 * Creates the data source.
	 *
	 * @param sModule the module
	 * @param sHost the host
	 * @param expiryDate the expiry date
	 * @throws Exception the exception
	 */
	static public void createDataSource(String sModule, String sHost, Long expiryDate) throws Exception
	{
		String sCleanModule = sModule.replaceAll("\\*", "");
		int nRetries = 0;
		while (nRetries < 100)
		{
			String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
		    String sDbName = "mgdb_" + sCleanModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX  + expiryDate));
		    MongoTemplate mongoTemplate = createMongoTemplate(sCleanModule, sHost, sDbName);
		    if (mongoTemplate.getCollectionNames().size() > 0)
		    	nRetries++;	// DB already exists, let's try with a different DB name
		    else
		    {
		    	templateMap.put(sCleanModule, mongoTemplate);
			    FileWriter fw = new FileWriter(new ClassPathResource("/" + resource + ".properties").getFile().getPath(), true);
			    fw.write("\r\n" + sModule + "=" + sHost + "," + sDbName);
			    fw.close();

				if (sModule.startsWith("*"))
					publicDatabases.add(sCleanModule);
				if (sModule.endsWith("*"))
					hiddenDatabases.add(sCleanModule);
			    return;
		    }
		}
		throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
	}

	/**
	 * Removes the data source.
	 *
	 * @param sModule the module
	 * @param fAlsoDropDatabase whether or not to also drop database
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	static public void removeDataSource(String sModule, boolean fAlsoDropDatabase) throws IOException
	{
		File f = new ClassPathResource("/" + resource + ".properties").getFile();
		Properties properties = new Properties();
		properties.load(new FileReader(f));
		properties.remove(sModule);
		FileOutputStream fos = new FileOutputStream(f);
		properties.store(fos, null);
		fos.close();

		String key = sModule.replaceAll("\\*", "");
		if (fAlsoDropDatabase)
			templateMap.get(key).getDb().dropDatabase();

		templateMap.remove(key);
	}

	/**
	 * Gets the host names.
	 *
	 * @return the host names
	 */
	static public Set<String> getHostNames()
	{
		return mongoClients.keySet();
	}

	/**
	 * Gets the.
	 *
	 * @param module the module
	 * @return the mongo template
	 */
	static public MongoTemplate get(String module)
	{
		return templateMap.get(module);
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
	static public boolean isModulePublic(String sModule)
	{
		return publicDatabases.contains(sModule);
	}

	/**
	 * Checks if is module hidden.
	 *
	 * @param sModule the module
	 * @return true, if is module hidden
	 */
	static public boolean isModuleHidden(String sModule)
	{
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
 * @param clazz the clazz
 * @return the mongo collection name
 */
public static String getMongoCollectionName(Class clazz) {
		Document document = (Document) clazz.getAnnotation(Document.class);
		if (document != null)
			return document.collection();

		return clazz.getSimpleName();
	}
//
//	public static String getFieldKeyName(Class clazz, String sField) {
//		for (java.lang.reflect.Field f : clazz.getFields())
//		{
//			Field field = (Field) f.getAnnotation(Field.class);
//			if (field != null)
//				return field.value();
//		}
//		return sField;
//	}

	/**
 * Close.
 */
@PreDestroy
	static public void close() {
		for (MongoTemplate mongoTemplate : templateMap.values())
			mongoTemplate.getDb().getMongo().close();
	}

}
