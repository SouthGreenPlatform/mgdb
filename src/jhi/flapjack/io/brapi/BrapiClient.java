// Copyright 2009-2016 Information & Computational Sciences, JHI. All rights
// reserved. Use is subject to the accompanying licence terms.

package jhi.flapjack.io.brapi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;
import jhi.brapi.api.authentication.BrapiSessionToken;
import jhi.brapi.api.calls.BrapiCall;
import jhi.brapi.api.genomemaps.BrapiGenomeMap;
import jhi.brapi.api.genomemaps.BrapiMapMetaData;
import jhi.brapi.api.genomemaps.BrapiMarkerPosition;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfile;
import jhi.brapi.api.studies.BrapiStudies;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class BrapiClient
{
	private BrapiService service;

	private String username, password;
	private String mapID, studyID, methodID;
	private Collection<String> germplasmDbIDs;

	private CallsUtils callsUtils;
	private OkHttpClient httpClient;

	public void initService(String baseURL)
		throws Exception
	{
		baseURL = baseURL.endsWith("/") ? baseURL : baseURL + "/";

		// Tweak to make the timeout on Retrofit connections last longer
		httpClient = new OkHttpClient.Builder()
			.readTimeout(60, TimeUnit.SECONDS)
			.connectTimeout(60, TimeUnit.SECONDS)
			.build();

		Retrofit retrofit = new Retrofit.Builder()
			.baseUrl(baseURL)
			.addConverterFactory(JacksonConverterFactory.create())
			.client(httpClient)
			.build();

		service = retrofit.create(BrapiService.class);
	}

	private String enc(String str)
	{
		try { return URLEncoder.encode(str, "UTF-8"); }
		catch (UnsupportedEncodingException e) { return str; }
	}

	public void getCalls()
		throws Exception
	{
		List<BrapiCall> calls = new ArrayList<>();
		Pager pager = new Pager();

		while (pager.isPaging())
		{
			BrapiListResource<BrapiCall> br = service.getCalls(pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			calls.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		callsUtils = new CallsUtils(calls);

		if (callsUtils.validate() == false)
			throw new Exception("/calls not valid");
	}

	public boolean hasToken()
		{ return callsUtils.hasToken(); }

	public boolean hasAlleleMatrixSearchTSV()
		{ return callsUtils.hasAlleleMatrixSearchTSV(); }

	public boolean hasMapsMapDbId()
		{ return callsUtils.hasMapsMapDbId(); }

	public boolean doAuthentication()
		throws Exception
	{
		if (true)
			return false;

		if (username == null && password == null)
			return false;

		BrapiSessionToken token = service.getAuthToken("password", enc(username), enc(password), "flapjack")
			.execute()
			.body();

//		String params = "grant_type=password&username=" + enc(username)
//			+ "&password=" + enc(password) + "&client_id=flapjack";
//		Form form = new Form(params);
//
//		BrapiSessionToken token = cr.post(form.getWebRepresentation(), BrapiSessionToken.class);
//
//		// Add the token information to all further calls
//		ChallengeResponse challenge = new ChallengeResponse(ChallengeScheme.HTTP_OAUTH_BEARER);
//		challenge.setRawValue(token.getSessionToken());
//		cr.setChallengeResponse(challenge);
		return false;
	}

	public BrapiService getService() {
		return service;
	}

	// Returns a list of available maps
	public List<BrapiGenomeMap> getMaps()
		throws Exception
	{
		List<BrapiGenomeMap> list = new ArrayList<>();
		Pager pager = new Pager();

		while (pager.isPaging())
		{
			BrapiListResource<BrapiGenomeMap> br = service.getMaps(null, pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			list.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		return list;
	}
	
//	// Returns the details (type, synonyms...) for a given marker
//	public List<BrapiMarker> getMarkerInfo()
//		throws Exception
//	{
//		List<BrapiMarker> list = new ArrayList<>();
//		Pager pager = new Pager();
//
//		while (pager.isPaging())
//		{
//			BrapiListResource<BrapiMarkerPosition> br = service.getMarkerInfo(enc(mapID), pager.getPageSize(), pager.getPage())
//				.execute()
//				.body();
//
//			list.addAll(br.data());
//
//			pager.paginate(br.getMetadata());
//		}
//
//		return list;
//	}

	// Returns the details (markers, chromosomes, positions) for a given map
	public List<BrapiMarkerPosition> getMapMarkerData()
		throws Exception
	{
		List<BrapiMarkerPosition> list = new ArrayList<>();
		Pager pager = new Pager();

		while (pager.isPaging())
		{
			BrapiListResource<BrapiMarkerPosition> br = service.getMapMarkerData(enc(mapID), pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			list.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		return list;
	}

//	public BrapiMapMetaData getMapMetaData()
//		throws Exception
//	{
//		BrapiBaseResource<BrapiMapMetaData> br = service.getMapMetaData(enc(mapID))
//			.execute()
//			.body();
//
//		return br.getResult();
//	}

	// Returns a list of available studies
	public List<BrapiStudies> getStudies()
		throws Exception
	{
		List<BrapiStudies> list = new ArrayList<>();
		Pager pager = new Pager();
//		pager.setPageSize("10");

		while (pager.isPaging())
		{
			BrapiListResource<BrapiStudies> br = service.getStudies("genotype", pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			list.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		return list;
	}
	

	public List<BrapiGermplasm> getStudyGerplasmDetails() throws IOException {
		List<BrapiGermplasm> list = new ArrayList<>();
		Pager pager = new Pager();

		while (pager.isPaging())
		{
			BrapiListResource<BrapiGermplasm> br = service.getStudyGerplasmDetails(studyID, pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			list.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		return list;
	}

	public List<BrapiMarkerProfile> getMarkerProfiles()
		throws Exception
	{
		List<BrapiMarkerProfile> list = new ArrayList<>();
		Pager pager = new Pager();

		while (pager.isPaging())
		{
			BrapiListResource<BrapiMarkerProfile> br = service.getMarkerProfiles(studyID, germplasmDbIDs, pager.getPageSize(), pager.getPage())
				.execute()
				.body();

			list.addAll(br.data());

			pager.paginate(br.getMetadata());
		}

		return list;
	}


//	public List<BrapiAlleleMatrix> getAlleleMatrix(List<BrapiMarkerProfile> markerprofiles, List<BrapiMarker> markers)
//		throws Exception
//	{
//		List<BrapiAlleleMatrix> list = new ArrayList<>();
//		Pager pager = new Pager();
//		pager.setPageSize("1000");
//
//		List<String> markerProfileIDs = markerprofiles.stream().map(BrapiMarkerProfile::getMarkerProfileDbId).collect(Collectors.toList());
//		List<String> markerIDs = markers == null ? null : markers.stream().map(BrapiMarker::getMarkerDbId).collect(Collectors.toList());
//
//		while (pager.isPaging())
//		{
//			BrapiBaseResource<BrapiAlleleMatrix> br = service.getAlleleMatrix(markerProfileIDs, markerIDs, null, pager.getPageSize(), pager.getPage())
//				.execute()
//				.body();
//
//			ArrayList<BrapiAlleleMatrix> temp = new ArrayList<>();
//			temp.add(br.getResult());
//			list.addAll(temp);
//
//			pager.paginate(br.getMetadata());
//		}
//
//		return list;
//	}

//	public URI getAlleleMatrixTSV(List<BrapiMarkerProfile> markerprofiles, List<BrapiMarker> markers)
//		throws Exception
//	{
//		List<String> markerProfileIDs = markerprofiles.stream().map(BrapiMarkerProfile::getMarkerProfileDbId).collect(Collectors.toList());
//		List<String> markerIDs = markers == null ? null : markers.stream().map(BrapiMarker::getMarkerDbId).collect(Collectors.toList());
//
//		BrapiBaseResource<BrapiAlleleMatrix> br = service.getAlleleMatrix(markerProfileIDs, markerIDs, "tsv", null, null)
//			.execute()
//			.body();
//
//		Metadata md = br.getMetadata();
//		List<String> files = md.getDatafiles();
//
//		return new URI(files.get(0));
//	}

	public XmlBrapiProvider getBrapiProviders()
		throws Exception, IOException
	{
		URL url = new URL("https://ics.hutton.ac.uk/resources/brapi/brapi.zip");

		File dir = new File("FlapjackUtils.getCacheDir()", "brapi");
		dir.mkdirs();

		// Download the zip file and extract its contents into a temp folder
		ZipInputStream zis = new ZipInputStream(new BufferedInputStream(url.openStream()));
		ZipEntry ze = zis.getNextEntry();

    	while (ze != null)
		{
			BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(new File(dir, ze.getName())));
			BufferedInputStream in = new BufferedInputStream(zis);

			byte[] b = new byte[4096];
			for (int n; (n = in.read(b)) != -1;)
				out.write(b, 0, n);

			out.close();
			ze = zis.getNextEntry();
		}
		zis.closeEntry();
		zis.close();


		// Now read the contents of the XML file
		JAXBContext jaxbContext = JAXBContext.newInstance(XmlBrapiProvider.class);
		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		File xml = new File(dir, "brapi.xml");

		return (XmlBrapiProvider) jaxbUnmarshaller.unmarshal(xml);
	}
	
	// Use the okhttp client we configured our retrofit service with. This means
	// the client is configured with any authentication tokens and any custom
	// certificates that may be required to interact with the current BrAPI
	// resource
	public InputStream getInputStream(URI uri)
		throws Exception
	{
		Request request = new Request.Builder()
			.url(uri.toURL())
			.build();

		Response response = httpClient.newCall(request).execute();

		return response.body().byteStream();
	}

	public String getUsername()
	{ return username; }

	public void setUsername(String username)
	{ this.username = username; }

	public String getPassword()
	{ return password; }

	public void setPassword(String password)
	{ this.password = password; }

	public String getMethodID()
	{ return methodID; }

	public void setMethodID(String methodID)
	{ this.methodID = methodID;	}

//	public XmlResource getResource()
//	{ return resource; }
//
//	public void setResource(XmlResource resource)
//	{ this.resource = resource; }

	public String getMapID()
	{ return mapID; }

	public void setMapID(String mapIndex)
	{ this.mapID = mapIndex; }

	public String getStudyID()
	{ return studyID; }

	public void setStudyID(String studyID)
	{ this.studyID = studyID; }
	
	public void setGermplasmDbIDs(Collection<String> germplasmDbIDs)
	{ this.germplasmDbIDs = germplasmDbIDs; }

//	private static void initCertificates(Client client, XmlResource resource)
//		throws Exception
//	{
//		if (resource.getCertificate() == null)
//			return;
//
//		// Download the "trusted" certificate needed for this resource
//		URLConnection yc = new URL(resource.getCertificate()).openConnection();
//
//		CertificateFactory cf = CertificateFactory.getInstance("X.509");
//		InputStream in = new BufferedInputStream(yc.getInputStream());
//		java.security.cert.Certificate cer;
//		try {
//			cer = cf.generateCertificate(in);
//		} finally { in.close();	}
//
//		// Create a KeyStore to hold the certificate
//		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
//		keyStore.load(null, null);
//		keyStore.setCertificateEntry("cer", cer);
//
//		// Create a TrustManager that trusts the certificate in the KeyStore
//		String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
//		TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
//		tmf.init(keyStore);
//
//		// Create an SSLContext that uses the TrustManager
//		SSLContext sslContext = SSLContext.getInstance("TLS");
//		sslContext.init(null, tmf.getTrustManagers(), null);
//
//		// Then *finally*, apply the TrustManager info to Restlet
//		client.setContext(new Context());
//		Context context = client.getContext();
//
//		context.getAttributes().put("sslContextFactory", new SslContextFactory() {
//		    public void init(Series<Parameter> parameters) { }
//		   	public SSLContext createSslContext() throws Exception { return sslContext; }
//		});
//	}

	public static class Pager
	{
		private boolean isPaging = true;
		private String pageSize = "100000";
		private String page = "0";

		// Returns true if another 'page' of data should be requested
		public void paginate(Metadata metadata)
		{
			Pagination p = metadata.getPagination();

			if (p.getTotalPages() == 0)
				isPaging = false;

			if (p.getCurrentPage() == p.getTotalPages()-1)
				isPaging = false;

			// If it's ok to request another page, update the URL (for the next call)
			// so that it does so
			pageSize = "" + p.getPageSize();
			page = "" + (p.getCurrentPage()+1);
		}

		public boolean isPaging()
		{ return isPaging; }

		public void setPaging(boolean paging)
		{ isPaging = paging; }

		public String getPageSize()
		{ return pageSize; }

		public void setPageSize(String pageSize)
		{ this.pageSize = pageSize; }

		public String getPage()
		{ return page; }

		public void setPage(String page)
		{ this.page = page; }
	}

}