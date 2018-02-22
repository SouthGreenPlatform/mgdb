// Copyright 2009-2016 Information & Computational Sciences, JHI. All rights
// reserved. Use is subject to the accompanying licence terms.

package fr.cirad.io.brapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;
import jhi.brapi.api.calls.BrapiCall;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfile;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.Buffer;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class BrapiClient
{
	private static final Logger LOG = Logger.getLogger(VariantData.class);

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
//		httpClient = new OkHttpClient.Builder()
//			.readTimeout(60, TimeUnit.SECONDS)
//			.connectTimeout(60, TimeUnit.SECONDS)
//			.build();

		httpClient = getUnsafeOkHttpClient();
		 
		Retrofit retrofit = new Retrofit.Builder()
			.baseUrl(baseURL)
			.addConverterFactory(JacksonConverterFactory.create())
			.client(httpClient)
			.build();

		service = retrofit.create(BrapiService.class);
	}
	
	public static OkHttpClient getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });

            OkHttpClient okHttpClient = builder.addInterceptor(new Interceptor() {
                @Override
                public Response intercept(Chain chain) throws IOException {
                    Request request = chain.request();
                    if (LOG.isEnabledFor(Level.DEBUG)) {
                        LOG.debug(getClass().getName() + ": " + request.method() + " " + request.url());
//                        LOG.debug(getClass().getName() + ": " + request.header("Cookie"));
                        RequestBody rb = request.body();
                        Buffer buffer = new Buffer();
                        if (rb != null)
                            rb.writeTo(buffer);
                        LOG.debug(getClass().getName() + ": " + "Payload- " + buffer.readUtf8());
                    }
                    return chain.proceed(request);
                }
            })
            .readTimeout(60, TimeUnit.SECONDS)
            .connectTimeout(60, TimeUnit.SECONDS)
            .build();
            
            return okHttpClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

	public BrapiService getService() {
		return service;
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