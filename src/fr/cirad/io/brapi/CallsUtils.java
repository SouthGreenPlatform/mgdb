// Copyright 2009-2016 Information & Computational Sciences, JHI. All rights
// reserved. Use is subject to the accompanying licence terms.

package fr.cirad.io.brapi;

import java.util.*;

import jhi.brapi.api.calls.*;

public class CallsUtils
{
	public static final String GET = "GET";
	public static final String POST = "POST";
	public static final String JSON = "json";
	public static final String TSV = "tsv";

	private List<BrapiCall> calls;

	public CallsUtils(List<BrapiCall> calls)
	{
		this.calls = calls;
	}

	boolean validate()
	{
		// First validate the calls that MUST be present
		if (hasCall("studies-search", JSON, GET) == false)
			return false;
		if (hasCall("maps", JSON, GET) == false)
			return false;
		if (hasCall("maps/{mapDbId}/positions", JSON, GET) == false)
			return false;
		if (hasCall("markerprofiles", JSON, GET) == false)
			return false;
		if (hasCall("allelematrix-search", JSON, POST) == false && hasCall("allelematrix-search", TSV, POST) == false)
			return false;

		return true;
	}

	public boolean hasCall(String signature, String datatype, String method)
	{
		for (BrapiCall call : calls)
			if (call.getCall().equals(signature) && call.getDatatypes().contains(datatype) && call.getMethods().contains(method))
				return true;

		return false;
	}

	boolean hasToken()
	{
		return hasCall("token", JSON, GET);
	}

	boolean hasPostMarkersSearch()
	{
		return hasCall("markers-search", JSON, POST);
	}

	boolean hasGetMarkersSearch()
	{
		return hasCall("markers-search", JSON, GET);
	}
	
	boolean hasMarkersDetails()
	{
		/* FIXME: this should be "markers/{id}", now as such to remain compatible with https://ics.hutton.ac.uk/germinate-demo/cactuar-devel/brapi/v1 */
		return hasCall("markers", JSON, GET);
	}

//	boolean hasMapsMapDbId()
//	{
//		return hasCall("maps/id", JSON, GET);
//	}

	boolean hasAlleleMatrixSearchTSV()
	{
		return hasCall("allelematrix-search", TSV, POST);
	}
}