package jhi.flapjack.io.brapi;

import java.util.*;

import jhi.brapi.api.*;
import jhi.brapi.api.authentication.*;
import jhi.brapi.api.calls.*;
import jhi.brapi.api.genomemaps.*;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.markerprofiles.*;
import jhi.brapi.api.markers.BrapiMarker;
import jhi.brapi.api.studies.*;

import retrofit2.Call;
import retrofit2.http.*;

public interface BrapiService
{
	@GET("calls")
	Call<BrapiListResource<BrapiCall>> getCalls(@Query("pageSize") String pageSize, @Query("page") String page);

	@FormUrlEncoded
	@POST("token")
	Call<BrapiSessionToken> getAuthToken(@Field("grant_type") String grantType, @Field("username") String username, @Field("password") String password, @Field("client_id") String clientId);

	@GET("studies-search")
	Call<BrapiListResource<BrapiStudies>> getStudies(@Query("studyType") String studyType, @Query("pageSize") String pageSize, @Query("page") String page);

	@GET("studies/{id}/germplasm")
	Call<BrapiListResource<BrapiGermplasm>> getStudyGerplasmDetails(@Path("id") String studyDbId, @Query("pageSize") String pageSize, @Query("page") String page);
	
	@GET("maps")
	Call<BrapiListResource<BrapiGenomeMap>> getMaps(@Query("species") String species, @Query("pageSize") String pageSize, @Query("page") String page);

	@GET("maps/{id}/positions")
	Call<BrapiListResource<BrapiMarkerPosition>> getMapMarkerData(@Path("id") String id, @Query("pageSize") String pageSize, @Query("page") String page);

	@GET("markers")
	Call<BrapiListResource<BrapiMarker>> getMarkerInfo(@Query("name") Set<String> name, @Query("matchMethod") String matchMethod, @Query("include") String include, @Query("type") String type, @Query("pageSize") String pageSize, @Query("page") String page);

	@FormUrlEncoded
	@POST("markers")
	Call<BrapiListResource<BrapiMarker>> getMarkerInfo_byPost(@Field("name") Set<String> name, @Field("matchMethod") String matchMethod, @Field("include") String include, @Field("type") String type, @Field("pageSize") String pageSize, @Field("page") String page);

	@GET("markerprofiles")
	Call<BrapiListResource<BrapiMarkerProfile>> getMarkerProfiles(@Query("studyDbId") String studyDbId, @Query("germplasm") Collection<String> germplasmDbIDs, @Query("pageSize") String pageSize, @Query("page") String page);

//	@FormUrlEncoded
//	@POST(value="allelematrix-search")
//	Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrixById(@Field(value="matrixDbId") String matrixDbId, @Field("format") String format, @Field("expandHomozygotes") Boolean expandHomozygotes, @Field("unknownString") String unknownString, @Field("sepPhased") String sepPhased, @Field("sepUnphased") String sepUnphased, @Field("pageSize") Integer pageSize, @Field(value="page") Integer page);
	
	@GET("allelematrix-search")
	Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Query("markerprofileDbId") List<String> markerProfileDbIds, @Query("markerDbId") List<String> markerDbIds, @Query("format") String format, @Query("expandHomozygotes") Boolean expandHomozygotes, @Query("unknownString") String unknownString, @Query("sepPhased") String sepPhased, @Query("sepUnphased") String sepUnphased, @Query("pageSize") String pageSize, @Query("page") String page);

	@FormUrlEncoded
	@POST("allelematrix-search")
	Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix_byPost(@Field("markerprofileDbId") List<String> markerProfileDbIds, 	@Field("markerDbId") List<String> markerDbIds,	@Field("format") String format, @Field("expandHomozygotes") Boolean expandHomozygotes, @Field("unknownString") String unknownString, @Field("sepPhased") String sepPhased, @Field("sepUnphased") String sepUnphased, @Field("pageSize") String pageSize, @Field("page") String page);
	
	@GET("allelematrix-search/status/{id}")
	Call<BrapiBaseResource<Object>> getAlleleMatrixStatus(@Path("id") String extractId);
}