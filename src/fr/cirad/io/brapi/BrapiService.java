package fr.cirad.io.brapi;

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

public interface BrapiService {
    @GET(value="calls")
    public Call<BrapiListResource<BrapiCall>> getCalls(@Query(value="pageSize") String var1, @Query(value="page") String var2);

    @FormUrlEncoded
    @POST(value="token")
    public Call<BrapiSessionToken> getAuthToken(@Field(value="grant_type") String var1, @Field(value="username") String var2, @Field(value="password") String var3, @Field(value="client_id") String var4);

    @GET(value="studies-search")
    public Call<BrapiListResource<BrapiStudies>> getStudies(@Query(value="studyType") String var1, @Query(value="pageSize") String var2, @Query(value="page") String var3);

    @GET(value="studies/{id}/germplasm")
    public Call<BrapiListResource<BrapiGermplasm>> getStudyGerplasmDetails(@Path(value="id") String var1, @Query(value="pageSize") String var2, @Query(value="page") String var3);

    @GET(value="maps")
    public Call<BrapiListResource<BrapiGenomeMap>> getMaps(@Query(value="species") String var1, @Query(value="pageSize") String var2, @Query(value="page") String var3);

    @GET(value="maps/{id}/positions")
    public Call<BrapiListResource<BrapiMarkerPosition>> getMapMarkerData(@Path(value="id") String var1, @Query(value="linkageGroupName") Collection<String> var2, @Query(value="pageSize") String var3, @Query(value="page") String var4);

    @POST(value="markers-search")
    public Call<BrapiListResource<BrapiMarker>> getMarkerInfo_byPost(@Body Map<String, Object> var1);

    @GET(value="markerprofiles")
    public Call<BrapiListResource<BrapiMarkerProfile>> getMarkerProfiles(@Query(value="studyDbId") String var1, @Query(value="germplasmDbId") Collection<String> var2, @Query(value="pageSize") String var3, @Query(value="page") String var4);

    @GET(value="allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Query(value="markerprofileDbId") List<String> var1, @Query(value="markerDbId") List<String> var2, @Query(value="format") String var3, @Query(value="expandHomozygotes") Boolean var4, @Query(value="unknownString") String var5, @Query(value="sepPhased") String var6, @Query(value="sepUnphased") String var7, @Query(value="pageSize") String var8, @Query(value="page") String var9);

    @POST(value="allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix_byPost(@Body Map<String, Object> var1);

    @GET(value="allelematrix-search/status/{id}")
    public Call<BrapiBaseResource<Object>> getAlleleMatrixStatus(@Path(value="id") String var1);
}