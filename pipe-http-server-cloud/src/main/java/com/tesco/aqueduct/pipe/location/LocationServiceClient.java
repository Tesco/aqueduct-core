package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;

@Client("${location.url}")
@Measure
public interface LocationServiceClient {
    @Get("${location.clusters.get.path}")
    @Consumes
    @CircuitBreaker(attempts = "${location.attempts}", delay = "${location.delay}", reset = "${location.reset}")
    HttpResponse<LocationServiceClusterResponse> getClusters(
        @Header("TraceId") String traceId,
        String locationUuid
    );
}
