package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Provider;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;

// It needs @Singleton because otherwise Micronaut @Cacheable won't intercept the cached method call
@Singleton
public class CloudLocationResolver implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(CloudLocationResolver.class));

    private final Provider<LocationServiceClient> locationServiceClient;

    public CloudLocationResolver(@NotNull Provider<LocationServiceClient> locationServiceClient) {
        this.locationServiceClient = locationServiceClient;
    }

    @Override
    @Cacheable(value = "cluster-cache", parameters = "locationId")
    public List<String> resolve(@NotNull String locationId) {
        try {
            return locationServiceClient.get().getClusters(traceId(), locationId)
                .getBody()
                .map(LocationServiceClusterResponse::getClusters)
                .orElseThrow(() -> new LocationServiceException("Unexpected response body, please check location service contract for this endpoint."));

        } catch (final HttpClientResponseException exception) {
            LOG.error("resolve", "Http client response error", exception);
            if (exception.getStatus().getCode() > 499) {
                throw new LocationServiceException("Unexpected error from location service with status - " + exception.getStatus());
            } else {
                throw exception;
            }
        } catch (final Exception exception) {
            LOG.error("resolve", "Unexpected error from location", exception);
            throw exception;
        }
    }

    private String traceId() {
        return MDC.get("trace_id") == null ? UUID.randomUUID().toString() : MDC.get("trace_id");
    }

}
