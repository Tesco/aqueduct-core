package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.LocationService;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenClient;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider;
import com.tesco.aqueduct.pipe.location.CloudLocationService;
import com.tesco.aqueduct.pipe.location.LocationServiceClient;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.pipe.storage.ClusterStorage;
import com.tesco.aqueduct.pipe.storage.LocationResolver;
import com.tesco.aqueduct.pipe.storage.OffsetFetcher;
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.registry.model.NodeRequestStorage;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRequestStorage;
import io.jaegertracing.Configuration;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import io.opentracing.Tracer;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.net.URL;
import java.time.Duration;

@Factory
@Singleton
public class Bindings {

    // This provides Reader as it implements it
    @Singleton @Named("local")
    PostgresqlStorage bindPostgreSQL(
        @Property(name = "persistence.read.limit") final int limit,
        @Property(name = "persistence.read.retry-after") final int retryAfter,
        @Property(name = "persistence.read.max-batch-size") final int maxBatchSize,
        @Value("${persistence.read.expected-node-count}") final int expectedNodeCount,
        @Value("${persistence.read.cluster-db-pool-size}") final long clusterDBPoolSize,
        @Value("${persistence.read.work-mem-mb:4}") final int workMemMb,
        @Named("pipe") final DataSource dataSource,
        final OffsetFetcher offsetFetcher,
        LocationResolver locationResolver
    ) {
        return new PostgresqlStorage(
            dataSource, limit, retryAfter, maxBatchSize, offsetFetcher, expectedNodeCount, clusterDBPoolSize, workMemMb, locationResolver
        );
    }

    @Singleton
    LocationResolver locationResolver(
        @Named("pipe") final DataSource dataSource,
        @Value("${location.clusters.cache.expire-after-write}") final Duration expireAfter,
        final LocationService locationService
    ) {
        return new ClusterStorage(dataSource, locationService, expireAfter);
    }

    @Singleton
    @Measure
    NodeRegistry bindNodeRegistry(
        @Named("registry") final DataSource dataSource,
        @Property(name = "pipe.server.url") final URL selfUrl,
        @Value("${registry.mark-offline-after:1m}") final Duration markAsOffline
    ) {
        return new PostgreSQLNodeRegistry(dataSource, selfUrl, markAsOffline);
    }

    @Singleton
    @Measure
    NodeRequestStorage bindNodeRequestStorage(@Named("registry") final DataSource dataSource) {
        return new PostgreSQLNodeRequestStorage(dataSource);
    }

    @Singleton
    TokenProvider bindTokenProvider(
        final Provider<IdentityIssueTokenClient> identityIssueTokenClient,
        @Property(name = "authentication.identity.client.id") String identityClientId,
        @Property(name = "authentication.identity.client.secret") String identityClientSecret
    ) {
        return new IdentityIssueTokenProvider(identityIssueTokenClient, identityClientId, identityClientSecret);
    }

    @Singleton
    LocationService locationService(final Provider<LocationServiceClient> locationServiceClientProvider) {
        return new CloudLocationService(locationServiceClientProvider);
    }

    @Singleton
    public Tracer tracer() {
        return new Configuration("Aqueduct Core").getTracer();
    }
}
