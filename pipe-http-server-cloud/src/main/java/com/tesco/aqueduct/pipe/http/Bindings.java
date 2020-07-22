package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenClient;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider;
import com.tesco.aqueduct.pipe.location.CloudLocationResolver;
import com.tesco.aqueduct.pipe.location.LocationServiceClient;
import com.tesco.aqueduct.pipe.metrics.Measure;
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
        @Value("${persistence.read.read-delay-seconds:0}") final int readDelaySeconds,
        @Value("${persistence.read.expected-node-count}") final int expectedNodeCount,
        @Value("${persistence.read.cluster-db-pool-size}") final long clusterDBPoolSize,
        @Named("postgres") final DataSource dataSource
        ) {
        return new PostgresqlStorage(
            dataSource, limit, retryAfter, maxBatchSize, readDelaySeconds, expectedNodeCount, clusterDBPoolSize
        );
    }

    @Singleton
    @Measure
    NodeRegistry bindNodeRegistry(
        @Named("postgres") final DataSource dataSource,
        @Property(name = "pipe.server.url") final URL selfUrl,
        @Value("${registry.mark-offline-after:1m}") final Duration markAsOffline
    ) {
        return new PostgreSQLNodeRegistry(dataSource, selfUrl, markAsOffline);
    }

    @Singleton
    @Measure
    NodeRequestStorage bindNodeRequestStorage(@Named("postgres") final DataSource dataSource) {
        return new PostgreSQLNodeRequestStorage(dataSource);
    }

    @Singleton
    PipeStateProvider bindPipeStateProvider() {
        return new CloudPipeStateProvider();
    }

    @Singleton
    TokenProvider bindTokenProvider(
        final IdentityIssueTokenClient identityIssueTokenClient,
        @Property(name = "authentication.identity.client.id") String identityClientId,
        @Property(name = "authentication.identity.client.secret") String identityClientSecret
    ) {
        return new IdentityIssueTokenProvider(identityIssueTokenClient, identityClientId, identityClientSecret);
    }

    @Singleton
    LocationResolver bindLocationResolver(final LocationServiceClient locationServiceClient) {
        return new CloudLocationResolver(locationServiceClient);
    }

    @Singleton
    public Tracer tracer() {
        return new Configuration("Aqueduct Core").getTracer();
    }
}
