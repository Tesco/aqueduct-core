package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.model.Bootstrapable;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.RegistryResponse;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final SummarySupplier selfSummary;
    private final ServiceList services;
    private final Bootstrapable provider;
    private final Bootstrapable pipe;
    private final long bootstrapDelayMs;

    @Inject
    public SelfRegistrationTask(
        final RegistryClient client,
        final SummarySupplier selfSummary,
        final ServiceList services,
        @Named("provider") final Bootstrapable provider,
        @Named("pipe") final Bootstrapable pipe,
        @Property(name = "pipe.http.registration.interval") String retryInterval,
        @Value("${pipe.bootstrap.delay:300000}") final int additionalDelay // 5 minutes extra to allow all nodes to reset
    ) {
        this.client = client;
        this.selfSummary = selfSummary;
        this.services = services;
        this.provider = provider;
        this.pipe = pipe;
        this.bootstrapDelayMs = Duration.parse("PT" + retryInterval).toMillis() + additionalDelay;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            final Node node = selfSummary.getSelfNode();
            final RegistryResponse registryResponse = client.register(node);
            if (registryResponse.getRequestedToFollow() == null) {
                LOG.error("SelfRegistrationTask.register", "Register error", "Null response received");
                return;
            }
            services.update(registryResponse.getRequestedToFollow());
            switch (registryResponse.getBootstrapType()) {
                case PROVIDER:
                    provider.stop();
                    provider.reset();
                    provider.start();
                    break;
                case PIPE_AND_PROVIDER:
                    provider.stop();
                    provider.reset();
                    pipe.reset();
                    pipe.start();
                    provider.start();
                    break;
                case PIPE:
                    pipe.reset();
                    pipe.start();
                    break;
                case PIPE_WITH_DELAY:
                    pipe.reset();
                    Thread.sleep(bootstrapDelayMs);
                    pipe.start();
                    break;
                case PIPE_AND_PROVIDER_WITH_DELAY:
                    provider.stop();
                    provider.reset();
                    pipe.reset();
                    Thread.sleep(bootstrapDelayMs);
                    pipe.start();
                    provider.start();
                    break;
                case CORRUPTION_RECOVERY:
                    provider.stop();
                    provider.reset();

                    //drop the database
                    //restart the provider
                    System.exit(0);
            }


        } catch (HttpClientResponseException hcre) {
            LOG.error("SelfRegistrationTask.register", "Register error [HttpClientResponseException]: %s", hcre.getMessage());
        } catch (Exception e) {
            LOG.error("SelfRegistrationTask.register", "Register error", e);
        }
    }
}
