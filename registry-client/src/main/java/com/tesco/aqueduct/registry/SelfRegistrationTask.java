package com.tesco.aqueduct.registry;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Context
@Requires(property = "pipe.http.registration.interval")
public class SelfRegistrationTask {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final RegistryHitList hitList;
    private final Supplier<Node> selfSummary;
    private final String cloudPipeUrl;

    private boolean hasRegistered;

    @Inject
    public SelfRegistrationTask(
        RegistryClient client,
        RegistryHitList hitList,
        @Named("selfSummarySupplier") Supplier<Node> selfSummary,
        @Property(name = "pipe.http.client.url") String cloudPipeUrl
    ) {
        this.client = client;
        this.hitList = hitList;
        this.selfSummary = selfSummary;
        this.cloudPipeUrl = cloudPipeUrl;
    }

    @Scheduled(fixedRate = "${pipe.http.registration.interval}")
    void register() {
        try {
            Node node = selfSummary.get();
            List<URL> upstreamEndpoints = client.register(node);
            hitList.update(upstreamEndpoints);
            hasRegistered = true;
        } catch (Exception e) {
            LOG.error("registration scheduled task", "Register error", e);

            if (!hasRegistered) {
                defaultToFollowCloudPipe();
            }
        }
     }

    private void defaultToFollowCloudPipe() {
        try {
            LOG.info("registration scheduled task", "Defaulting to follow the Cloud Pipe server.");
            hitList.update(Collections.singletonList(new URL(cloudPipeUrl)));

        } catch (MalformedURLException e) {
            LOG.error("registration scheduled task", "Invalid Cloud Pipe URL.", e);
        }
     }
}
