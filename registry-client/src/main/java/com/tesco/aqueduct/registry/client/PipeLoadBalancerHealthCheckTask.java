package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

@Context
@Requires(property = "pipe.http.client.healthcheck.interval")
public class PipeLoadBalancerHealthCheckTask {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PipeLoadBalancerHealthCheckTask.class));
    private final ServiceList services;

    @Inject
    public PipeLoadBalancerHealthCheckTask(ServiceList services) {
        this.services = services;
    }

    @Scheduled(fixedDelay = "${pipe.http.client.healthcheck.interval}")
    public void checkState() {
        try {
            services.checkState();
        } catch (Throwable t) {
            LOG.error("healthcheck","unexpected error", t);
        }
    }
}
