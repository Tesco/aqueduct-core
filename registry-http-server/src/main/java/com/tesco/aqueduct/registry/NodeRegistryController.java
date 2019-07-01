package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.http.annotation.*;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

@Measure
@Controller("/registry")
public class NodeRegistryController {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(NodeRegistryController.class));

    @Inject
    private NodeRegistry registry;

    // This is temporary, it might be better for us to make pipe depend on registry and have it register itself in it.
    @Inject
    private MessageReader pipe;

    public NodeRegistryController(NodeRegistry registry) {
        this.registry = registry;
    }

    @Secured(SecurityRule.IS_ANONYMOUS)
    @Get
    public StateSummary getSummary(@Nullable List<String> groups) {
        return registry.getSummary(pipe.getLatestOffsetMatching(null), "ok", groups);
    }

    @Secured(SecurityRule.IS_AUTHENTICATED)
    @Post
    public List<URL> registerNode(@Body Node node) {
        List<URL> requestedToFollow = registry.register(node);
        String followStr = requestedToFollow.stream().map(URL::toString).collect(Collectors.joining(","));
        LOG.withNode(node).info("requested to follow", followStr);
        return requestedToFollow;
    }
}
