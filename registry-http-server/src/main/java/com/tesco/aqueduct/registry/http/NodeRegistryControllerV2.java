package com.tesco.aqueduct.registry.http;

import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.pipe.api.OffsetName;
import com.tesco.aqueduct.pipe.api.Reader;
import com.tesco.aqueduct.pipe.codec.GzipCodec;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.model.Status;
import com.tesco.aqueduct.registry.model.*;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;

@Measure
@Controller("/v2/registry")
public class NodeRegistryControllerV2 {
    private static final String REGISTRY_DELETE = "REGISTRY_DELETE";
    private static final String BOOTSTRAP_NODE = "BOOTSTRAP_NODE";
    private static final String REGISTRY_WRITE = "REGISTRY_WRITE";

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(NodeRegistryControllerV2.class));
    private final NodeRegistry registry;
    private final NodeRequestStorage nodeRequestStorage;
    private final Reader pipe;

    @Property(name="compression.threshold-in-bytes")
    private int compressionThreshold;

    @Inject
    private GzipCodec gzip;

    public NodeRegistryControllerV2(final NodeRegistry registry, final NodeRequestStorage nodeRequestStorage, final Reader pipe) {
        this.registry = registry;
        this.nodeRequestStorage = nodeRequestStorage;
        this.pipe = pipe;
    }

    @Secured(SecurityRule.IS_AUTHENTICATED)
    @Get
    public HttpResponse<byte[]> getSummary(@Nullable final List<String> groups) {
        StateSummary stateSummary = registry.getSummary(pipe.getOffset(OffsetName.GLOBAL_LATEST_OFFSET).getAsLong(), Status.OK, groups);

        return compressResponseIfNeeded(stateSummary);
    }

    @Secured(REGISTRY_WRITE)
    @Post
    public RegistryResponse registerNode(@Body final Node node) throws SQLException, SubGroupIdNotAvailableException {
        if (node.getSubGroupId() == null) {
            throw new SubGroupIdNotAvailableException(
                String.format("Sub group id needs to be available for %s", node.getHost())
            );
        }
        LOG.withNode(node).info("register node: ", "node registered");
        final List<URL> requestedToFollow = registry.register(node);
        final BootstrapType bootstrapType = nodeRequestStorage.requiresBootstrap(node.getHost());
        LOG.withNode(node).info("requested to follow", "node registration complete");
        return new RegistryResponse(requestedToFollow, bootstrapType);
    }

    @Secured(REGISTRY_DELETE)
    @Delete("/{group}/{host}")
    public HttpResponse deleteNode(final String group, final String host) {
        final boolean deleted = registry.deleteNode(group, host);
        if (deleted) {
            return HttpResponse.status(HttpStatus.OK);
        } else {
            return HttpResponse.status(HttpStatus.NOT_FOUND);
        }
    }

    @Secured(BOOTSTRAP_NODE)
    @Post("/bootstrap")
    public HttpResponse bootstrap(@Body final BootstrapRequest bootstrapRequest) throws SQLException {
        bootstrapRequest.save(nodeRequestStorage);
        return HttpResponse.status(HttpStatus.OK);
    }

    @Error(exception = SubGroupIdNotAvailableException.class)
    public HttpResponse<JsonError> subGroupIdNotAvailableError(SubGroupIdNotAvailableException exception) {
        JsonError error = new JsonError(exception.getMessage());
        LOG.error("NodeRegistryControllerV2", "Failed to register", exception);
        return HttpResponse.<JsonError>status(HttpStatus.UNPROCESSABLE_ENTITY).body(error);
    }

    private HttpResponse<byte[]> compressResponseIfNeeded(StateSummary stateSummary) {
        byte[] stateSummaryInBytes = JsonHelper.toJsonBytes(stateSummary);
        if (stateSummaryInBytes.length > compressionThreshold) {
            return HttpResponse.ok(gzip.encode(stateSummaryInBytes))
                .header(HttpHeaders.CONTENT_ENCODING, "gzip");
        } else {
            return HttpResponse.ok(stateSummaryInBytes);
        }
    }
}