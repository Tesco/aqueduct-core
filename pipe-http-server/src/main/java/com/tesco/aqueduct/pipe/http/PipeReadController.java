package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.convert.format.ReadableBytes;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.security.annotation.Secured;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tesco.aqueduct.pipe.api.PipeState.OUT_OF_DATE;
import static com.tesco.aqueduct.pipe.api.PipeState.UP_TO_DATE;

@Secured("PIPE_READ")
@Measure
@Controller
public class PipeReadController {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadController.class));

    //TODO: Use constructor
    @Inject @Named("local")
    private Reader reader;

    @Inject
    private PipeStateProvider pipeStateProvider;

    @Inject
    private LocationResolver locationResolver;

    @Value("${pipe.http.server.read.poll-seconds:0}")
    private int pollSeconds;

    @ReadableBytes @Value("${pipe.http.server.read.response-size-limit-in-bytes:1024kb}")
    private int maxPayloadSizeBytes;

    @Get("/pipe/offset/latest")
    @Deprecated // remove when its non-usage is confirmed
    public String latestOffset(@QueryValue final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return Long.toString(reader.getLatestOffsetMatching(types));
    }

    @Get("/pipe/state{?type}")
    @Deprecated // remove when its non-usage is confirmed
    public PipeStateResponse state(@Nullable final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return pipeStateProvider.getState(types, reader);
    }

    @Get("/pipe/{offset}{?type,location}")
    public HttpResponse<List<Message>> readMessages(
        final long offset,
        final HttpRequest<?> request,
        @Nullable final List<String> type,
        @Nullable final String location
    ) {
        if(offset < 0 || StringUtils.isEmpty(location)) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request.getRemoteAddress().getHostName());

        final List<String> types = flattenRequestParams(type);

        LOG.withTypes(types).debug("pipe read controller", "reading with types");
        final MessageResults messageResults = reader.read(types, offset, resolveTargetUuidsFrom(location));
        final List<Message> list = messageResults.getMessages();
        final long retryTime = messageResults.getRetryAfterSeconds();

        LOG.debug("pipe read controller", String.format("set retry time to %d", retryTime));
        MutableHttpResponse<List<Message>> response = HttpResponse.ok(list)
            .header(HttpHeaders.RETRY_AFTER, String.valueOf(retryTime))
            .header(
                HttpHeaders.PIPE_STATE,
                pipeStateProvider.getState(types, reader).isUpToDate() ? UP_TO_DATE.toString() : OUT_OF_DATE.toString()
            );

        messageResults.getGlobalLatestOffset()
            .ifPresent(
                globalLatestOffset -> response.header(HttpHeaders.GLOBAL_LATEST_OFFSET, Long.toString(globalLatestOffset))
            );

        return response;
    }

    private List<String> resolveTargetUuidsFrom(@Nullable String location) {
        return location == null
            ? Collections.emptyList()
            : locationResolver.resolve(location).stream().map(Cluster::getId).collect(Collectors.toList());
    }

    private void logOffsetRequestFromRemoteHost(final long offset, final String hostName) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(
                "pipe read controller",
                String.format("reading from offset %d, requested by %s", offset, hostName)
            );
        }
    }

    private List<String> flattenRequestParams(final List<String> strings) {
        if(strings == null) {
            return Collections.emptyList();
        }
        return strings
            .stream()
            .flatMap(s -> Stream.of(s.split(",")))
            .collect(Collectors.toList());
    }
}
