package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.convert.format.ReadableBytes;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.security.annotation.Secured;
import lombok.val;
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

    @Value("${pipe.http.server.read.poll-seconds:0}")
    private int pollSeconds;

    @ReadableBytes @Value("${pipe.http.server.read.response-size-limit-in-bytes:1024kb}")
    private int maxPayloadSizeBytes;

    @Get("/pipe/offset/latest")
    public long latestOffset(@QueryValue final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return reader.getLatestOffsetMatching(types);
    }

    @Get("/pipe/state{?type}")
    public PipeStateResponse state(@Nullable final List<String> type) {
        final List<String> types = flattenRequestParams(type);
        return pipeStateProvider.getState(types, reader);
    }

    @Get("/pipe/{offset}{?type}")
    public HttpResponse<List<Message>> readMessages(
        final long offset,
        final HttpRequest<?> request,
        @Nullable final List<String> type,
        @Nullable final String locationUuid
    ) {
        if(offset < 0) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request.getRemoteAddress().getHostName());

        final List<String> types = flattenRequestParams(type);

        LOG.withTypes(types).debug("pipe read controller", "reading with types");
        final val messageResults = reader.read(types, offset, Collections.singletonList(locationUuid));
        final val list = messageResults.getMessages();
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
