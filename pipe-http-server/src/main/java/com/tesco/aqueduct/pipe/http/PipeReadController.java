package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.codec.ContentEncoder;
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
import io.micronaut.security.annotation.Secured;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Inject
    ContentEncoder contentEncoder;

    @Inject
    PipeRateLimiter rateLimiter;

    @Get("/pipe/{offset}{?type,location}")
    public HttpResponse<byte[]> readMessages(
        final long offset,
        final HttpRequest<?> request,
        @Nullable final List<String> type,
        @Nullable final String location
    ) {
        if (offset < 0 || StringUtils.isEmpty(location)) {
            return HttpResponse.badRequest();
        }

        logOffsetRequestFromRemoteHost(offset, request);
        final List<String> types = flattenRequestParams(type);
        LOG.withTypes(types).debug("pipe read controller", "reading with types");

        final MessageResults messageResults = reader.read(types, offset, locationResolver.resolve(location));
        final List<Message> messages = messageResults.getMessages();

        final long retryAfterMs = isBootstrappingAndCapacityAvailable(messages) ? 0 : messageResults.getRetryAfterMs();

        LOG.debug("pipe read controller", String.format("set retry time to %d", retryAfterMs));
        byte[] responseBytes = JsonHelper.toJson(messages).getBytes();

        ContentEncoder.EncodedResponse encodedResponse = contentEncoder.encodeResponse(request, responseBytes);

        Map<CharSequence, CharSequence> responseHeaders = new HashMap<>(encodedResponse.getHeaders());

        final long retryAfterSeconds = (long) Math.ceil(retryAfterMs / (double) 1000);

        responseHeaders.put(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
        responseHeaders.put(HttpHeaders.RETRY_AFTER_MS, String.valueOf(retryAfterMs));
        responseHeaders.put(HttpHeaders.PIPE_STATE,
                pipeStateProvider.getState(types, reader).isUpToDate() ? UP_TO_DATE.toString() : OUT_OF_DATE.toString());

        MutableHttpResponse<byte[]> response = HttpResponse.ok(encodedResponse.getEncodedBody()).headers(responseHeaders);

        messageResults.getGlobalLatestOffset()
            .ifPresent(
                globalLatestOffset -> response.header(HttpHeaders.GLOBAL_LATEST_OFFSET, Long.toString(globalLatestOffset))
            );

        return response;
    }

    private boolean isBootstrappingAndCapacityAvailable(List<Message> messages) {
        return !messages.isEmpty()
            && messages.get(0).getCreated().isBefore(ZonedDateTime.now().minusHours(6))
            && rateLimiter.tryAcquire(1);
    }

    private void logOffsetRequestFromRemoteHost(final long offset, final HttpRequest<?> request) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(
                "pipe read controller",
                String.format("reading from offset %d, requested by %s", offset, request.getRemoteAddress().getHostName())
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
