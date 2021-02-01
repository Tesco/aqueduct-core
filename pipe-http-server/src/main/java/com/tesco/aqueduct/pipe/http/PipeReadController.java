package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.codec.ContentEncoder;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.context.annotation.Property;
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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Secured("PIPE_READ")
@Measure
@Controller
public class PipeReadController {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadController.class));

    private final Reader reader;
    private final Duration bootstrapThreshold;
    private final ContentEncoder contentEncoder;
    private final PipeRateLimiter rateLimiter;
    private final boolean logging;

    @Inject
    public PipeReadController(
        @Named("local") Reader reader,
        @Property(name = "pipe.bootstrap.threshold", defaultValue = "6h") Duration bootstrapThreshold,
        @Property(name= "bootstrap.retry.logging", defaultValue = "false") boolean logging,
        ContentEncoder contentEncoder,
        PipeRateLimiter rateLimiter
    ) {
        this.reader = reader;
        this.bootstrapThreshold = bootstrapThreshold;
        this.logging = logging;
        this.contentEncoder = contentEncoder;
        this.rateLimiter = rateLimiter;
    }

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

        final MessageResults messageResults = reader.read(types, offset, location);
        final List<Message> messages = messageResults.getMessages();

        final long retryAfterMs = calculateRetryAfter(messageResults);
        LOG.debug("pipe read controller", String.format("set retry time to %d", retryAfterMs));

        byte[] responseBytes = JsonHelper.toJson(messages).getBytes();

        ContentEncoder.EncodedResponse encodedResponse = contentEncoder.encodeResponse(request, responseBytes);

        Map<CharSequence, CharSequence> responseHeaders = new HashMap<>(encodedResponse.getHeaders());

        final long retryAfterSeconds = (long) Math.ceil(retryAfterMs / (double) 1000);

        responseHeaders.put(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
        responseHeaders.put(HttpHeaders.RETRY_AFTER_MS, String.valueOf(retryAfterMs));
        responseHeaders.put(HttpHeaders.PIPE_STATE, messageResults.getPipeState().toString());

        MutableHttpResponse<byte[]> response = HttpResponse.ok(encodedResponse.getEncodedBody()).headers(responseHeaders);

        messageResults.getGlobalLatestOffset()
            .ifPresent(
                globalLatestOffset -> response.header(HttpHeaders.GLOBAL_LATEST_OFFSET, Long.toString(globalLatestOffset))
            );

        return response;
    }

    private long calculateRetryAfter(MessageResults messageResults) {
        if (isBootstrappingAndCapacityAvailable(messageResults.getMessages())) {
            if(logging) {
                LOG.info("pipe read controller", "retry time is 0ms");
            }
            return 0;
        } else {
            return messageResults.getRetryAfterMs();
        }
    }

    private boolean isBootstrappingAndCapacityAvailable(List<Message> messages) {
        return !messages.isEmpty()
            && isBeforeBootstrapThreshold(messages)
            && rateLimiter.tryAcquire();
    }

    private boolean isBeforeBootstrapThreshold(List<Message> messages) {
        return messages.get(0).getCreated().isBefore(ZonedDateTime.now().minus(bootstrapThreshold));
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
