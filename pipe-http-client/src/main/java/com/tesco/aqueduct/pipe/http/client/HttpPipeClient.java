package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.codec.Codec;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.tesco.aqueduct.pipe.api.HttpHeaders.X_CONTENT_ENCODING;

@Named("remote")
public class HttpPipeClient implements Reader {

    private final InternalHttpPipeClient client;
    private final Codec codec;
    private final long defaultRetryAfter;

    @Inject
    public HttpPipeClient(
        final InternalHttpPipeClient client,
        final Codec codec,
        @Property(name = "persistence.read.default-retry-after") long defaultRetryAfter
    ) {
        this.client = client;
        this.codec = codec;
        this.defaultRetryAfter = defaultRetryAfter;
    }

    @Override
    public MessageResults read(@Nullable final List<String> types, final long offset, final String locationUuid) {

        final HttpResponse<byte[]> response = client.httpRead(types, offset, locationUuid);

        final byte[] responseBody;

        if (response.getHeaders().contains(X_CONTENT_ENCODING) &&
                response.getHeaders().get(X_CONTENT_ENCODING).contains("br")) {
            responseBody = codec.decode(response.body());
        } else {
            responseBody = response.body();
        }

        final long retryAfter = Optional
            .ofNullable(response.header(HttpHeaders.RETRY_AFTER_MS))
            .map(value -> checkForValidNumber(value, 1))
            .orElse(Optional
                .ofNullable(response.header(HttpHeaders.RETRY_AFTER))
                .map(value -> checkForValidNumber(value, 1000))
                .orElse(defaultRetryAfter));

        return new MessageResults(
            JsonHelper.messageFromJsonArray(responseBody),
            retryAfter,
            getGlobalOffsetHeader(response),
            getPipeState(response)
        );
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        throw new UnsupportedOperationException("HttpPipeClient does not support this operation.");
    }

    @Override
    public PipeState getPipeState() {
        throw new UnsupportedOperationException("HttpPipeClient does not support this operation.");
    }

    private long checkForValidNumber(String value, int multiplier) {
        try {
            return Long.parseLong(value) >= 0 ? Long.parseLong(value) * multiplier : defaultRetryAfter;
        } catch (NumberFormatException exception) {
            return defaultRetryAfter;
        }
    }

    private OptionalLong getGlobalOffsetHeader(HttpResponse<?> response) {
        String globalLatestOffset = response.header(HttpHeaders.GLOBAL_LATEST_OFFSET);
        return globalLatestOffset == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(globalLatestOffset));
    }

    private PipeState getPipeState(HttpResponse<?> response) {
        return PipeState.valueOf(response.header(HttpHeaders.PIPE_STATE));
    }
}
