package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.codec.Codec;
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

    @Inject
    public HttpPipeClient(final InternalHttpPipeClient client, final Codec codec) {
        this.client = client;
        this.codec = codec;
    }

    @Override
    public MessageResults read(@Nullable final List<String> types, final long offset, final List<String> locationUuids) {

        if(locationUuids.size() != 1) {
            throw new IllegalArgumentException("Multiple location uuid's not supported in the http pipe client");
        }

        final HttpResponse<byte[]> response = client.httpRead(types, offset, locationUuids.get(0));

        final byte[] responseBody;

        if (response.getHeaders().contains(X_CONTENT_ENCODING) &&
                response.getHeaders().get(X_CONTENT_ENCODING).contains("br")) {
            responseBody = codec.decode(response.body());
        } else {
            responseBody = response.body();
        }

        final long retryAfter = Optional
            .ofNullable(response.header(HttpHeaders.RETRY_AFTER_MS))
            .map(this::checkForValidNumber)
            .map(value -> Long.max(0, value))
            .orElse(Optional
                .ofNullable(response.header(HttpHeaders.RETRY_AFTER))
                .map(this::checkForValidNumber)
                .map(value -> Long.max(0, value * 1000))
                .orElse(0L));

        return new MessageResults(
            JsonHelper.messageFromJsonArray(responseBody),
            retryAfter,
            OptionalLong.of(getGlobalOffsetHeader(response)),
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

    private long checkForValidNumber(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            return 0L;
        }
    }

    private Long getGlobalOffsetHeader(HttpResponse<?> response) {
        return Long.parseLong(response.header(HttpHeaders.GLOBAL_LATEST_OFFSET));
    }

    private PipeState getPipeState(HttpResponse<?> response) {
        return PipeState.valueOf(response.header(HttpHeaders.PIPE_STATE));
    }
}
