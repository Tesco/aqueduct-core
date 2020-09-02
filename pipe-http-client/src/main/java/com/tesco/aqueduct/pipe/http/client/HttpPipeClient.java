package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.*;
import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

@Named("remote")
public class HttpPipeClient implements Reader {

    private final InternalHttpPipeClient client;

    @Inject
    public HttpPipeClient(final InternalHttpPipeClient client) {
        this.client = client;
    }

    @Override
    public MessageResults read(@Nullable final List<String> types, final long offset, final List<String> locationUuids) {

        if(locationUuids.size() != 1) {
            throw new IllegalArgumentException("Multiple location uuid's not supported in the http pipe client");
        }

        final HttpResponse<List<Message>> response = client.httpRead(types, offset, locationUuids.get(0));

        final long retryAfter = Optional
            .ofNullable(response.header(HttpHeaders.RETRY_AFTER))
            .map(value -> {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException exception) {
                    return 0L;
                }
            })
            .map(value -> Long.max(0, value))
            .orElse(0L);

        return new MessageResults(
            response.body(),
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

    private Long getGlobalOffsetHeader(HttpResponse<List<Message>> response) {
        return Long.parseLong(response.header(HttpHeaders.GLOBAL_LATEST_OFFSET));
    }

    private PipeState getPipeState(HttpResponse<List<Message>> response) {
        return PipeState.valueOf(response.header(HttpHeaders.PIPE_STATE));
    }
}
