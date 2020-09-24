package com.tesco.aqueduct.pipe.http.client;

import io.micronaut.http.HttpResponse;

import javax.annotation.Nullable;
import java.util.List;

public interface InternalHttpPipeClient {
    HttpResponse<byte[]> httpRead(
        @Nullable List<String> type,
        long offset,
        String location
    );
}
