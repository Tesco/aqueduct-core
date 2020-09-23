package com.tesco.aqueduct.pipe.http.client;

import io.micronaut.context.annotation.Primary;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;

import javax.annotation.Nullable;
import java.util.List;

@Primary
@Client(id = "pipe")
public interface InternalBrotliHttpPipeClient extends InternalHttpPipeClient {

    @Get("/pipe/{offset}{?type,location}")
    @Consumes
    @Header(name="Accept-Encoding", value="br")
    @CircuitBreaker(delay = "${pipe.http.client.delay}", attempts = "${pipe.http.client.attempts}", reset = "${pipe.http.client.reset}")
    HttpResponse<byte[]> httpRead(
        @Nullable List<String> type,
        long offset,
        String location
    );
}