package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.Message;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;

import javax.annotation.Nullable;
import java.util.List;

@Retryable(attempts = "3")
@Client(id = "pipe")
public interface InternalHttpPipeClient {

    @Get("/pipe/{offset}{?type}")
    @Consumes
    @Header(name="Accept-Encoding", value="gzip, deflate")
    HttpResponse<List<Message>> httpRead(
        @Nullable List<String> type,
        long offset,
        String locationUuid
    );

    @Get("/pipe/offset/latest{?type}")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    @Retryable(attempts = "${pipe.http.latest-offset.attempts}", delay = "${pipe.http.latest-offset.delay}")
    long getLatestOffsetMatching(@Nullable List<String> type);
}