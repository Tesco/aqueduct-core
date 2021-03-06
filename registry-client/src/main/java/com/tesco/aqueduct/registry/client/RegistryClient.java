package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.RegistryResponse;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;

@Client("${registry.http.client.url}")
@Requires(property = "pipe.http.register.retry.interval")
public interface RegistryClient {
    @Retryable(delay = "${pipe.http.register.retry.interval}")
    @Post(uri = "/registry")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    RegistryResponse register(@Body Node node);
}
