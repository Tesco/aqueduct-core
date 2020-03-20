package com.tesco.aqueduct.pipe.identity.issuer;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;

@Client(value="${authentication.identity.url}")
@Header(name = "Content-Type", value = MediaType.APPLICATION_JSON)
@Requires(property = "authentication.identity.url")
@Requires(property = "authentication.identity.issue.token.path")
public interface IdentityIssueTokenClient {

    @Post(value = "${authentication.identity.issue.token.path}", consumes = "application/vnd.tesco.identity.tokenresponse+json")
    @CircuitBreaker(delay = "${authentication.identity.delay}", attempts = "${authentication.identity.attempts}")
    IssueTokenResponse retrieveIdentityToken(
        @Header("TraceId") String traceId,
        @Body IssueTokenRequest issueTokenRequest
    );
}
