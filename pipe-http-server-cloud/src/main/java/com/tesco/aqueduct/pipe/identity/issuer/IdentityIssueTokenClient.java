package com.tesco.aqueduct.pipe.identity.issuer;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;

@Client(id="identity-issue-token")
public interface IdentityIssueTokenClient {

    @Post(value = "${authentication.identity.issue.token.path}", consumes = "application/vnd.tesco.identity.tokenresponse+json")
    @CircuitBreaker(delay = "${authentication.identity.delay}", attempts = "${authentication.identity.attempts}")
    IssueTokenResponse retrieveIdentityToken(
        @Header("TraceId") String traceId,
        @Body IssueTokenRequest issueTokenRequest
    );
}
