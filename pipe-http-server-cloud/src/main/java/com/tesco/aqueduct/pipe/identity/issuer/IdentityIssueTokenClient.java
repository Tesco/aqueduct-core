package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;
import io.reactivex.Single;

@Client("${authentication.identity.url}")
@Measure
public interface IdentityIssueTokenClient {

    @Post(value = "${authentication.identity.issue.token.path}", consumes = "${authentication.identity.consumes}")
    @CircuitBreaker(delay = "${authentication.identity.delay}", attempts = "${authentication.identity.attempts}")
    Single<IssueTokenResponse> retrieveIdentityToken(
        @Header("TraceId") String traceId,
        @Body IssueTokenRequest issueTokenRequest
    );
}
