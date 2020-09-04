package com.tesco.aqueduct.pipe.identity.validator;

import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.CircuitBreaker;
import io.reactivex.Flowable;

@Measure
@Client("${authentication.identity.url}")
@Requires(property = "authentication.identity.url")
public interface IdentityTokenValidatorClient {

    @Post("${authentication.identity.validate.token.path}")
    @CircuitBreaker(delay = "${authentication.identity.delay}", attempts = "${authentication.identity.attempts}")
    Flowable<ValidateTokenResponse> validateToken(
        @Header("TraceId") String traceId,
        @Body ValidateTokenRequest identityRequest,
        @QueryValue(value="clientIdAndSecret") String clientIdAndSecret
    );
}
