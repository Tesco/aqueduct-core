package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.TokenProvider;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import org.reactivestreams.Publisher;

@Filter("${location.clusters.get.path.filter.pattern}")
public class AuthenticateLocationServiceFilter implements HttpClientFilter {

    private final TokenProvider identityTokenProvider;

    public AuthenticateLocationServiceFilter(TokenProvider identityTokenProvider) {
        this.identityTokenProvider = identityTokenProvider;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(
        final MutableHttpRequest<?> request, final ClientFilterChain chain) {
        return identityTokenProvider.retrieveIdentityToken()
            .map(tokenResponse -> request.bearerAuth(tokenResponse.getAccessToken()))
            .flatMapPublisher(chain::proceed);
    }
}
