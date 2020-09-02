package com.tesco.aqueduct.pipe.http;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationFailed;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
@Requires(property = "authentication.users")
public class PipeReadAuthenticationProvider implements AuthenticationProvider {
    private final List<User> users;

    @Inject
    public PipeReadAuthenticationProvider(final List<User> users) {
        this.users = users;
    }

    private AuthenticationResponse authenticate(final Object username, final Object password) {
        return users.stream()
            .filter(user -> user.isAuthenticated(username, password))
            .findAny()
            .map(User::toAuthenticationResponse)
            .orElse(new AuthenticationFailed());
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(
        @Nullable HttpRequest<?> httpRequest,
        AuthenticationRequest<?, ?> authenticationRequest
    ) {
        final Object identity  = authenticationRequest.getIdentity();
        final Object secret = authenticationRequest.getSecret();
        return Flowable.just(
            authenticate(identity, secret)
        );
    }
}
