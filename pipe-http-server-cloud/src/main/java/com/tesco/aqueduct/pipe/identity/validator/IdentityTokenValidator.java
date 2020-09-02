package com.tesco.aqueduct.pipe.identity.validator;

import com.tesco.aqueduct.pipe.identity.issuer.IdentityServiceUnavailableException;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.async.annotation.SingleResult;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationUserDetailsAdapter;
import io.micronaut.security.authentication.UserDetails;
import io.micronaut.security.token.validator.TokenValidator;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

@Singleton
@Requires(property = "authentication.identity.url")
@Requires(property = "authentication.identity.users")
public class IdentityTokenValidator implements TokenValidator {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(IdentityTokenValidator.class));
    private final List<TokenUser> users;
    private final String clientIdAndSecret;
    private final IdentityTokenValidatorClient identityTokenValidatorClient;
    private final ExecutorService requestThreadPool;

    @Inject
    public IdentityTokenValidator(
        final IdentityTokenValidatorClient identityTokenValidatorClient,
        @Value("${authentication.identity.client.id}") final String clientId,
        @Value("${authentication.identity.client.secret}") final String clientSecret,
        final List<TokenUser> users,
        @Named(TaskExecutors.IO) ExecutorService requestThreadPool
    ) {
        this.identityTokenValidatorClient = identityTokenValidatorClient;
        this.requestThreadPool = requestThreadPool;
        this.clientIdAndSecret = clientId + ":" + clientSecret;
        this.users = users;
    }

    @Override // Deprecated, hence remove at some point
    public Publisher<Authentication> validateToken(String token) {
        return validateToken(token, null);
    }

    @Override
    @SingleResult
    @Cacheable(value="identity-cache", parameters = "token")
    public Publisher<Authentication> validateToken(String token, @Nullable HttpRequest<?> request) {

        if (token == null) {
            LOG.error("token validator", "null token", "");
            return Flowable.empty();
        }

        return identityTokenValidatorClient
            .validateToken(traceId(), new ValidateTokenRequest(token), clientIdAndSecret)
            .doOnError(error -> handleError(error))
            // Need to observe on configured thread pool so our blocking controller continues on this thread pool too.
            // @executeOn annotation supported by Micronaut 2.0 doesn't seem to work in conjunction with Micronaut TokenValidator
            // we raised an issue to Micronaut: https://github.com/micronaut-projects/micronaut-security/issues/313
            // we are keen to remove the RX code once a fix is released
            .observeOn(Schedulers.from(this.requestThreadPool))
            .filter(ValidateTokenResponse::isTokenValid)
            .map(ValidateTokenResponse::getClientUserID)
            .map(this::toUserDetailsAdapter);
    }

    private void handleError(Throwable error) {
        if (error instanceof HttpClientResponseException) {
            HttpClientResponseException exception = (HttpClientResponseException) error;
            LOG.error("validateToken", "Identity response error", exception);
            if (exception.getStatus().getCode() > 499) {
                throw new IdentityServiceUnavailableException("Unexpected error from Identity with status - " + exception.getStatus());
            }
        } else {
            LOG.error("validateToken", "Unexpected error from Identity", error);
        }
    }

    private String traceId() {
        return MDC.get("trace_id") == null ? UUID.randomUUID().toString() : MDC.get("trace_id");
    }

    private AuthenticationUserDetailsAdapter toUserDetailsAdapter(String clientId) {
        List<String> roles = users.stream()
            .filter(u -> u.clientId.equals(clientId))
            .filter(u -> u.roles != null)
            .map(u -> u.roles)
            .findFirst()
            .orElse(Collections.emptyList());

        UserDetails userDetails = new UserDetails(clientId, roles);

        return new AuthenticationUserDetailsAdapter(userDetails, "roles", "user");
    }

    //lowest precedence chosen so it is used after others
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}