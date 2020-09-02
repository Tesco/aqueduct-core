package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.api.IdentityToken;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.reactivex.Single;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Provider;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class IdentityIssueTokenProvider implements TokenProvider {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(IdentityIssueTokenProvider.class));

    private final Provider<IdentityIssueTokenClient> identityIssueTokenClient;
    private final String identityClientId;
    private final String identityClientSecret;
    // identityToken is a mutable value shared across different threads, hence why we are using an AtomicReference
    private AtomicReference<IssueTokenResponse> identityToken;

    public IdentityIssueTokenProvider(
        Provider<IdentityIssueTokenClient> identityIssueTokenClient,
        String identityClientId,
        String identityClientSecret
    ) {
        this.identityIssueTokenClient = identityIssueTokenClient;
        this.identityClientId = identityClientId;
        this.identityClientSecret = identityClientSecret;
        identityToken = new AtomicReference<IssueTokenResponse>();
    }

    @Override
    public Single<IdentityToken> retrieveIdentityToken() {

        return Single.defer(() -> {
            IssueTokenResponse identityTokenIssueResponse = identityToken.get();

            if (identityTokenIssueResponse != null && !identityTokenIssueResponse.isTokenExpired()) {
                return Single.just(identityTokenIssueResponse);
            }

            return identityIssueTokenClient.get()
                .retrieveIdentityToken(traceId(), new IssueTokenRequest(identityClientId, identityClientSecret))
                .doOnSuccess(issueResponse -> identityToken.set(issueResponse))
                .doOnError(this::handleError);
        });
    }

    private void handleError(Throwable error) {
        if (error instanceof HttpClientResponseException) {
            HttpClientResponseException exception = (HttpClientResponseException) error;
            LOG.error("retrieveIdentityToken", "Identity response error: ", exception);
            if (exception.getStatus().getCode() > 499) {
                throw new IdentityServiceUnavailableException("Unexpected error from Identity with status - " + exception.getStatus());
            }
        } else {
            LOG.error("retrieveIdentityToken", "Unexpected error from Identity", error);
        }
    }

    private String traceId() {
        return MDC.get("trace_id") == null ? UUID.randomUUID().toString() : MDC.get("trace_id");
    }
}
