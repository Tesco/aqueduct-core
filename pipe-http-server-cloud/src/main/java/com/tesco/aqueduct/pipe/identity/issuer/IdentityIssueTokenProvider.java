package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.api.IdentityToken;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IdentityIssueTokenProvider implements TokenProvider {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(IdentityIssueTokenProvider.class));

    private final IdentityIssueTokenClient identityIssueTokenClient;
    private final String identityClientId;
    private final String identityClientSecret;
    private IdentityToken identityToken;

    public IdentityIssueTokenProvider(
        IdentityIssueTokenClient identityIssueTokenClient,
        String identityClientId,
        String identityClientSecret
    ) {
        this.identityIssueTokenClient = identityIssueTokenClient;
        this.identityClientId = identityClientId;
        this.identityClientSecret = identityClientSecret;
    }

    @Override
    public IdentityToken retrieveIdentityToken() {
        if (identityToken != null && !identityToken.isTokenExpired()) {
            return identityToken;
        }
        final String traceId = UUID.randomUUID().toString();

        try {
            identityToken = identityIssueTokenClient.retrieveIdentityToken(
                    traceId,
                    new IssueTokenRequest(
                            identityClientId,
                            identityClientSecret
                    )
            );
        } catch (HttpClientResponseException httpException) {
            try {
                LOG.info("retrieveIdentityToken",
            "request: " + JsonHelper.toJson(new IssueTokenRequest("removedClientId", "removedClientSecret")) +
                    ", response body: " + httpException.getResponse().body() +
                    ", response header: " + headers(httpException.getResponse().getHeaders()) +
                    ", status code: " + httpException.getStatus().getCode()
                );
            } catch (IOException e) {
                LOG.error("retrieveIdentityToken", "Erro while serializing issue token request", e);
            }
            throw httpException;
        }
        catch(Exception exception) {

            LOG.error("retrieveIdentityToken", "trace_id: " + traceId, exception);
            throw exception;
        }

        return identityToken;
    }

    private String headers(HttpHeaders headers) {
        Iterator<Map.Entry<String, List<String>>> iterator = headers.iterator();
        StringBuilder stringBuilder = new StringBuilder();

        while (iterator.hasNext()) {
            Map.Entry<String, List<String>> entry = iterator.next();
            stringBuilder.append(entry.getKey()).append(": ").append(entry.getValue()).append(" ");
        }
        return stringBuilder.toString();
    }
}
