package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.api.TokenProvider;

import java.util.Collections;
import java.util.List;

public class CloudLocationResolver implements LocationResolver {

    private final TokenProvider tokenProvider;

    /*
        The TokenProvider dependency here exists to make call to Identity to issue a token but not do anything with it for now.
        Once we have a declarative Location service client the dependency will be removed and filter should be used for issuing a token.
     */
    public CloudLocationResolver(TokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    @Override
    public List<String> resolve(String location) {
        tokenProvider.retrieveIdentityToken();
        return Collections.emptyList();
    }
}
