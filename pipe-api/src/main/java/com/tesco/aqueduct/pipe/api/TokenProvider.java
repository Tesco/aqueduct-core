package com.tesco.aqueduct.pipe.api;

import io.reactivex.Single;

public interface TokenProvider {
    Single<? extends IdentityToken> retrieveIdentityToken();
}
