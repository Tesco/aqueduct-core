package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.server.netty.HttpCompressionStrategy;
import io.netty.handler.codec.http.HttpResponse;

@Replaces(HttpCompressionStrategy.class)
class DefaultHttpCompressionStrategy implements HttpCompressionStrategy {

    @Override
    public boolean shouldCompress(HttpResponse response) {
        return false;
    }
}
