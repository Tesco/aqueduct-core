package com.tesco.aqueduct.pipe.http;

import io.micronaut.http.server.netty.HttpCompressionStrategy;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Compression strategy hook provided by Micronaut to disable smart gzip compression it performs by default.
 * Micronaut has a content size threshold where it compresses by default if content size is above certain threshold,
 * another way to disable smart compression is to specify large threshold value within configuration.
 */
public class HttpNoFrameworkCompressionStrategy implements HttpCompressionStrategy {
    @Override
    public boolean shouldCompress(HttpResponse response) {
        return false;
    }
}
