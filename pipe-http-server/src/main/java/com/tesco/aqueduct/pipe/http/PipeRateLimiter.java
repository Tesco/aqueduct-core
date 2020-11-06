package com.tesco.aqueduct.pipe.http;

import com.google.common.util.concurrent.RateLimiter;
import io.micronaut.context.annotation.Property;

import javax.inject.Singleton;

@Singleton
public class PipeRateLimiter {
    private RateLimiter rateLimiter;

    public PipeRateLimiter(@Property(name="rate-limiter.capacity", defaultValue = "10") double capacity) {
        this.rateLimiter = RateLimiter.create(capacity);
    }

    public boolean tryAcquire() {
        return this.rateLimiter.tryAcquire();
    }
}
