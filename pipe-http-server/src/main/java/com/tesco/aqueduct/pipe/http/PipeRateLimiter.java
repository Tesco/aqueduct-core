package com.tesco.aqueduct.pipe.http;

import com.google.common.util.concurrent.RateLimiter;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;

@Singleton
public class PipeRateLimiter {
    private RateLimiter rateLimiter;

    public PipeRateLimiter(@Value("rate-limiter.capacity:10") double capacity) {
        this.rateLimiter = RateLimiter.create(capacity);
    }

    public boolean tryAcquire() {
        return this.rateLimiter.tryAcquire();
    }
}
