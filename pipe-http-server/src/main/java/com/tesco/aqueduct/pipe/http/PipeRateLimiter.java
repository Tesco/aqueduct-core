package com.tesco.aqueduct.pipe.http;

import com.google.common.util.concurrent.RateLimiter;
import io.micronaut.context.annotation.Property;

import javax.inject.Singleton;

@Singleton
public class PipeRateLimiter {
    private RateLimiter rateLimiter;

    public PipeRateLimiter(@Property(name = "rate-limiter.capacity") int capacity) {
        this.rateLimiter = RateLimiter.create(capacity);
    }

    public boolean tryAcquire(int permits){
        return this.rateLimiter.tryAcquire(permits);
    }
}
