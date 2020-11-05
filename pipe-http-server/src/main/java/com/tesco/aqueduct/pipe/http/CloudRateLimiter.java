package com.tesco.aqueduct.pipe.http;

import com.google.common.util.concurrent.RateLimiter;

public class CloudRateLimiter implements PipeRateLimiter {
    private RateLimiter rateLimiter;

    public CloudRateLimiter(double capacity) {
        this.rateLimiter = RateLimiter.create(capacity);
    }

    @Override
    public boolean tryAcquire(int permits){
        return this.rateLimiter.tryAcquire(permits);
    }
}
