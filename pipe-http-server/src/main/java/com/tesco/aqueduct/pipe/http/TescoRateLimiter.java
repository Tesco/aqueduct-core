package com.tesco.aqueduct.pipe.http;

import com.google.common.util.concurrent.RateLimiter;
import lombok.Getter;

import javax.inject.Singleton;

@Getter
@Singleton
public class TescoRateLimiter {
    private RateLimiter rateLimiter;

    public TescoRateLimiter() {
        this.rateLimiter = RateLimiter.create(1.0);
    }
}
