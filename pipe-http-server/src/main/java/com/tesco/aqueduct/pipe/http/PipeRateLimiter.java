package com.tesco.aqueduct.pipe.http;

public interface PipeRateLimiter {
    boolean tryAcquire(int permits);
}
