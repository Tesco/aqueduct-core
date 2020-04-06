package com.tesco.aqueduct.pipe.storage;

import org.openjdk.jmh.annotations.*;

@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 5, time = 1)
public class QueueBenchmark {
    @State(Scope.Thread)
    public static class MyState {

        @Setup(Level.Iteration)
        public void doSetup() {

        }

        @TearDown(Level.Iteration)
        public void doTearDown() {

        }
    }

    @Benchmark
    public void addToQueueMethod(MyState state) throws InterruptedException {
    }
}
