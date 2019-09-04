package com.tesco.aqueduct.registry;

public interface ThrowingRunnable<T extends Throwable> {
  void run() throws T;
}
