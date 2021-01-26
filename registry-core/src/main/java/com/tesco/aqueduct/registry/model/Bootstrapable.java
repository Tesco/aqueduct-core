package com.tesco.aqueduct.registry.model;

public interface Bootstrapable extends Resetable {
    void stop();
    void start();
}
