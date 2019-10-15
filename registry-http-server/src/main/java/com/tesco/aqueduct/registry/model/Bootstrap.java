package com.tesco.aqueduct.registry.model;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Bootstrap {
    private final BootstrapType type;
    private final LocalDateTime requestedDate;
}
