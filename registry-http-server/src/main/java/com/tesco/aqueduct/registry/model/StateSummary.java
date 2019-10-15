package com.tesco.aqueduct.registry.model;

import lombok.Data;

import java.util.List;

@Data
public class StateSummary {
    private final Node root;
    private final List<Node> followers;
}
