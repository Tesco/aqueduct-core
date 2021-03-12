package com.tesco.aqueduct.registry.model;

import java.util.List;

public interface NodeRegistry {
    /**
     * @param node Node to register as new or update current state
     * @return Node registered
     */
    Node register(Node node);

    /**
     * @param offset Latest offset of root
     * @param status Status of root
     * @param groups List of groups to return, all if empty or null
     * @return Summary of all currently known nodes
     */
    StateSummary getSummary(long offset, Status status, List<String> groups);

    boolean deleteNode(String group, String host);
}
