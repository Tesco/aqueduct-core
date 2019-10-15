package com.tesco.aqueduct.registry.model;

import java.net.URL;
import java.util.List;

public interface NodeRegistry {
    /**
     * @param node Node to register as new or update current state
     * @return List of URL this node should currently follow
     */
    List<URL> register(Node node);

    /**
     * @param offset Latest offset of root
     * @param status Status of root
     * @param groups List of groups to return, all if empty or null
     * @return Summary of all currently known nodes
     */
    StateSummary getSummary(long offset, String status, List<String> groups);

    boolean deleteNode(String group, String id);
}
