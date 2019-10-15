package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NodeGroup {
    private static final int NUMBER_OF_CHILDREN_PER_NODE = 2;

    public final List<Node> nodes;

    public NodeGroup() {
        this(new ArrayList<>());
    }

    public NodeGroup(final List<Node> nodes) {
        this.nodes = nodes;
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public boolean removeById(final String nodeId) {
        return nodes.removeIf(node -> node.getId().equals(nodeId));
    }

    public Node add(final Node node, final URL cloudUrl) {
        final List<URL> followUrls = getFollowerUrls(cloudUrl);
        final Node newNode = node.toBuilder()
            .requestedToFollow(followUrls)
            .lastSeen(ZonedDateTime.now())
            .build();
        nodes.add(newNode);
        return newNode;
    }

    public Node get(final int index) {
        return nodes.get(index);
    }

    public Node getById(final String nodeId) {
        return nodes
            .stream()
            .filter(n -> n.getId().equals(nodeId))
            .findAny()
            .orElse(null);
    }

    public List<URL> getNodeUrls() {
        return nodes.stream()
            .map(Node::getLocalUrl)
            .collect(Collectors.toList());
    }

    public Node updateNode(final Node updatedNode) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).getId().equals(updatedNode.getId())) {
                return nodes.set(i, updatedNode);
            }
        }

        throw new IllegalStateException("The node was not found " + updatedNode.getId());
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(nodes);
    }

    public void rebalance(final URL cloudUrl) {
        final List<URL> allUrls = getNodeUrls();
        for (int i = 0; i < allUrls.size(); i++) {
            final List<URL> followUrls = getFollowerUrls(cloudUrl, i);
            final Node updatedNode = nodes
                .get(i)
                .toBuilder()
                .requestedToFollow(followUrls)
                .build();

            this.updateNode(updatedNode);
        }
    }

    private List<URL> getFollowerUrls(final URL cloudUrl) {
        return getFollowerUrls(cloudUrl, getNodeUrls().size());
    }

    private List<URL> getFollowerUrls(final URL cloudUrl, int nodeIndex) {
        final List<URL> followUrls = new ArrayList<>();
        final List<URL> allUrls = getNodeUrls();
        if (nodeIndex < 0) nodeIndex = allUrls.size();
        while (nodeIndex != 0) {
            nodeIndex = ((nodeIndex + 1) / NUMBER_OF_CHILDREN_PER_NODE) - 1;
            followUrls.add(allUrls.get(nodeIndex));
        }
        followUrls.add(cloudUrl);
        return followUrls;
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        for (final Node node : nodes) {
            if (node.getLastSeen().compareTo(threshold) < 0) {
                this.updateNode(node.toBuilder().status("offline").build());
            }
        }
    }
}
