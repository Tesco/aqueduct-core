package com.tesco.aqueduct.registry.model;

import com.tesco.aqueduct.pipe.api.JsonHelper;

import java.io.IOException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;

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

    public boolean removeByHost(final String host) {
        return nodes.removeIf(node -> node.getHost().equals(host));
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

    private void updateNodeByIndex(final Node updatedNode, int index) {
        nodes.set(index, updatedNode);
    }

    public String nodesToJson() throws IOException {
        return JsonHelper.toJson(nodes);
    }

    public void updateGetFollowing(final URL cloudUrl) {
        for (int i = 0; i < nodes.size(); i++) {
            final List<URL> followUrls = getFollowerUrls(cloudUrl, i);
            final Node updatedNode = nodes
                .get(i)
                .toBuilder()
                .requestedToFollow(followUrls)
                .build();

            this.updateNodeByIndex(updatedNode, i);
        }
    }

    private List<URL> getFollowerUrls(final URL cloudUrl) {
        return getFollowerUrls(cloudUrl, getNodeUrls().size());
    }

    private List<URL> getFollowerUrls(final URL cloudUrl, int nodeIndex) {
        final List<URL> followUrls = new ArrayList<>();

        if (nodeIndex == 0) {
            followUrls.add(cloudUrl);
        } else {
            int parentNodeIndex = ((nodeIndex + 1) / NUMBER_OF_CHILDREN_PER_NODE) - 1;
            followUrls.add(nodes.get(parentNodeIndex).getLocalUrl());
            followUrls.addAll(nodes.get(parentNodeIndex).getRequestedToFollow());
        }

        return followUrls;
    }

    public void markNodesOfflineIfNotSeenSince(final ZonedDateTime threshold) {
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            if (node.getLastSeen().compareTo(threshold) < 0) {
                updateNodeByIndex(node.toBuilder().status(OFFLINE).build(), i);
            }
        }
    }

    public void sortOfflineNodes(final URL cloudUrl) {
        nodes.sort(this::comparingStatus);
        updateGetFollowing(cloudUrl);
    }

    private int comparingStatus(Node node1, Node node2) {
        if (isOffline(node1) && !isOffline(node2)) {
            return 1;
        } else if (!isOffline(node1) && isOffline(node2)) {
            return -1;
        } else {
            return 0;
        }
    }

    private boolean isOffline(Node node) {
        return node.getStatus() == OFFLINE;
    }
}