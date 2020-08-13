package com.tesco.aqueduct.registry.model;

import lombok.EqualsAndHashCode;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tesco.aqueduct.registry.model.Status.OFFLINE;
import static java.util.Comparator.comparing;

@EqualsAndHashCode
public class SubNodeGroup {

    private static final int NUMBER_OF_CHILDREN_PER_NODE = 2;

    public final List<Node> nodes;

    public final String subGroupId; //this is version for now

    public SubNodeGroup(String subGroupId) {
        this.nodes = new ArrayList<>();
        this.subGroupId = subGroupId;
    }

    public boolean isFor(Node node) {
        return node.getSubGroupId().equals(subGroupId);
    }

    public Node add(Node node, URL cloudUrl) {
        final List<URL> followUrls = calculateFollowerUrls(cloudUrl, getNodeUrls().size());
        final Node newNode = node.buildWith(followUrls);
        nodes.add(newNode);
        return newNode;
    }

    public Node add(Node newNode) {
        nodes.add(newNode);
        return newNode;
    }

    private List<URL> getNodeUrls() {
        return nodes.stream()
            .map(Node::getLocalUrl)
            .collect(Collectors.toList());
    }

    private List<URL> calculateFollowerUrls(final URL cloudUrl, int nodeIndex) {
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

    public void updateGetFollowing(URL cloudUrl) {
        IntStream.range(0, nodes.size())
            .forEach(i -> {
                final Node updatedNode = nodes
                    .get(i)
                    .toBuilder()
                    .requestedToFollow(calculateFollowerUrls(cloudUrl, i))
                    .build();

                this.updateNodeByIndex(updatedNode, i);
        });
    }

    private void updateNodeByIndex(final Node updatedNode, int index) {
        nodes.set(index, updatedNode);
    }

    public void markNodesOfflineIfNotSeenSince(ZonedDateTime threshold) {
        IntStream.range(0, nodes.size())
            .filter(i -> nodes.get(i).getLastSeen().compareTo(threshold) < 0)
            .forEach(i -> updateNodeByIndex(nodes.get(i).toBuilder().status(OFFLINE).build(), i));
    }

    public Optional<Node> getByHost(String host) {
        return nodes
            .stream()
            .filter(n -> n.getHost().equals(host))
            .findAny();
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public boolean removeByHost(String host) {
        return nodes.removeIf(node -> node.getHost().equals(host));
    }

    public Node get(int index) {
        return nodes.get(index);
    }

    public void sortNodes(URL cloudUrl) {
        nodes.sort(comparing(Node::getPipeState));
        nodes.sort(this::comparingOfflineStatus);
        updateGetFollowing(cloudUrl);
    }

    private int comparingOfflineStatus(Node node1, Node node2) {
        if (node1.isOffline() && !node2.isOffline()) {
            return 1;
        } else if (!node1.isOffline() && node2.isOffline()) {
            return -1;
        } else {
            return 0;
        }
    }

    public Optional<Node> findAndUpdate(Node nodeToRegister) {
        for (int i=0; i<nodes.size(); i++) {
            if(nodes.get(i).getHost().equals(nodeToRegister.getHost())) {
                Node updatedNode = nodeToRegister.buildWith(nodes.get(i).getRequestedToFollow());
                nodes.set(i, updatedNode);
                return Optional.ofNullable(updatedNode);
            }
        }

        return Optional.empty();
    }
}
