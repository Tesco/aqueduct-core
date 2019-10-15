package com.tesco.aqueduct.registry.model

import spock.lang.Specification

import java.time.ZonedDateTime

class NodeGroupSpec extends Specification {
    def "Group has node"() {
        given: "A Group with Nodes"
        def group = new NodeGroup([Mock(Node)])
        when: "checking the group has nodes"
        def result = group.isEmpty()
        then: "the result is false"
        !result
    }

    def "Group does not have nodes"() {
        given: "A Group with no Nodes"
        def group = new NodeGroup([])
        when: "checking the group has nodes"
        def result = group.isEmpty()
        then: "the result is true"
        result
    }

    def "A node can be removed from a node group given an id"() {
        given: "A node with an id"
        def node = Mock Node
        node.id >> "test_node_id"

        and: "A node with a different id"
        def anotherNode = Mock Node
        anotherNode.id >> "another_node_id"

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "Removing the node from the group using the id"
        def result = group.removeById("test_node_id")

        then: "the result is true (the node was found and deleted)"
        result

        and: "The group no longer contains the removed node"
        group.nodes == [anotherNode]
    }

    def "a node can be added to the group"() {
        given: "an empty node group"
        def group = new NodeGroup([])
        when: "a new node is added"
        def node = Node.builder().build()
        group.add(node, new URL("http://test-url"))
        then: "the node group is no longer empty"
        !group.isEmpty()
    }

    def "a node can be fetched from the group by index"() {
        given: "A node with an id"
        def node = Mock Node
        node.id >> "test_node_id"

        and: "A node with a different id"
        def anotherNode = Mock Node
        anotherNode.id >> "another_node_id"

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "fetching the node at index 0"
        def result = group.get(0)

        then: "the correct node is returned"
        result == node

        when: "fetching the node at index 1"
        result = group.get(1)

        then: "the correct node is returned"
        result == anotherNode
    }

    def "Fetch URLs for all nodes"() {
        given: "A node with a local url"
        def nodeUrl = new URL("http://test_node_1")
        def node = Mock Node
        node.localUrl >> nodeUrl

        and: "A node with a different id"
        def anotherNodeUrl = new URL("http://test_node_2")
        def anotherNode = Mock Node
        anotherNode.localUrl >> anotherNodeUrl

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "all Node urls have been fetched"
        def result = group.getNodeUrls()

        then: "Both URLs have been returned"
        result == [nodeUrl, anotherNodeUrl]
    }

    def "get node by id"() {
        given: "A node with an id"
        def node = Mock Node
        node.id >> "test_node_id"

        and: "A node with a different id"
        def anotherNode = Mock Node
        anotherNode.id >> "another_node_id"

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "fetching a node by id"
        def result = group.getById("test_node_id")

        then: "the correct node is returned"
        result == node

        when: "fetching the other node by id"
        result = group.getById("another_node_id")

        then: "the correct node is returned"
        result == anotherNode

        when: "fetching a node with an id that doesn't exist"
        result = group.getById("non_existent_id")

        then: "null is returned"
        result == null
    }

    def "A node can be updated in the Group"() {
        given: "A node with a local url"
        def nodeUrl = new URL("http://test_node_1")
        def node = Mock Node
        node.id >> "test_node_id"
        node.localUrl >> nodeUrl

        and: "A node with a different id"
        def anotherNode = Mock Node
        anotherNode.id >> "another_node_id"

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "An updated node is provided to the group"
        def updatedNode = Mock(Node)
        updatedNode.id >> "test_node_id"
        group.updateNode(updatedNode)

        then: "The group contains the updated node"
        group.nodes == [updatedNode, anotherNode]

        when: "Another updated node is provided to the group"
        def anotherUpdatedNode = Mock(Node)
        anotherUpdatedNode.id >> "another_node_id"
        group.updateNode(anotherUpdatedNode)

        then: "The group contains the updated nodes"
        group.nodes == [updatedNode, anotherUpdatedNode]
    }

    def "Nodes are correctly rebalanced"() {
        given: "a cloud url"
        URL cloudUrl = new URL("http://cloud")
        and: "a nodegroup with unbalanced Nodes"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .build()
        URL n3Url = new URL("http://node-3")
        Node n3 = Node.builder()
            .localUrl(n3Url)
            .build()
        NodeGroup group = new NodeGroup([n1, n2, n3])
        when: "the group is rebalanced"
        group.rebalance(cloudUrl)
        then: "the result is a balanced group"
        group.nodes.get(0).requestedToFollow == [cloudUrl]
        group.nodes.get(1).requestedToFollow == [n1Url, cloudUrl]
        group.nodes.get(2).requestedToFollow == [n1Url, cloudUrl]
    }

    def "NodeGroup nodes json format is correct"() {
        given: "a NodeGroup"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .build()
        NodeGroup group = new NodeGroup([n1, n2])
        when: "the NodeGroup nodes are output as JSON"
        String result = group.nodesToJson()
        then: "the JSON format is correct"
        result ==
            "[" +
                "{" +
                    "\"localUrl\":\"http://node-1\"," +
                    "\"offset\":\"0\"," +
                    "\"providerLastAckOffset\":\"0\"," +
                    "\"id\":\"http://node-1\"" +
                "}," +
                "{" +
                    "\"localUrl\":\"http://node-2\"," +
                    "\"offset\":\"0\"," +
                    "\"providerLastAckOffset\":\"0\"," +
                    "\"id\":\"http://node-2\"" +
                "}" +
            "]"
    }

    def "Nodes are correctly marked as offline"() {
        given: "A node group"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .lastSeen(ZonedDateTime.now())
            .status("online")
            .build()
        Node n2 = Node.builder()
            .localUrl(new URL("http://node-2"))
            .lastSeen(ZonedDateTime.now().minusDays(10))
            .status("online")
            .build()
        Node n3 = Node.builder()
            .localUrl(new URL("http://node-3"))
            .lastSeen(ZonedDateTime.now().minusDays(3))
            .status("online")
            .build()
        NodeGroup group = new NodeGroup([n1, n2, n3])
        when: "requesting nodes be marked offline"
        group.markNodesOfflineIfNotSeenSince(ZonedDateTime.now().minusDays(5))
        then: "Only nodes not seen since the threshold are marked offline"
        group.nodes.get(0).status == "online"
        group.nodes.get(1).status == "offline"
        group.nodes.get(0).status == "online"
    }
}
