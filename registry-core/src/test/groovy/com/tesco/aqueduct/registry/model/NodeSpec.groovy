package com.tesco.aqueduct.registry.model

import com.tesco.aqueduct.pipe.api.PipeState
import spock.lang.Specification
import spock.lang.Unroll

class NodeSpec extends Specification {
    def "Node has id"() {
        given: "A node with a URL and no group"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .build()
        when: "Getting the node id"
        def result = testNode.getId()
        then: "the result is the localUrl"
        result == "http://test-url"
    }

    def "Node has id containing group"() {
        given: "A node with a URL and group"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .group("group-name")
            .build()
        when: "Getting the node id"
        def result = testNode.getId()
        then: "the result is a combination of group name and url"
        result == "group-name|http://test-url"
    }

    def "Can get Node host"() {
        given: "A node with a URL"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .build()
        when: "Getting the node host"
        def result = testNode.getHost()
        then: "the result is the host from the URL"
        result == "test-url"
    }

    def "Node version defaults to '0.0.0' when null"() {
        given: "Node constructed with null version"
        def testNode = Node.builder()
            .localUrl(new URL("http://test-url"))
            .pipe(["v":null])
            .build()
        when: "get the version"
        def version = testNode.getPipeVersion()

        then: "version is 0.0.0"
        version == "0.0.0"
    }

    @Unroll
    def "Can get pipeState"() {
        given: "A node with pipeState in pipe"
        def node = Node.builder().pipe(map).build()

        when: "get pipeState"
        PipeState result = node.getPipeState()

        then:
        result == expectedValue

        where:
        map                          | expectedValue
        ["pipeState": "UP_TO_DATE"]  | PipeState.UP_TO_DATE
        ["pipeState": "OUT_OF_DATE"] | PipeState.OUT_OF_DATE
        ["pipeState": "UNKNOWN"]     | PipeState.UNKNOWN
        ["":""]                      | PipeState.UNKNOWN
        ["pipeState": "gibberish"]   | PipeState.UNKNOWN
        ["v": "1.0"]                 | PipeState.UNKNOWN
    }
}
