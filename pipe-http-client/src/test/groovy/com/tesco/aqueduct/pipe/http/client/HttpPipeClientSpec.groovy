package com.tesco.aqueduct.pipe.http.client

import com.tesco.aqueduct.pipe.api.*
import io.micronaut.http.HttpResponse
import spock.lang.Specification

class HttpPipeClientSpec extends Specification {

    InternalHttpPipeClient internalClient = Mock()
    HttpPipeClient client = new HttpPipeClient(internalClient)

    def "a read from the implemented interface method returns a result with the retry after and messages"() {
        given: "call returns a http response with retry after header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> retry
        response.header(HttpHeaders.GLOBAL_LATEST_OFFSET) >> 0L
        response.header(HttpHeaders.PIPE_STATE) >> PipeState.UP_TO_DATE
        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response

        when: "we call read and get defined response back"
        def results = client.read([], 0, ["locationUuid"])

        then: "we parse the retry after if its correct or return 0 otherwise"
        results.messages.size() == 1
        results.retryAfterSeconds == result

        where:
        retry | result
        ""    | 0
        null  | 0
        "5"   | 5
        "-5"  | 0
        "foo" | 0
    }

    def "if global offset is available in the header, it should be returned in MessageResults"() {
        given: "call returns a http response with global offset header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> 1
        response.header(HttpHeaders.GLOBAL_LATEST_OFFSET) >> "100"
        response.header(HttpHeaders.PIPE_STATE) >> PipeState.UP_TO_DATE
        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response

        when: "we call read"
        def messageResults = client.read([], 0, ["locationUuid"])

        then: "global latest offset header is set correctly in the result"
        messageResults.globalLatestOffset == OptionalLong.of(100)
    }

    def "if pipe state is available in the header and is true, then results should report pipe state as up to date"() {
        given: "call returns a http response with global offset header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> 1
        response.header(HttpHeaders.GLOBAL_LATEST_OFFSET) >> "100"
        response.header(HttpHeaders.PIPE_STATE) >> PipeState.UP_TO_DATE
        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response

        when: "we call read"
        MessageResults messageResults = client.read([], 0, ["locationUuid"])

        then: "pipe state is set correctly in the result"
        messageResults.pipeState == PipeState.UP_TO_DATE

        and: "internal client is invoked to fetch pipe state from parent"
        0 * internalClient.getPipeState(_ as List)

    }

    def "throws unsupported operation error when getOffset invoked"() {
        when:
        client.getOffset(OffsetName.GLOBAL_LATEST_OFFSET)

        then:
        thrown(UnsupportedOperationException)
    }

    def "throws IllegalArgumentException when locationUuids does not contain single value"() {
        when:
        client.read([], 0, locationUuids)

        then:
        thrown(IllegalArgumentException)

        where:
        locationUuids           | _
        ["uuid-1", "uuid-2"]    | _
        []                      | _
    }
}
