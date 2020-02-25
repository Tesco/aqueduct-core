package com.tesco.aqueduct.pipe.http.client

import com.tesco.aqueduct.pipe.api.HttpHeaders
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageResults
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.PipeState
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import io.micronaut.cache.CacheManager
import io.micronaut.cache.SyncCache
import io.micronaut.http.HttpResponse
import spock.lang.Specification

class HttpPipeClientSpec extends Specification {

    InternalHttpPipeClient internalClient = Mock()
    CacheManager cacheManager = Mock() {
        getCache(_) >> Mock(SyncCache)
    }
    HttpPipeClient client = new HttpPipeClient(internalClient, cacheManager)

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

    // Ensure backwards compatible, need to update to throw error once all tills have latest software
    def "if no global offset is available in the header, call getLatestOffsetMatching"() {
        given: "call returns a http response with retry after header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> 1
        response.header(HttpHeaders.PIPE_STATE) >> PipeState.UP_TO_DATE
        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response

        when: "we call read"
        client.read([], 0, ["locationUuid"])

        then: "getLatestOffsetMatching is called"
        1 * internalClient.getLatestOffsetMatching(_)
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

    // Ensure backwards compatible, need to update to throw error or default once all tills have latest software
    def "if no pipe state is available in the header then fetch it from get pipe state endpoint"() {
        given: "call returns a http response with retry after header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> 1
        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response

        when: "we call read"
        def messageResults = client.read([], 0, ["locationUuid"])

        then: "internal client is invoked to fetch pipe state from parent"
        1 * internalClient.getPipeState(_ as List) >> new PipeStateResponse(returnedPipeState, 100L)

        and: "pipe state is up to date in the result"
        messageResults.pipeState == expectedPipeState

        where:
        returnedPipeState | expectedPipeState
        true              | PipeState.UP_TO_DATE
        false             | PipeState.OUT_OF_DATE
    }

    def "an exception is thrown when getLatestOffsetMatching fails"() {
        given: "call returns a http response with retry after header"
        HttpResponse<List<Message>> response = Mock()
        response.body() >> [Mock(Message)]
        response.header(HttpHeaders.RETRY_AFTER) >> 1

        internalClient.httpRead(_ as List, _ as Long, _ as String) >> response
        internalClient.getLatestOffsetMatching(_ as List) >> { throw new Exception() }

        when: "we call read"
        client.read([], 0, ["locationUuid"])

        then: "an exception is thrown"
        thrown Exception
    }

    def "allows to get latest offset"() {
        given:
        internalClient.getLatestOffsetMatching(types) >> offset

        when:
        def response = client.getLatestOffsetMatching(types)

        then:
        response == offset

        where:
        types  | offset
        ["x"]  | 1
        []     | 2
    }

    def "allows to get pipe status"() {
        given:
        def offset = 1
        def types = []
        def pipeState = new PipeStateResponse(true, offset)
        internalClient.getPipeState(types) >> pipeState

        when: "getting pipe state"
        def response = client.getPipeStateResponse(types)

        then: "the response is as expected"
        response == pipeState
    }

    def "invalidates cache if pipestatus is not up to date"() {
        given:
        def offset = 1
        def types = []
        def pipeState = new PipeStateResponse(false, offset)
        internalClient.getPipeState(types) >> pipeState
        cacheManager.getCache("health-check") >> Mock(SyncCache)

        when: "getting pipe state"
        def response = client.getPipeStateResponse(types)

        then: "the response is as expected"
        response == pipeState

        and: "the cache has been invalidated"
        1* cacheManager.getCache("health-check").invalidateAll()
    }

    def "throws unsupported operation error when getOffset invoked"() {
        when:
        client.getOffset(OffsetName.GLOBAL_LATEST_OFFSET)

        then:
        thrown(UnsupportedOperationException)
    }
}
