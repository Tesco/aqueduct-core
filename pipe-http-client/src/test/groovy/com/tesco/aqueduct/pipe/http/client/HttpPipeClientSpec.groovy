package com.tesco.aqueduct.pipe.http.client

import com.tesco.aqueduct.pipe.api.*
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import io.micronaut.http.HttpResponse
import io.micronaut.http.simple.SimpleHttpResponse
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime

class HttpPipeClientSpec extends Specification {

    InternalHttpPipeClient internalClient = Mock()
    HttpPipeClient client = new HttpPipeClient(internalClient, new BrotliCodec(4, false), 240)

    static def responseBody = """[
            {
                "type": "type",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""

    def "a read from the implemented interface method returns a result with the retry after and messages"() {
        given:
        def response = """[
            {
                "type": "type",
                "key": "x",
                "contentType": "ct",
                "offset": 10,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""

        and: "call returns a http response with retry after header"
        HttpResponse<byte[]> httpResponse = new SimpleHttpResponse()
        httpResponse.body(response.bytes)
        httpResponse.headers.set(HttpHeaders.RETRY_AFTER, retry)
        httpResponse.headers.set(HttpHeaders.RETRY_AFTER_MS, retryMs)
        httpResponse.headers.set(HttpHeaders.GLOBAL_LATEST_OFFSET, String.valueOf(0L))
        httpResponse.headers.set(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.name())

        internalClient.httpRead(_ as List, _ as Long, _ as String) >> httpResponse

        when: "we call read and get defined response back"
        def results = client.read([], 0, "locationUuid")

        then: "we parse the retry after if its correct or return 0 otherwise"
        results.messages.size() == 1
        results.retryAfterMs == result

        where:
        retry | retryMs | result
        ""    | ""      | 240
        null  | null    | 240
        "5"   | null    | 5000
        null  | "5000"  | 5000
        "6"   | "5000"  | 5000
        "-5"  | "-5000" | 240
        "foo" | "bar"   | 240
        "foo" | null    | 240
        "0"   | "0"     | 0
        "0"   | null    | 0
        null  | "0"     | 0
    }

    def "if global offset is available in the header, it should be returned in MessageResults"() {
        def responseBody = """[
            {
                "type": "type",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""

        given: "call returns a http response with global offset header"
        HttpResponse<byte[]> httpResponse = new SimpleHttpResponse()
        httpResponse.body(responseBody.bytes)
        httpResponse.headers.set(HttpHeaders.RETRY_AFTER, "1")
        httpResponse.headers.set(HttpHeaders.GLOBAL_LATEST_OFFSET, "100")
        httpResponse.headers.set(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.name())

        internalClient.httpRead(_ as List, _ as Long, _ as String) >> httpResponse

        when: "we call read"
        def messageResults = client.read([], 0, "locationUuid")

        then: "global latest offset header is set correctly in the result"
        messageResults.globalLatestOffset == OptionalLong.of(100)
    }

    def "if pipe state is available in the header and is true, then results should report pipe state as up to date"() {
        def responseBody = """[
            {
                "type": "type",
                "key": "x",
                "contentType": "ct",
                "offset": 100,
                "created": "2018-10-01T13:45:00Z", 
                "data": "{ \\"valid\\": \\"json\\" }"
            }
        ]"""

        given: "call returns a http response with global offset header"
        HttpResponse<byte[]> httpResponse = new SimpleHttpResponse()
        httpResponse.body(responseBody.bytes)
        httpResponse.headers.set(HttpHeaders.RETRY_AFTER, "1")
        httpResponse.headers.set(HttpHeaders.GLOBAL_LATEST_OFFSET, "100")
        httpResponse.headers.set(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.name())

        internalClient.httpRead(_ as List, _ as Long, _ as String) >> httpResponse

        when: "we call read"
        MessageResults messageResults = client.read([], 0, "locationUuid")

        then: "pipe state is set correctly in the result"
        messageResults.pipeState == PipeState.UP_TO_DATE

        and: "internal client is invoked to fetch pipe state from parent"
        0 * internalClient.getPipeState(_ as List)

    }

    @Unroll
    def "response is decoded correctly as per given content encoding header #content_encoding"() {
        given: "call returns a http encoded response"
        HttpResponse<byte[]> httpResponse = new SimpleHttpResponse()
        httpResponse.body(responseBytes)
        httpResponse.headers.set(HttpHeaders.RETRY_AFTER, "1")
        httpResponse.headers.set(HttpHeaders.GLOBAL_LATEST_OFFSET, "100")
        httpResponse.headers.set(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.name())
        httpResponse.headers.set(HttpHeaders.X_CONTENT_ENCODING, content_encoding)

        internalClient.httpRead(_ as List, _ as Long, _ as String) >> httpResponse

        when: "we call read"
        MessageResults messageResults = client.read([], 0, "locationUuid")

        then: "message is set correctly in the result"
        messageResults.messages.size() == 1
        messageResults.messages[0] == new Message("type","x", "ct", 100, ZonedDateTime.parse("2018-10-01T13:45:00Z"), "{ \"valid\": \"json\" }")

        where:
        responseBytes                                | content_encoding
        new BrotliCodec(4, false).encode(responseBody.bytes) | "br"
        responseBody.bytes                           | "gzip"
    }

    def "throws unsupported operation error when getOffset invoked"() {
        when:
        client.getOffset(OffsetName.GLOBAL_LATEST_OFFSET)

        then:
        thrown(UnsupportedOperationException)
    }
}
