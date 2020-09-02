package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.*
import io.micronaut.context.annotation.Property
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MicronautTest
import io.micronaut.test.annotation.MockBean
import io.restassured.RestAssured
import spock.lang.Specification
import spock.lang.Unroll

import javax.inject.Inject
import javax.inject.Named
import java.time.ZonedDateTime

import static java.util.Arrays.asList
import static java.util.OptionalLong.of
import static org.hamcrest.Matchers.equalTo

@Newify(Message)
@MicronautTest
@Property(name="pipe.http.server.read.response-size-limit-in-bytes", value="200")
@Property(name="micronaut.security.enabled", value="false")
class PipeReadControllerIntegrationSpec extends Specification {

    @Inject @Named("local")
    Reader reader

    @Inject
    LocationResolver locationResolver

    @Inject
    PipeStateProvider pipeStateProvider

    @Inject
    EmbeddedServer server

    static int RETRY_AFTER_SECONDS = 600
    static String type = "type1"

    void setup() {
        RestAssured.port = server.port

        locationResolver.resolve(_) >> ["cluster1"]
        pipeStateProvider.getState(*_) >> new PipeStateResponse(true, 0)
    }
    @Unroll
    void "Test empty responses has Retry-After of #retryAfter seconds - #requestPath"() {
        given: "empty storage"
        reader.read(*_) >> new MessageResults([], retryAfter, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get(requestPath)

        then:
        response
            .then()
            .statusCode(statusCode)
            .content(equalTo(responseBody))
            .header(HttpHeaders.RETRY_AFTER, "" + RETRY_AFTER_SECONDS)

        where:
        requestPath                         | statusCode | retryAfter          | responseBody
        "/pipe/0?location='someLocation'"   | 200        | RETRY_AFTER_SECONDS | "[]"
        "/pipe/1?location='someLocation'"   | 200        | RETRY_AFTER_SECONDS | "[]"
    }

    @Unroll
    void "Test bad requests do not have Retry-After header - #requestPath"() {
        given: "empty storage"
        reader.read(*_) >> new MessageResults([], 0, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get(requestPath)

        then:
        response
            .then()
            .statusCode(statusCode)

        response.getHeader(HttpHeaders.RETRY_AFTER) == null

        where:
        requestPath                             | statusCode
        "/pipe/-1?location='someLocation'"      | 400
        "/pipe/type?location='someLocation'"    | 400
        "/pipe?location='someLocation'"         | 404
        "/pipe/?location='someLocation'"        | 404
        "/piipe/0?location='someLocation'"      | 404
        "/pipe/type/0?location='someLocation'"  | 404
        "/"                                     | 404
        ""                                      | 404
    }

    @Unroll
    void "Check responses has correct payload and that Retry-After header has a value of 0 - #requestPath"() {
        given:
        reader.read(*_) >> new MessageResults(
            [Message(type, "a", "ct", 100, null, null)], 0, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get(requestPath)

        then:
        String expectedResponseBody = """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        response
            .then()
            .statusCode(statusCode)
            .content(equalTo(expectedResponseBody))
            .header(HttpHeaders.RETRY_AFTER, "0")

        where:
        requestPath                          | statusCode
        "/pipe/0?location='someLocation'"    | 200
        "/pipe/1?location='someLocation'"    | 200
    }

    @Unroll
    void "non empty response returns first available element - #requestPath"() {
        given:
        reader.read(_ as List, 0, _ as List) >> new MessageResults(
            [Message(type, "a", "ct", 100, null, null)], 0, of(0), PipeState.UP_TO_DATE)

        reader.read(_ as List, 1, _ as List) >> new MessageResults(
            [Message(type, "a", "ct", 100, null, null)], 0, of(0), PipeState.UP_TO_DATE)

        reader.read(_ as List, 101, _ as List) >> new MessageResults([], 0, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get(requestPath)

        then:
        response
            .then()
            .statusCode(statusCode)
            .body(equalTo((String) responseBody))

        where:
        requestPath                       | statusCode | responseBody
        "/pipe/0?location=someLocation"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        "/pipe/1?location=someLocation"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        "/pipe/101?location=someLocation" | 200        | '[]'
    }

    @Unroll
    void "filtering by type: #types"() {
        given:
        def typeList = asList(types.split(","))
        def offset = of(messages.isEmpty() ? 0 : messages.last().offset)
        reader.read(typeList, 0, _ as List) >> new MessageResults(messages, 0, offset, PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/0?type=$types&location=someLocation")

        then:
        response
            .then()
            .statusCode(statusCode)
            .body(equalTo(responseBody))

        where:
        types               | statusCode | messages                                                                                     | responseBody
        "type1"             | 200        | [Message("type1", "a", "ct", 100, null, null)]                                               | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"}]'
        "type2"             | 200        | [Message("type2", "b", "ct", 101, null, null)]                                               | '[{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
        "type3"             | 200        | []                                                                                           | '[]'
        "type1,type2"       | 200        | [Message("type1", "a", "ct", 100, null, null), Message("type2", "b", "ct", 101, null, null)] | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"},{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
        "type1,type2,type3" | 200        | [Message("type1", "a", "ct", 100, null, null), Message("type2", "b", "ct", 101, null, null)] | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"},{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
    }

    @Unroll
    void "filtering by location and type: #query"() {
        given:
        reader.read(["type1"], 0, _ as List) >> new MessageResults(
            [Message("type1", "a", "ct", 100, null, null)], 0, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/0$query")

        then:
        response
            .then()
            .statusCode(statusCode)
            .body(equalTo(responseBody))

        where:
        query                           | statusCode | responseBody
        ""                              | 400        | ''
        "?type=type1"                   | 400        | ''
        "?type=type1&location="         | 400        | ''
        "?type=type1&location"          | 400        | ''
        "?type=type1&location=1234"     | 200        | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"}]'
    }

    @Unroll
    void "pipe signals next offset despite messages not routed"() {
        given:
        reader.read(["type1"], 0, _ as List) >> new MessageResults([], 0, of(headerValue), PipeState.UP_TO_DATE)

        reader.read(["type2"], 0, _ as List) >> new MessageResults(
            [Message("type2", "b", "ct", headerValue, null, null)], 0, of(headerValue), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/0?type=$type&location=someLocation")

        then:
        response
            .then()
            .statusCode(statusCode)
            .header(headerName, headerValue.toString())
            .body(equalTo(responseBody))

        where:
        type    | statusCode    | headerName                              | headerValue         | responseBody
        'type1' |  200          | HttpHeaders.GLOBAL_LATEST_OFFSET        | 101                 | '[]'
        'type2' |  200          | HttpHeaders.GLOBAL_LATEST_OFFSET        | 101                 | '[{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
    }

    void "pipe signals pipe state in response header"() {
        given:
        reader.read(["type1"], 0, _ as List) >> new MessageResults([], 0, of(0), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/0?type=type1&location=someLocation")

        then:
        response
            .then()
            .statusCode(200)
            .header(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.toString())
    }

    @Unroll
    void "the header does not contain Global-Latest-Offset when no global latest offset is stored"() {
        given: "no global offset from storage"
        reader.read(["type1"], 0, _ as List) >> new MessageResults([], 0, OptionalLong.empty(), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/0?type=type1&location=someLocation")

        then:
        response
            .then()
            .statusCode(200)
            .body(equalTo('[]'))

        response.getHeader(HttpHeaders.GLOBAL_LATEST_OFFSET) == null
    }

    void "A single message that is over the payload size is still transported"() {
        def dataBlob = "some very big data blob with more than 200 bytes of size"
        given:
        reader.read([], 100, _ as List) >> new MessageResults(
            [Message(null, "a", "contentType", 100, null, dataBlob)],
            0,
            OptionalLong.empty(),
            PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/100?location=someLocation")

        then:
        response
            .then()
            .statusCode(200)
            .body("[0].offset", equalTo("100"))
            .body("[0].key", equalTo("a"))
            .body("[0].contentType", equalTo("contentType"))
            .body("[0].data", equalTo(dataBlob))
            .body("size", equalTo(1))
    }

    def "assert response schema"() {
        given:
        reader.read([], 100, _ as List) >> new MessageResults(
                [Message(type, "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
                 Message(type, "b", null, 101, null, null)],
                0, OptionalLong.empty(), PipeState.UP_TO_DATE)

        when:
        def response = RestAssured.get("/pipe/100?location=someLocation")

        then:
        response
            .then()
            .content(equalTo("""
                [
                    {"type":"type1","key":"a","contentType":"contentType","offset":"100","created":"2018-12-20T15:13:01Z","data":"data"},
                    {"type":"type1","key":"b","offset":"101"}
                ]
            """.replaceAll("\\s", "")))
    }

    @MockBean(Reader)
    @Named("local")
    Reader reader() {
        Mock(Reader)
    }

    @MockBean(PipeStateProvider)
    PipeStateProvider pipeStateProvider() {
        Mock(PipeStateProvider)
    }

    @MockBean(LocationResolver)
    LocationResolver locationResolver() {
        Mock(LocationResolver)
    }
}