package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.HttpHeaders
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.pipe.api.PipeState
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.storage.CentralInMemoryStorage
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime

import static org.hamcrest.Matchers.equalTo

@Newify(Message)
class PipeReadControllerIntegrationSpec extends Specification {
    static final String DATA_BLOB = "aaaaaaaaaaaaabbbbbbbbbbbbcccccccccccccdddddddeeeeeeeee"
    static String type = "type1"
    static int RETRY_AFTER_SECONDS = 600

    @Shared InMemoryStorage storage = new CentralInMemoryStorage(10, RETRY_AFTER_SECONDS)
    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server
    private static PipeStateProvider pipeStateProvider

    // overloads of settings for this test
    @Shared propertyOverloads = [
        "pipe.http.server.read.response-size-limit-in-bytes": "200"
    ]

    void setupSpec() {
        // There is nicer way in the works: https://github.com/micronaut-projects/micronaut-test
        // but it is not handling some basic things yet and is not promoted yet
        // Eventually this whole thing should be replaced with @MockBean(MessageReader) def provide(){ storage }
        pipeStateProvider = Mock(PipeStateProvider)

        context = ApplicationContext
            .build()
            .propertySources(PropertySource.of(propertyOverloads))
            .mainClass(EmbeddedServer)
            .build()

        context.registerSingleton(MessageReader, storage, Qualifiers.byName("local"))

        // SetupSpec cannot be overridden within specific features, hence we had to mock the conditional behaviour here
        pipeStateProvider.getState(_ ,_) >> { args ->
            def type = args[0]
            return type.contains("OutOfDateType") ?
                    new PipeStateResponse(PipeState.OUT_OF_DATE, 1000) :
                    new PipeStateResponse(PipeState.UP_TO_DATE, 1000)
        }

        context.registerSingleton(pipeStateProvider)
        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void setup() {
        storage.clear()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    @Unroll
    void "Test empty responses has Retry-After of #retryAfter seconds - #requestPath"() {
        given: "empty storage"

        when:
        def request = RestAssured.get(requestPath)

        then:
        request
            .then()
            .statusCode(statusCode)
            .content(equalTo(response))
            .header(HttpHeaders.RETRY_AFTER, "" + RETRY_AFTER_SECONDS)

        where:
        requestPath | statusCode | retryAfter          | response
        "/pipe/0"   | 200        | RETRY_AFTER_SECONDS | "[]"
        "/pipe/1"   | 200        | RETRY_AFTER_SECONDS | "[]"
    }

    @Unroll
    void "Test bad requests do not have Retry-After header - #requestPath"() {
        given: "empty storage"

        when:
        def request = RestAssured.get(requestPath)

        then:
        request
            .then()
            .statusCode(statusCode)

        request.getHeader(HttpHeaders.RETRY_AFTER) == null

        where:
        requestPath    | statusCode
        "/pipe/-1"     | 400
        "/pipe/type"   | 400
        "/pipe"        | 404
        "/pipe/"       | 404
        "/piipe/0"     | 404
        "/pipe/type/0" | 404
        "/"            | 404
        ""             | 404
    }

    @Unroll
    void "Check responses has correct payload and that Retry-After header has a value of 0 - #requestPath"() {
        given:
        storage.write(Message(type, "a", "ct", 100, null, null))

        when:
        def request = RestAssured.get(requestPath)
        then:

        String expectedResponse = """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        request
            .then()
            .statusCode(statusCode)
            .content(equalTo(expectedResponse))
            .header(HttpHeaders.RETRY_AFTER, "0")

        where:
        requestPath | statusCode | response
        "/pipe/0"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        "/pipe/1"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
    }

    @Unroll
    void "non empty response returns first available element - #requestPath"() {
        given:
        storage.write(Message(type, "a", "ct", 100, null, null))

        when:
        def request = RestAssured.get(requestPath)

        then:
        request
            .then()
            .statusCode(statusCode)
            .body(equalTo((String) response))

        where:
        requestPath | statusCode | response
        "/pipe/0"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        "/pipe/1"   | 200        | """[{"type":"$type","key":"a","contentType":"ct","offset":"100"}]"""
        "/pipe/101" | 200        | '[]'
    }

    @Unroll
    void "filtering by type: #types"() {
        given:
        storage.write([
            Message("type1", "a", "ct", 100, null, null),
            Message("type2", "b", "ct", 101, null, null)
        ])

        when:
        def request = RestAssured.get("/pipe/0?type=$types")

        then:
        request
            .then()
            .statusCode(statusCode)
            .body(equalTo(response))

        where:
        types               | statusCode | response
        "type1"             | 200        | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"}]'
        "type2"             | 200        | '[{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
        "type3"             | 200        | '[]'
        "type1,type2"       | 200        | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"},{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
        "type1,type2,type3" | 200        | '[{"type":"type1","key":"a","contentType":"ct","offset":"100"},{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
    }

    @Unroll
    void "pipe signals next offset despite messages not routed"() {
        given:
        storage.write([
            Message("type2", "b", "ct", 101, null, null)
        ])

        when:
        def request = RestAssured.get("/pipe/0?type=$type")

        then:
        request
            .then()
            .statusCode(statusCode)
            .header(headerName, headerValue)
            .body(equalTo(response))

        where:
        type    | statusCode    | headerName                              | headerValue           | response
        'type1' |  200          | HttpHeaders.GLOBAL_LATEST_OFFSET        | '101'                 | '[]'
        'type2' |  200          | HttpHeaders.GLOBAL_LATEST_OFFSET        | '101'                 | '[{"type":"type2","key":"b","contentType":"ct","offset":"101"}]'
    }

    @Unroll
    void "pipe signals pipe state in response header"() {
        given:
        pipeStateProvider.getState(["$type"], _) >> new PipeStateResponse(isPipeUpToDate, 1)

        when:
        def request = RestAssured.get("/pipe/0?type=$type")

        then:
        request
            .then()
            .statusCode(200)
            .header(HttpHeaders.PIPE_STATE, headerValue)

        where:
        type           | isPipeUpToDate                  | headerValue
        'type1'        | PipeState.UP_TO_DATE            | "UP_TO_DATE"
        'OutOfDateType'| PipeState.OUT_OF_DATE           | "OUT_OF_DATE"
    }

    @Unroll
    void "the header does not contain Global-Latest-Offset when no global latest offset is stored"() {
        given:

        when:
        def response = RestAssured.get("/pipe/0?type=type1")

        then:
        response
            .then()
            .statusCode(200)
            .body(equalTo('[]'))

        response.getHeader(HttpHeaders.GLOBAL_LATEST_OFFSET) == null
    }

    @Unroll
    void "responds with messages - #requestPath"() {
        given:
        storage.write(
            Message(type, "a", "contentType", 100, null, data)
        )

        when:
        def request = RestAssured.get(requestPath)

        then:
        request
            .then()
            .statusCode(statusCode)
            .body("[0].offset", equalTo(offset))
            .body("[0].key", equalTo(key))
            .body("[0].data", equalTo(data))

        where:
        requestPath | statusCode | offset | key | data
        "/pipe/0"   | 200        | "100"  | "a" | '{"x":"1"}'
        "/pipe/100" | 200        | "100"  | "a" | '{"x":"1"}'
    }

    void "A single message that is over the payload size is still transported"() {
        given:
        Message message1 = Message(type, "a", "contentType", 100, null, DATA_BLOB)
        storage.write(message1)

        when:
        def response = RestAssured.get("/pipe/100")

        then:
        response
            .then()
            .statusCode(200)
            .body("[0].offset", equalTo("100"))
            .body("[0].key", equalTo("a"))
            .body("[0].contentType", equalTo("contentType"))
            .body("[0].data", equalTo(DATA_BLOB))
            .body("size", equalTo(1))
    }

    def "assert response schema"() {
        given:
        storage.write([
            Message(type, "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
            Message(type, "b", null, 101, null, null)
        ])

        when:
        def request = RestAssured.get("/pipe/100")

        then:
        request
            .then()
            .content(equalTo("""
            [
                {"type":"type1","key":"a","contentType":"contentType","offset":"100","created":"2018-12-20T15:13:01Z","data":"data"},
                {"type":"type1","key":"b","offset":"101"}
            ]
            """.replaceAll("\\s", "")))
    }

    @Unroll
    def "Returns latest offset of #types"() {
        given:
        storage.write([
            Message("a", "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
            Message("b", "b", "contentType", 101, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
            Message("c", "c", "contentType", 102, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
        ])

        when:
        def request = RestAssured.get("/pipe/offset/latest?type=$types")

        then:
        request
            .then()
            .content(equalTo(offset))

        where:
        types   | offset
        "a"     | '"100"'
        "b"     | '"101"'
        "c"     | '"102"'
        "b,a"   | '"101"'
        "a,c"   | '"102"'
        "a,b,c" | '"102"'
    }

    def "Latest offset endpoint requires types"() {
        given:
        storage.write([
                Message("a", "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
                Message("b", "b", "contentType", 101, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
                Message("c", "c", "contentType", 102, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
        ])

        when:
        def request = RestAssured.get("/pipe/offset/latest")

        then:
        def response = """{"message":"Required QueryValue [List type] not specified","path":"/type","_links":{"self":{"href":"/pipe/offset/latest","templated":false}}}"""
        request
            .then()
            .statusCode(400)
            .body(equalTo(response))
    }

    def "state endpoint returns result of state provider"() {
        given: "A pipe state provider mocked"

        when: "we call to get state"
        def request = RestAssured.get("/pipe/state?type=a")

        then: "response is serialised correctly"
        def response = """{"upToDate":"UP_TO_DATE","localOffset":"1000"}"""
        request
            .then()
            .statusCode(200)
            .body(equalTo(response))
    }
}