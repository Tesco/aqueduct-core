package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.*
import com.tesco.aqueduct.pipe.storage.CentralInMemoryStorage
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
@Deprecated // Remove this test when /offset/latest and /pipe/state endpoints are removed
class PipeReadControllerIntegrationDeprecatedSpec extends Specification {
    public static final String SOME_CLUSTER = "someCluster"

    @Shared CentralInMemoryStorage storage = new CentralInMemoryStorage(10, 600)
    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server
    private static PipeStateProvider pipeStateProvider
    private static LocationResolver locationResolver

    // overloads of settings for this test
    @Shared propertyOverloads = [
        "pipe.http.server.read.response-size-limit-in-bytes": "200"
    ]

    void setupSpec() {
        // There is nicer way in the works: https://github.com/micronaut-projects/micronaut-test
        // but it is not handling some basic things yet and is not promoted yet
        // Eventually this whole thing should be replaced with @MockBean(Reader) def provide(){ storage }
        pipeStateProvider = Mock(PipeStateProvider)
        locationResolver = Mock(LocationResolver) {
            resolve(_) >> [new Cluster("cluster_A"), new Cluster("cluster_B")]
        }

        // SetupSpec cannot be overridden within specific features, hence we had to mock the conditional behaviour here
        pipeStateProvider.getState(_ ,_) >> { args ->
            def type = args[0]
            return type.contains("OutOfDateType") ? new PipeStateResponse(false, 1000) : new PipeStateResponse(true, 1000)
        }

        context = ApplicationContext
            .build()
            .propertySources(PropertySource.of(propertyOverloads))
            .mainClass(EmbeddedServer)
            .build()

        context.registerSingleton(Reader, storage, Qualifiers.byName("local"))

        context.registerSingleton(pipeStateProvider)
        context.registerSingleton(locationResolver)
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
    def "Returns latest offset of #types"() {
        given:
        storage.write([
            Message("a", "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
            Message("b", "b", "contentType", 101, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"),
            Message("c", "c", "contentType", 102, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data")
        ])

        when:
        def request = RestAssured.get("/pipe/offset/latest?type=$types")

        then:
        request
            .then()
            .content(equalTo(offset))

        where:
        types   | offset
        "a"     | "100"
        "b"     | "101"
        "c"     | "102"
        "b,a"   | "101"
        "a,c"   | "102"
        "a,b,c" | "102"
    }

    def "Latest offset endpoint requires types"() {
        given:
        storage.write([
            new CentralInMemoryStorage.ClusteredMessage(Message("a", "a", "contentType", 100, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"), SOME_CLUSTER),
            new CentralInMemoryStorage.ClusteredMessage(Message("b", "b", "contentType", 101, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"), SOME_CLUSTER),
            new CentralInMemoryStorage.ClusteredMessage(Message("c", "c", "contentType", 102, ZonedDateTime.parse("2018-12-20T15:13:01Z"), "data"), SOME_CLUSTER)
        ])

        when:
        def request = RestAssured.get("/pipe/offset/latest")

        then:
        def response = """{"message":"Required QueryValue [type] not specified","path":"/type","_links":{"self":{"href":"/pipe/offset/latest","templated":false}}}"""
        request
            .then()
            .statusCode(400)
            .body(equalTo(response))
    }

    def "state endpoint returns result of state provider"() {
        given: "A pipe state provider mocked"

        when: "we call to get state"
        def request = RestAssured.get("/pipe/state")

        then: "response is serialised correctly"
        def response = """{"upToDate":true,"localOffset":"1000"}"""
        request
            .then()
            .statusCode(200)
            .body(equalTo(response))
    }
}