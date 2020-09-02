package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.HttpHeaders
import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.storage.DistributedInMemoryStorage
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

import static org.hamcrest.Matchers.equalTo

@Newify(Message)
class DistributedStoragePipeReadControllerIntegrationSpec extends Specification {
    static int RETRY_AFTER_SECONDS = 600

    @Shared InMemoryStorage storage = new DistributedInMemoryStorage(10, RETRY_AFTER_SECONDS)
    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server
    @Shared PipeStateProvider pipeStateProvider = Mock()
    @Shared LocationResolver locationResolver = Mock()

    // overloads of settings for this test
    @Shared propertyOverloads = [
        "pipe.http.server.read.response-size-limit-in-bytes": "200",
        "micronaut.security.enabled": "false"
    ]

    void setupSpec() {
        // There is nicer way in the works: https://github.com/micronaut-projects/micronaut-test
        // but it is not handling some basic things yet and is not promoted yet
        // Eventually this whole thing should be replaced with @MockBean(Reader) def provide(){ storage }

        context = ApplicationContext
            .build()
            .propertySources(PropertySource.of(propertyOverloads))
            .mainClass(EmbeddedServer)
            .build()

        context.registerSingleton(Reader, storage, Qualifiers.byName("local"))

        pipeStateProvider.getState(_ ,_) >> new PipeStateResponse(true, 1000)

        locationResolver.resolve(_) >> ["clusterId"]

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
    void "the header does not contain Global-Latest-Offset when no global latest offset is stored"() {
        given:
        storage.write(
            Message("type1", "b", "ct", 101, null, null)
        )

        when:
        def response = RestAssured.get("/pipe/0?type=type1&location=someLocation")

        then:
        response
            .then()
            .statusCode(200)
            .body(equalTo('[{"type":"type1","key":"b","contentType":"ct","offset":"101"}]'))

        response.getHeader(HttpHeaders.GLOBAL_LATEST_OFFSET) == null
    }
}