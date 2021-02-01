package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.*
import io.micronaut.context.annotation.Property
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MicronautTest
import io.micronaut.test.annotation.MockBean
import io.restassured.RestAssured
import spock.lang.Specification

import javax.inject.Inject
import javax.inject.Named
import java.time.ZonedDateTime

import static java.util.OptionalLong.of
import static org.hamcrest.Matchers.equalTo

@Newify(Message)
@MicronautTest
@Property(name="micronaut.security.enabled", value="false")
@Property(name="rate-limiter.capacity", value = "1")
class PipeReadControllerBatchIntegrationSpec extends Specification {
    static final String DATA_BLOB = "some very big data blob with more than 200 bytes of size"
    static String type = "type1"

    @Inject @Named("local")
    Reader reader

    @Inject
    LocationService locationResolver

    @Inject
    EmbeddedServer server

    void setup() {
        RestAssured.port = server.port
        locationResolver.getClusterUuids(_) >> ["cluster1"]
    }

    void cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    @Property(name="pipe.http.server.read.response-size-limit-in-bytes", value="409")
    @Property(name="compression.threshold-in-bytes", value="1024")
    void "A batch of messages that equals the payload size is still transported"() {
        given:
        def messages = [
            Message(type, "a", "contentType", 100, ZonedDateTime.now(), DATA_BLOB),
            Message(type, "b", "contentType", 101, ZonedDateTime.now(), DATA_BLOB),
            Message(type, "c", "contentType", 102, ZonedDateTime.now(), DATA_BLOB)
        ]

        and:
        reader.read(_ as List, 100, _ as String) >> {
            new MessageResults(messages, 0, of(102), PipeState.UP_TO_DATE)
        }

        when:
        def request = RestAssured.get("/pipe/100?location=someLocation")

        then:
        request
            .then()
            .statusCode(200)
            .body("size", equalTo(3))
            .body("[0].offset", equalTo("100"))
            .body("[0].key", equalTo("a"))
            .body("[0].data", equalTo(DATA_BLOB))
            .body("[1].offset", equalTo("101"))
            .body("[1].key", equalTo("b"))
            .body("[1].data", equalTo(DATA_BLOB))
            .body("[2].offset", equalTo("102"))
            .body("[2].key", equalTo("c"))
            .body("[2].data", equalTo(DATA_BLOB))
    }

    @MockBean(Reader)
    @Named("local")
    Reader reader() {
        Mock(Reader)
    }

    @MockBean(LocationService)
    LocationService locationResolver() {
        Mock(LocationService)
    }
}