package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.Cluster
import com.tesco.aqueduct.pipe.api.JsonHelper
import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.Reader
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.storage.CentralInMemoryStorage
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.PropertySource
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import java.nio.charset.StandardCharsets

import static org.hamcrest.Matchers.equalTo

@Newify(Message)
class PipeReadControllerBatchIntegrationSpec extends Specification {
    static final String DATA_BLOB = "aaaaaaaaaaaaabbbbbbbbbbbbcccccccccccccdddddddeeeeeeeee"
    static String type = "type1"
    static int RETRY_AFTER_SECONDS = 600
    PipeStateProvider pipeStateProvider

    @Shared CentralInMemoryStorage storage = new CentralInMemoryStorage(10, RETRY_AFTER_SECONDS)
    @Shared ApplicationContext context
    @Shared LocationResolver locationResolver = Mock(LocationResolver)
    private Cluster cluster

    ApplicationContext setupContext(maxPayloadSize) {

        cluster = new Cluster("clusterId")
        locationResolver.resolve(_) >> [cluster]

        def context = ApplicationContext
            .build()
            .propertySources(PropertySource.of(["pipe.http.server.read.response-size-limit-in-bytes": maxPayloadSize]))
            .mainClass(EmbeddedServer)
            .build()

        context.registerSingleton(Reader, storage, Qualifiers.byName("local"))
        context.registerSingleton(pipeStateProvider)
        context.registerSingleton(locationResolver)
        context.start()

        EmbeddedServer server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port

        return context
    }

    void setup() {
        storage.clear()

        pipeStateProvider = Mock(PipeStateProvider) {
            getState(_ as List, _ as Reader) >> new PipeStateResponse(true, 100)
        }
    }

    void cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }


    void "A batch of messages that equals the payload size is still transported"() {
        given:
        def messages = [
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "a", "contentType", 100, null, DATA_BLOB), "clusterId"),
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "b", "contentType", 101, null, DATA_BLOB), "clusterId"),
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "c", "contentType", 102, null, DATA_BLOB), "clusterId")
        ]

        def batchInJson = JsonHelper.toJson(messages)
        def batchSize = batchInJson.getBytes(StandardCharsets.UTF_8).length

        context = setupContext(batchSize)

        storage.write(messages)

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

    @Ignore
    void "A batch of messages that exceeds the payload size is truncated correctly"() {
        given:
        def messages = [
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "a", "contentType", 100, null, DATA_BLOB), "clusterId"),
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "b", "contentType", 101, null, DATA_BLOB), "clusterId"),
            new CentralInMemoryStorage.ClusteredMessage(Message(type, "c", "contentType", 102, null, DATA_BLOB), "clusterId")
        ]

        def batchJson = JsonHelper.toJson(messages)
        def batchSize = batchJson.getBytes(StandardCharsets.UTF_8).length

        int twoOutOfThree = batchSize * 0.8

        // ~0.67 would be around 2 messages, so third message does not fit
        context = setupContext(twoOutOfThree)

        storage.write(messages)

        when:
        def request = RestAssured.get("/pipe/100?location=someLocation")

        then:
        request
            .then()
            .statusCode(200)
            .body("size", equalTo(2))
            .body("[0].offset", equalTo("100"))
            .body("[0].key", equalTo("a"))
            .body("[0].data", equalTo(DATA_BLOB))
            .body("[1].offset", equalTo("101"))
            .body("[1].key", equalTo("b"))
            .body("[1].data", equalTo(DATA_BLOB))
    }
}