package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static org.hamcrest.Matchers.equalTo

@Newify(Message)
class PipeReadAuthenticationProviderIntegrationSpec extends Specification {

    static final int RETRY_AFTER_SECONDS = 600
    static final String USERNAME = "username"
    static final String PASSWORD = "password"

    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

    static InMemoryStorage storage = new InMemoryStorage(10, RETRY_AFTER_SECONDS)

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    void setupSpec() {
        context = ApplicationContext
                .build()
                .properties(
                    "micronaut.security.enabled": true,
                    "authentication.read-pipe.username": USERNAME,
                    "authentication.read-pipe.password": PASSWORD,
                    "authentication.read-pipe.runscope-username": RUNSCOPE_USERNAME,
                    "authentication.read-pipe.runscope-password": RUNSCOPE_PASSWORD
                )
                .mainClass(PipeReadController)
                .build()

        context.registerSingleton(MessageReader, storage, Qualifiers.byName("local"))
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

    def 'expect unauthorized when not providing username and password'(){
        expect:
        RestAssured.get("/pipe/0")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def 'username and password authentication allows access to the data on the pipe'(){
        given: "a message on the pipe"
        storage.write(Message("type", "a", "ct", 100, null, null, null))

        expect: "to receive the message when authorized"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.OK.code)
            .content(equalTo('[{"type":"type","key":"a","contentType":"ct","offset":"100"}]'))
    }

    def 'runscope username and password authentication allows access to the data on the pipe'(){
        given: "a message on the pipe"
        storage.write(Message("type", "a", "ct", 100, null, null, null))

        expect: "to receive the message when authorized"
        def encodedCredentials = "${RUNSCOPE_USERNAME}:${RUNSCOPE_PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
                .header("Authorization", "Basic $encodedCredentials")
                .get("/pipe/0")
                .then()
                .statusCode(HttpStatus.OK.code)
                .content(equalTo('[{"type":"type","key":"a","contentType":"ct","offset":"100"}]'))
    }
}
