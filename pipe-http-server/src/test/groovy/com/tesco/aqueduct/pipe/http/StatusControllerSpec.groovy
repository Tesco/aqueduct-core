package com.tesco.aqueduct.pipe.http

import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.equalTo

class StatusControllerSpec extends Specification {
    static final String USERNAME = "username"
    static final String PASSWORD = "password"

    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

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
            .mainClass(StatusController)
            .build()

        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "pipe status contains version in it"() {
        expect:
        when()
            .get("/pipe/_status")
            .then()
            .statusCode(HttpStatus.OK.code)
            .body("version", equalTo(Version.getImplementationVersion()))
    }
}
