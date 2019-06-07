package com.tesco.aqueduct.pipe.http

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PageSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    void setupSpec() {
        context = ApplicationContext
            .build()
            .mainClass(EmbeddedServer)
            .properties(
                "micronaut.security.enabled": true,
            ).build()

        context.start()
        server = context.getBean(EmbeddedServer)

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    @Unroll
    void "UI is returned for #path"() {
        given: "a started server"
        server.start()

        when: "we call #path"
        def request = RestAssured.get(path)

        then: "we expect the correct statusCode"
        request
                .then()
                .statusCode(200)

        where:
        path << ["/ui", "/ui/"]
    }
}
