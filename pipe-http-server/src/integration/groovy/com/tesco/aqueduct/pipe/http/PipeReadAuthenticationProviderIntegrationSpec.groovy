package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.*
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MicronautTest
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

@Newify(Message)
@MicronautTest
class PipeReadAuthenticationProviderIntegrationSpec extends Specification {

    static final int RETRY_AFTER_SECONDS = 600
    static final String USERNAME = "username"
    static final String PASSWORD = "password"

    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server
    @Shared LocationResolver locationResolver = Mock()

    void setupSpec() {
        locationResolver.resolve(_) >> ["cluster1"]

        context = ApplicationContext
            .build()
            .properties(
                parseYamlConfig(
                    """
                    micronaut.security.enabled: true
                    compression.threshold-in-bytes: 1024
                    authentication:
                      users:
                        $USERNAME:
                          password: $PASSWORD
                          roles:
                            - PIPE_READ
                        $RUNSCOPE_USERNAME:
                          password: $RUNSCOPE_PASSWORD
                          roles:
                            - PIPE_READ
                    """
                )
            )
            .mainClass(PipeReadController)
            .build()

        CentralStorage centralStorageMock = Mock(CentralStorage)
        centralStorageMock.read(_, _, _) >> new MessageResults([], 0, OptionalLong.of(1), PipeState.UP_TO_DATE)

        context.registerSingleton(Reader, centralStorageMock, Qualifiers.byName("local"))
        def pipeStateProvider = Mock(PipeStateProvider) {
            getState(_ as List, _ as Reader) >> new PipeStateResponse(true, 100)
        }
        context.registerSingleton(pipeStateProvider)
        context.registerSingleton(locationResolver)
        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
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
        expect: "to receive the message when authorized"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def 'runscope username and password authentication allows access to the data on the pipe'(){
        expect: "to receive the message when authorized"
        def encodedCredentials = "${RUNSCOPE_USERNAME}:${RUNSCOPE_PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
