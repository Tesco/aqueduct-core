package com.tesco.aqueduct.pipe.identity.issuer

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class IdentityIssueTokenClientIntegrationSpec extends Specification {

    private final static String ISSUE_TOKEN_PATH = "/v4/issue-token/token"
    private final static String ACCESS_TOKEN = "asdfasfsfsdfsfssasdfsfwerwe"

    @Shared @AutoCleanup ErsatzServer identityMockService
    @Shared @AutoCleanup ApplicationContext context
    private static final String CLIENT_ID = "identityClientId"
    private static final String CLIENT_SECRET = "identityClientSecret"

    def setupSpec() {
        identityMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        identityMockService.start()

        context = ApplicationContext
                .build()
                .mainClass(EmbeddedServer)
                .properties(
                    parseYamlConfig(
                    """
                    authentication:
                      identity:
                        url:                ${identityMockService.getHttpUrl()}
                        issue.token.path:   "$ISSUE_TOKEN_PATH"
                        attempts:           3
                        delay:              500ms
                        client:
                         id:                "$CLIENT_ID"
                         secret:            "$CLIENT_SECRET"
                    micronaut:
                      http:
                        services:
                          identity-issue-token:
                            url: "${identityMockService.getHttpUrl()}"
                            logger-name: issue.token.client
                    """
                    )
                )
                .build()

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def setup() {
        identityMockService.clearExpectations()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "Identity token is issued from Identity service when valid token request is passed"() {
        given: "a mocked Identity service for issue token endpoint"
        def requestJson = JsonOutput.toJson([
            client_id       : CLIENT_ID,
            client_secret   : CLIENT_SECRET,
            grant_type      : "client_credentials",
            scope           : "internal public",
            confidence_level: 12
        ])

        identityMockService.expectations {
            post(ISSUE_TOKEN_PATH) {
                body(requestJson, "application/json")
                header("Accept", "application/vnd.tesco.identity.tokenresponse+json")
                header("TraceId", "someTraceId")
                header("Content-Type", "application/json")
                called(1)

                responder {
                    header("Content-Type", "application/vnd.tesco.identity.tokenresponse+json")
                    body("""
                    {
                        "access_token": "${ACCESS_TOKEN}",
                        "token_type"  : "bearer",
                        "expires_in"  : 1000,
                        "scope"       : "some: scope: value"
                    }
                    """)
                }
            }
        }

        and: "identity issue token client bean"
        IdentityIssueTokenClient identityIssueTokenClient = context.getBean(IdentityIssueTokenClient)

        when: "get issued token through Identity client"
        def identityToken = identityIssueTokenClient.retrieveIdentityToken(
            "someTraceId", new IssueTokenRequest(CLIENT_ID, CLIENT_SECRET)
        )

        then: "received expected identity token back"
        identityToken.accessToken == ACCESS_TOKEN
        !identityToken.isTokenExpired()

        and: "Identity mock service is called once"
        identityMockService.verify()
    }

    def "Identity service circuit is opened when it returns 5xx to connect for given number of times"() {
        given: "a mocked Identity service for issue token endpoint failing to connect"
        def requestJson = JsonOutput.toJson([
            client_id       : CLIENT_ID,
            client_secret   : CLIENT_SECRET,
            grant_type      : "client_credentials",
            scope           : "internal public",
            confidence_level: 12
        ])

        identityMockService.expectations {
            post(ISSUE_TOKEN_PATH) {
                body(requestJson, "application/json")
                header("Accept", "application/vnd.tesco.identity.tokenresponse+json")
                header("TraceId", "someTraceId")
                called(4)
                responder {
                    code(500)
                }
            }
        }

        and: "identity issue token client bean"
        IdentityIssueTokenClient identityIssueTokenClient = context.getBean(IdentityIssueTokenClient)

        when: "get issued token through Identity client"
        identityIssueTokenClient.retrieveIdentityToken(
            "someTraceId", new IssueTokenRequest(CLIENT_ID, CLIENT_SECRET)
        )

        then: "identity service is retried 4 times"
        identityMockService.verify()

        and:
        thrown(HttpClientResponseException)

        when:
        identityIssueTokenClient.retrieveIdentityToken(
            "someTraceId", new IssueTokenRequest(CLIENT_ID, CLIENT_SECRET)
        )

        then:
        identityMockService.verify()

        and:
        thrown(HttpClientResponseException)
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
