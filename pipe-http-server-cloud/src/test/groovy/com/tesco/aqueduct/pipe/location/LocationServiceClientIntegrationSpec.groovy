package com.tesco.aqueduct.pipe.location

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.stehno.ersatz.junit.ErsatzServerRule
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class LocationServiceClientIntegrationSpec extends Specification {

    private final static String LOCATION_BASE_PATH = "/tescolocation"
    private final static String LOCATION_CLUSTER_PATH = "/clusters/v1/locations/{locationUuid}/clusters/ids"
    private final static String ISSUE_TOKEN_PATH = "/v4/issue-token/token"
    private final static String ACCESS_TOKEN = "some_encrypted_token"
    private final static String CLIENT_ID = "someClientId"
    private final static String CLIENT_SECRET = "someClientSecret"
    private final static String CACHE_EXPIRY_HOURS = "1h"

    @Shared
    @AutoCleanup
    ErsatzServer locationMockService
    @Shared
    @AutoCleanup
    ErsatzServer identityMockService
    @Shared
    @AutoCleanup
    ApplicationContext context

    def setup() {
        locationMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        identityMockService = new ErsatzServerRule({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService.start()
        identityMockService.start()

        String locationBasePath = locationMockService.getHttpUrl() + "$LOCATION_BASE_PATH/"

        context = ApplicationContext
                .build()
                .mainClass(EmbeddedServer)
                .properties(
                    parseYamlConfig(
                    """
                    micronaut.caches.cluster-cache..expire-after-write: $CACHE_EXPIRY_HOURS
                    location:
                        url:                    $locationBasePath
                        attempts:               3
                        delay:                  500ms  
                    authentication:
                        identity:
                            url:                ${identityMockService.getHttpUrl()}
                            issue.token.path:   "$ISSUE_TOKEN_PATH"
                            attempts:           3
                            delay:              500ms
                            client:
                                id:         "$CLIENT_ID"
                                secret:     "$CLIENT_SECRET"
                    """
                    )
                )
                .build()

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()
    }

    def "A list of clusters are provided for given location Uuid by authorized location service"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        def clusterResponse = locationServiceClient.getClusters("someTraceId", locationUuid)

        then:
        clusterResponse.body().clusters == ["cluster_A","cluster_B"]

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "location service is retried 3 times before throwing exception when it fails to resolve location to cluster"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning error"
        locationServiceReturningError(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        locationServiceClient.getClusters("someTraceId", locationUuid)

        then: "location service is called 4 times in total"
        locationMockService.verify()

        and: "identity is called once"
        identityMockService.verify()

        and: "Http client response exception is thrown"
        thrown(HttpClientResponseException)
    }

    def "Unauthorised exception is thrown if token is invalid or missing"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service that fails to issue a token"
        identityIssueTokenFailure()

        and: "a mock for location service is not called"
        locationServiceNotInvoked(locationUuid)

        and: "location service bean is initialized"
        def locationServiceClient = context.getBean(LocationServiceClient)

        when: "get clusters for a location Uuid"
        locationServiceClient.getClusters("someTraceId", locationUuid)

        then: "location service is not called"
        locationMockService.verify()

        and: "an exception is thrown"
        thrown(HttpClientResponseException)
    }

    private void locationServiceReturningListOfClustersForGiven(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_BASE_PATH + locationPathIncluding(locationUuid)) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [
                            "cluster_A",
                            "cluster_B"
                        ],
                        "totalCount": 2
                    }
               """)
                }
            }
        }
    }

    private void locationServiceReturningError(String locationUuid) {
        locationServiceReturningError(locationUuid, 4)
    }

    private void locationServiceReturningError(String locationUuid, Integer times) {
        locationMockService.expectations {
            get(LOCATION_BASE_PATH + locationPathIncluding(locationUuid)) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(times)

                responder {
                    header("Content-Type", "application/json")
                    code(500)
                }
            }
        }
    }

    private void locationServiceNotInvoked(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_BASE_PATH + locationPathIncluding(locationUuid)) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(0)
            }
        }
    }

    private void identityIssueTokenService() {
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
                called(1)
                responder {
                    header("Content-Type", "application/vnd.tesco.identity.tokenresponse+json")
                    code(200)
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
    }

    private void identityIssueTokenFailure() {
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
                called(1)
                responder {
                    code(403)
                }
            }
        }
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }

    private String locationPathIncluding(String locationUuid) {
        LOCATION_CLUSTER_PATH.replace("{locationUuid}", locationUuid)
    }
}
