package com.tesco.aqueduct.pipe.location

import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.stehno.ersatz.junit.ErsatzServerRule
import com.tesco.aqueduct.pipe.api.Cluster
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenClient
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class LocationResolverIntegrationSpec extends Specification {

    private final static String LOCATION_BASE_PATH = "/tescolocation"
    private final static String LOCATION_CLUSTER_PATH = "/v4/clusters/locations"
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
                        get: 
                            cluster:
                            path:               "${LOCATION_CLUSTER_PATH}"
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
        context.registerSingleton(new CloudLocationResolver(context.getBean(LocationServiceClient)))
        context.registerSingleton(new IdentityIssueTokenProvider(context.getBean(IdentityIssueTokenClient), CLIENT_ID, CLIENT_SECRET))

        def server = context.getBean(EmbeddedServer)
        server.start()
    }

    def "Location service is invoked to fetch cluster for given location Uuid"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningClustersFor(locationUuid)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(CloudLocationResolver)

        when: "get clusters for a location Uuid"
        List<Cluster> clusters = locationResolver.resolve(locationUuid)

        then:
        clusters == [new Cluster("cluster_A"), new Cluster("cluster_B")]

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "Location service throws LocationServiceUnavailable error when client throws http client response error with status code 5xx"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningInternalServerErrorFor(locationUuid)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(CloudLocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then:
        thrown(LocationServiceUnavailableException)

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "Location service propagates client error when client throws http client response error with status code 4xx"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningBadRequestFor(locationUuid)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(CloudLocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then:
        thrown(HttpClientResponseException)

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }


    def "Empty list of location clusters when location service fails with 404"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityIssueTokenService()

        and: "location service returning list of clusters for a given Uuid"
        locationServiceReturningNotFoundFor(locationUuid)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(CloudLocationResolver)

        when: "get clusters for a location Uuid"
        def listOfClusters = locationResolver.resolve(locationUuid)

        then:
        listOfClusters.isEmpty()

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    private void locationServiceReturningNotFoundFor(String locationUuid) {
        locationServiceReturningError(locationUuid, 404, 1)
    }

    private void locationServiceReturningBadRequestFor(String locationUuid) {
        locationServiceReturningError(locationUuid, 400, 4)
    }

    private void locationServiceReturningInternalServerErrorFor(String locationUuid) {
        locationServiceReturningError(locationUuid, 500, 4)
    }

    private ErsatzServer locationServiceReturningError(String locationUuid, int status, int invocationCount) {
        locationMockService.expectations {
            get(LOCATION_BASE_PATH + LOCATION_CLUSTER_PATH + "/$locationUuid") {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(invocationCount)
                responder {
                    code(status)
                }
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

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }

    void locationServiceReturningClustersFor(String locationUuid) {
        locationMockService.expectations {
            get(LOCATION_BASE_PATH + LOCATION_CLUSTER_PATH + "/$locationUuid") {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [
                            {
                                "id": "cluster_A",
                                "name": "Cluster A",
                                "origin": "ORIGIN1"
                            },
                            {
                                "id": "cluster_B",
                                "name": "Cluster B",
                                "origin": "ORIGIN2"
                            }
                        ],
                        "totalCount": 2
                    }
               """)
                }
            }
        }
    }
}