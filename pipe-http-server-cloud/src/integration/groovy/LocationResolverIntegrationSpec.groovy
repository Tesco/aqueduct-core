import Helper.IdentityMock
import Helper.LocationMock
import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenClient
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider
import com.tesco.aqueduct.pipe.location.LocationServiceException
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource

class LocationResolverIntegrationSpec extends Specification {

    private final static String ACCESS_TOKEN = "some_encrypted_token"
    private final static String CLIENT_ID = "someClientId"
    private final static String CLIENT_SECRET = "someClientSecret"
    private final static String CACHE_EXPIRY_HOURS = "1h"

    @Shared LocationMock locationMockService
    @Shared IdentityMock identityMockService

    @Shared
    @AutoCleanup
    ApplicationContext context

    def setup() {
        locationMockService = new LocationMock(ACCESS_TOKEN)

        identityMockService = new IdentityMock(CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN)

        context = ApplicationContext
            .build()
            .mainClass(EmbeddedServer)
            .properties(
                parseYamlConfig(
                """
                micronaut.caches.cluster-cache.expire-after-write: $CACHE_EXPIRY_HOURS
                location:
                    url:                                ${locationMockService.getUrl()}
                    clusters.get.path:                  ${LocationMock.LOCATION_CLUSTER_PATH_WITH_QUERY_PARAM}
                    clusters.get.path.filter.pattern:   ${LocationMock.LOCATION_CLUSTER_PATH_FILTER_PATTERN}
                    attempts:               3
                    delay:                  1ms
                    reset:                  10ms
                authentication:
                    identity:
                        url:                ${identityMockService.getUrl()}
                        issue.token.path:   ${IdentityMock.ISSUE_TOKEN_PATH}
                        attempts:           3
                        delay:              500ms
                        consumes:           "application/token+json"
                        client:
                            id:         "$CLIENT_ID"
                            secret:     "$CLIENT_SECRET"
                """
                )
            )
            .build()
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("registry"))
            .registerSingleton(new IdentityIssueTokenProvider(() -> context.getBean(IdentityIssueTokenClient), CLIENT_ID, CLIENT_SECRET))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()
    }

    void cleanup() {
        locationMockService.clearExpectations()
        identityMockService.clearExpectations()
    }

    def "Location service is invoked to fetch cluster for given location Uuid"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning list of clusters for a given Uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A", "Cluster_B"])

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        List<String> clusters = locationResolver.resolve(locationUuid)

        then:
        clusters == ["Cluster_A","Cluster_B"]

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "Location service throws LocationServiceUnavailable error when client throws http client response error with status code 5xx"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning list of clusters for a given Uuid"
        locationMockService.returningError(locationUuid, 500, 4)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then:
        thrown(LocationServiceException)

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "Location resolver caches the response"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning list of clusters for a given Uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A", "Cluster_B"])

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        def clusters = locationResolver.resolve(locationUuid)

        then:
        clusters == ["Cluster_A","Cluster_B"]

        when: "get clusters again for the same location Uuid"
        def clusters2 = locationResolver.resolve(locationUuid)

        then:
        clusters2 == ["Cluster_A","Cluster_B"]

        and: "location service is called once"
        locationMockService.verify()
    }

    def "Location resolver does not cache when location service throws error"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning 5xx error and has expectation on how many times it should be called"
        locationMockService.returningError(locationUuid, 500, 6) // 4 times on first call and 2 times on second invocation

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then: "throw an error"
        thrown(LocationServiceException)

        when: "circuit breaker is reset to half-open"
        sleep(50)

        and: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then: "error is thrown again"
        thrown(LocationServiceException)

        and: "location service is called again and exception was not cached"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "Location service propagates client error when client throws http client response error with status code 4xx"() {
        given:
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning list of clusters for a given Uuid"
        locationMockService.returningError(locationUuid, 400, 4)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then:
        thrown(HttpClientResponseException)

        and: "location service is called once"
        locationMockService.verify()

        and: "identity service is called once"
        identityMockService.verify()
    }

    def "location service error when location service return unexpected response"() {
        given: "a location Uuid"
        def locationUuid = "locationUuid"

        and: "a mocked Identity service for issue token endpoint"
        identityMockService.issueValidTokenFromIdentity()

        and: "location service returning empty body for a given Uuid"
        locationMockService.returnEmptyBody(locationUuid)

        and: "location service bean is initialized"
        def locationResolver = context.getBean(LocationResolver)

        when: "get clusters for a location Uuid"
        locationResolver.resolve(locationUuid)

        then:
        thrown(LocationServiceException)
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
