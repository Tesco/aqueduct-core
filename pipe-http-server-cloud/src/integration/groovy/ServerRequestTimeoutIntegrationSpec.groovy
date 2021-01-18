import Helper.IdentityMock
import Helper.SqlWrapper
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.LocationService
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.apache.http.NoHttpResponseException
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource

class ServerRequestTimeoutIntegrationSpec extends Specification {

    private static final String CLIENT_ID = UUID.randomUUID().toString()
    private static final String CLIENT_SECRET = UUID.randomUUID().toString()
    private static final String CLIENT_ID_AND_SECRET = "trn:tesco:cid:${CLIENT_ID}:${CLIENT_SECRET}"
    public static final String VALIDATE_TOKEN_PATH = "${IdentityMock.VALIDATE_PATH}?client_id=${CLIENT_ID_AND_SECRET}"

    private final static String ACCESS_TOKEN = UUID.randomUUID().toString()
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @Shared @AutoCleanup ApplicationContext context

    def setup() {
        def identityMock = new IdentityMock(CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN)

        def mockLocationResolver = Mock(LocationService)

        new SqlWrapper(pg.embeddedPostgres.postgresDatabase)

        mockLocationResolver.getClusterUuids(_) >> {
            sleep(3000) // Mocking method processing to take longer than server idle timeout
            ["someCluster"]
        }

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url":                              "http://cloud.pipe",
                "persistence.read.limit":                       1000,
                "persistence.read.retry-after":                 10000,
                "persistence.read.max-batch-size":              "10485760",
                "persistence.read.expected-node-count":         2,
                "persistence.read.cluster-db-pool-size":        10,
                "persistence.read.read-delay-seconds":          0,
                "location.clusters.get.path.filter.pattern":    "some/filter/pattern",

                "authentication.identity.url":                  "${identityMock.getUrl()}",
                "authentication.identity.validate.token.path":  "$VALIDATE_TOKEN_PATH",
                "authentication.identity.client.id":            "$CLIENT_ID",
                "authentication.identity.client.secret":        "$CLIENT_SECRET",
                "authentication.identity.attempts":             "3",
                "authentication.identity.delay":                "10ms",
                "authentication.identity.users.userA.clientId": "someClientUserId",
                "authentication.identity.users.userA.roles":    "PIPE_READ",

                "compression.threshold-in-bytes":               1024,
                "micronaut.server.idle-timeout":                "1s",
                // Following config ensures Micronaut retains thread management behaviour from 1.x.x where it chooses
                // which thread pool (event-loop or IO) to allocate to the controller based on blocking or non-blocking
                // return type from it.
                "micronaut.server.thread-selection":            "AUTO"
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("registry"))
            .registerSingleton(LocationService, mockLocationResolver)

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
        identityMock.acceptIdentityTokenValidationRequest()
    }

    def "No response from server when it is taking longer than configured server idle timeout"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        when: "read messages for the given location"
        RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "a no http response error is returned"
        thrown(NoHttpResponseException)
    }
}
