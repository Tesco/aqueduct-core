import Helper.IdentityMock
import Helper.LocationMock
import Helper.SqlWrapper
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import groovy.sql.Sql
import groovy.transform.NamedVariant
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class LocationRoutingIntegrationSpec extends Specification {

    private static final String CLIENT_ID = UUID.randomUUID().toString()
    private static final String CLIENT_SECRET = UUID.randomUUID().toString()
    private static final String CLIENT_ID_AND_SECRET = "trn:tesco:cid:${CLIENT_ID}:${CLIENT_SECRET}"
    public static final String VALIDATE_TOKEN_PATH = "${IdentityMock.VALIDATE_PATH}?client_id=${CLIENT_ID_AND_SECRET}"

    private final static String ISSUE_TOKEN_PATH = "/v4/issue-token/token"
    private final static String ACCESS_TOKEN = UUID.randomUUID().toString()

    @Shared IdentityMock identityMockService
    @Shared LocationMock locationMockService

    @Shared @AutoCleanup ApplicationContext context
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()
    @AutoCleanup Sql sql

    def setupSpec() {
        locationMockService = new LocationMock(ACCESS_TOKEN)
        identityMockService = new IdentityMock(CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN)

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

                "authentication.identity.url":                  "${identityMockService.getUrl()}",
                "authentication.identity.validate.token.path":  "$VALIDATE_TOKEN_PATH",
                "authentication.identity.client.id":            "$CLIENT_ID",
                "authentication.identity.client.secret":        "$CLIENT_SECRET",
                "authentication.identity.issue.token.path":     "${IdentityMock.ISSUE_TOKEN_PATH}",
                "authentication.identity.attempts":             "3",
                "authentication.identity.delay":                "10ms",
                "authentication.identity.consumes":             "application/token+json",
                "authentication.identity.users.userA.clientId": "someClientUserId",
                "authentication.identity.users.userA.roles":    "PIPE_READ",

                "micronaut.caches.latest-offset-cache.expire-after-write": "0s",

                "location.url":                                 "${locationMockService.getUrl()}",
                "location.clusters.get.path":                   "${LocationMock.LOCATION_CLUSTER_PATH_WITH_QUERY_PARAM}",
                "location.clusters.get.path.filter.pattern":    "${LocationMock.LOCATION_CLUSTER_PATH_FILTER_PATTERN}",
                "location.attempts":                            3,
                "location.delay":                               "2ms",
                "location.reset":                               "10ms",

                "compression.threshold-in-bytes":               1024
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("registry"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void setup() {
        sql = new SqlWrapper(pg.embeddedPostgres.postgresDatabase).sql
        identityMockService.acceptIdentityTokenValidationRequest()
        identityMockService.issueValidTokenFromIdentity()
    }

    void cleanup() {
        identityMockService.clearExpectations()
        locationMockService.clearExpectations()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
        context.close()
    }

    def "messages are routed correctly for the given location when exists in storage"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")
        Long clusterB = insertCluster("Cluster_B")

        and: "messages in the storage for the clusters"
        def message1 = message(1, "type1", "A", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message3 = message(3, "type3", "C", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message4 = message(4, "type2", "D", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message5 = message(5, "type1", "E", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message6 = message(6, "type3", "F", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")

        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterB)
        insertWithCluster(message3, clusterB)
        insertWithCluster(message4, clusterA)
        insertWithCluster(message5, clusterA)
        insertWithCluster(message6, clusterA)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message1, message4, message5, message6]
    }

    def "messages are routed for the given location's cluster and default cluster when both exists in storage"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")
        Long clusterB = insertCluster("Cluster_B")

        and: "messages in the storage for the clusters"
        def message1 = message(1, "type1", "A", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message3 = message(3, "type3", "C", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message4 = message(4, "type2", "D", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message5 = message(5, "type1", "E", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message6 = message(6, "type3", "F", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterB)
        insertWithCluster(message3, clusterB)
        insertWithCluster(message4, clusterA)
        insertWithCluster(message5, clusterA)
        insertWithCluster(message6, clusterA)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message1, message4, message5, message6]
    }

    def "messages are not routed if the location does not belong to the clusters"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "some messages with default cluster"
        def message1 = message(1, "type1", "A", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")
        insertWithoutCluster(message1)
        insertWithoutCluster(message2)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has no messages for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == []
    }

    def "messages are routed for the given location belonging to multiple clusters"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_B", "Cluster_C"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")
        Long clusterB = insertCluster("Cluster_B")
        Long clusterC = insertCluster("Cluster_C")

        and: "messages in storage for the clusters"
        def message1 = message(1, "type1", "A", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message3 = message(3, "type3", "C", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message4 = message(4, "type2", "D", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message5 = message(5, "type1", "E", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message6 = message(6, "type3", "F", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message7 = message(7, "type4", "G", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")
        def message8 = message(8, "type5", "H", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")

        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterB)
        insertWithCluster(message3, clusterB)
        insertWithCluster(message4, clusterA)
        insertWithCluster(message5, clusterA)
        insertWithCluster(message6, clusterA)
        insertWithCluster(message7, clusterC)
        insertWithCluster(message8, clusterC)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message2, message3, message7, message8]
    }

    private ZonedDateTime utcZoned(String dateTimeFormat) {
        ZonedDateTime.parse(dateTimeFormat).withZoneSameLocal(ZoneId.of("UTC"))
    }

    static ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC).withZoneSameLocal(ZoneId.of("UTC"))

    @NamedVariant
    Message message(Long offset, String type, String key, String contentType, ZonedDateTime created, String data) {
        new Message(
            type ?: "type",
            key ?: "key",
            contentType ?: "contentType",
            offset,
            created ?: time,
            data ?: "data"
        )
    }

    void insertWithoutCluster(Message msg, int maxMessageSize=0, def time = Timestamp.valueOf(msg.created.toLocalDateTime()) ) {
        sql.execute(
                "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?,?);",
                msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize
        )
    }

    void insertWithCluster(Message msg, Long clusterId, def time = Timestamp.valueOf(msg.created.toLocalDateTime()), int maxMessageSize=0) {
        sql.execute(
                "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size, cluster_id) VALUES(?,?,?,?,?,?,?,?);",
                msg.offset, msg.key, msg.contentType, msg.type, time, msg.data, maxMessageSize, clusterId
        )
    }

    Long insertCluster(String clusterUuid){
        sql.executeInsert("INSERT INTO CLUSTERS(cluster_uuid) VALUES (?);", [clusterUuid]).first()[0] as Long
    }
}
