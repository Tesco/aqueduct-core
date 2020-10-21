import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.Message
import groovy.json.JsonOutput
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

import static java.util.stream.Collectors.joining

class LocationRoutingIntegrationSpec extends Specification {

    private static final String VALIDATE_TOKEN_BASE_PATH = '/v4/access-token/auth/validate'
    private static final String CLIENT_ID = UUID.randomUUID().toString()
    private static final String CLIENT_SECRET = UUID.randomUUID().toString()
    private static final String CLIENT_ID_AND_SECRET = "trn:tesco:cid:${CLIENT_ID}:${CLIENT_SECRET}"
    public static final String VALIDATE_TOKEN_PATH = "${VALIDATE_TOKEN_BASE_PATH}?client_id=${CLIENT_ID_AND_SECRET}"

    private final static String ISSUE_TOKEN_PATH = "/v4/issue-token/token"
    private final static String ACCESS_TOKEN = UUID.randomUUID().toString()

    private final static String LOCATION_BASE_PATH = "/tescolocation"
    private final static String LOCATION_CLUSTER_PATH = "/clusters/v1/locations/{locationUuid}/clusters/ids"

    @Shared @AutoCleanup ErsatzServer identityMockService
    @Shared @AutoCleanup ErsatzServer locationMockService
    @Shared @AutoCleanup ApplicationContext context
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()
    @AutoCleanup Sql sql

    def setupSpec() {
        identityMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        locationMockService.start()
        identityMockService.start()

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url":                              "http://cloud.pipe",
                "persistence.read.limit":                       1000,
                "persistence.read.retry-after":                 10000,
                "persistence.read.max-batch-size":              "10485760",
                "persistence.read.expected-node-count":         2,
                "persistence.read.cluster-db-pool-size":        10,

                "authentication.identity.url":                  "${identityMockService.getHttpUrl()}",
                "authentication.identity.validate.token.path":  "$VALIDATE_TOKEN_PATH",
                "authentication.identity.client.id":            "$CLIENT_ID",
                "authentication.identity.client.secret":        "$CLIENT_SECRET",
                "authentication.identity.issue.token.path":     "$ISSUE_TOKEN_PATH",
                "authentication.identity.attempts":             "3",
                "authentication.identity.delay":                "10ms",
                "authentication.identity.users.userA.clientId": "someClientUserId",
                "authentication.identity.users.userA.roles":    "PIPE_READ",

                "location.url":                                 "${locationMockService.getHttpUrl() + "$LOCATION_BASE_PATH/"}",
                "location.attempts":                            3,
                "location.delay":                               "2ms",
                "location.reset":                               "10ms",

                "compression.threshold-in-bytes":               1024
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("pipe"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void setup() {
        setupPostgres(pg.embeddedPostgres.postgresDatabase)
        acceptIdentityTokenValidationRequest()
        issueValidTokenFromIdentity()
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
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

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

    def "messages for default cluster are routed when no clusters are found for the given location"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, [])

        and: "some clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")
        Long clusterB = insertCluster("Cluster_B")

        and: "messages in the storage for default clusters"
        def message1 = message(1, "type1", "A", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message3 = message(3, "type3", "C", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message4 = message(4, "type2", "D", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message5 = message(5, "type1", "E", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")
        def message6 = message(6, "type3", "F", "content-type", utcZoned("2000-12-01T10:00:00Z"), "data")

        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterB)
        insertWithoutCluster(message3)
        insertWithoutCluster(message4)
        insertWithCluster(message5, clusterA)
        insertWithoutCluster(message6)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for default cluster"
        Arrays.asList(response.getBody().as(Message[].class)) == [message3, message4, message6]
    }

    def "messages are routed for the given location's cluster and default cluster when both exists in storage"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

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

        and: "some messages with default cluster"
        def message7 = message(7, "type4", "G", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")
        def message8 = message(8, "type5", "H", "content-type", utcZoned("2000-12-03T10:00:00Z"), "data")
        insertWithoutCluster(message7)
        insertWithoutCluster(message8)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message1, message4, message5, message6, message7, message8]
    }

    def "messages are routed for the default cluster when no clustered message exists in storage"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

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

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message1, message2]
    }

    def "messages are routed for the given location belonging to multiple clusters"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_B", "Cluster_C"])

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

    private void setupPostgres(DataSource dataSource) {
        sql = new Sql(dataSource.connection)
        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS CLUSTERS;
          
        CREATE TABLE EVENTS(
            msg_offset BIGSERIAL PRIMARY KEY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL, 
            data text NULL,
            event_size int NOT NULL,
            cluster_id BIGINT NOT NULL DEFAULT 1
        );
        
        CREATE TABLE CLUSTERS(
            cluster_id BIGSERIAL PRIMARY KEY NOT NULL,
            cluster_uuid VARCHAR NOT NULL
        );
        
        INSERT INTO CLUSTERS (cluster_uuid) VALUES ('NONE');
        """)
    }

    def acceptIdentityTokenValidationRequest() {
        def json = JsonOutput.toJson([access_token: ACCESS_TOKEN])

        identityMockService.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                queries("client_id": [CLIENT_ID_AND_SECRET])
                body(json, "application/json")
                called(1)

                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "UserId": "someClientUserId",
                          "Status": "VALID",
                          "Claims": [
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/clientid",
                              "value": "trn:tesco:cid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/scope",
                              "value": "oob"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/userkey",
                              "value": "trn:tesco:uid:uuid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel",
                              "value": "12"
                            },
                            {
                              "claimType": "http://schemas.microsoft.com/ws/2008/06/identity/claims/expiration",
                              "value": "1548413702"
                            }
                          ]
                        }
                    """)
                }
            }
        }
    }

    private void locationServiceReturningListOfClustersForGiven(
            String locationUuid, List<String> clusters) {

        def clusterString = clusters.stream().map{"\"$it\""}.collect(joining(","))

        def revisionId = clusters.isEmpty() ? null : "2"

        locationMockService.expectations {
            get(LOCATION_BASE_PATH + locationPathIncluding(locationUuid)) {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [$clusterString],
                        "revisionId": "$revisionId"
                    }
               """)
                }
            }
        }
    }

    private String locationPathIncluding(String locationUuid) {
        LOCATION_CLUSTER_PATH.replace("{locationUuid}", locationUuid)
    }

    def issueValidTokenFromIdentity() {
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
    }

}
