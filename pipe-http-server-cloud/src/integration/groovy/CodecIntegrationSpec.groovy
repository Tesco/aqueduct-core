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
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream
import org.hamcrest.Matchers
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

import static com.tesco.aqueduct.pipe.api.JsonHelper.messageFromJsonArray
import static java.util.stream.Collectors.joining

class CodecIntegrationSpec extends Specification {

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
                "location.delay":                               "10ms"
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("postgres"))

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

    def "messages can be read from pipe in gzip codec format as specified in the request header"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")

        and: "some messages in the storage"
        def message1 = message(1, "type1", "A", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterA)

        when: "read messages for the given location with codec gzip"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .header("Accept-Encoding", "gzip")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response content encoding is gzip"
        response
            .then()
            .header("content-encoding", Matchers.is("gzip"))

        and: "response body correctly decoded messages"
        messageFromJsonArray(response.getBody().asString()) == [message1, message2]
    }

    def "messages can be read from pipe in brotli codec format as specified in the request header"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")

        and: "some messages in the storage"
        def message1 = message(1, "type1", "A", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        insertWithCluster(message1, clusterA)
        insertWithCluster(message2, clusterA)

        when: "read messages for the given location with codec gzip"
        def response = RestAssured.given()
                .header("Authorization", "Bearer $ACCESS_TOKEN")
                .header("Accept-Encoding", "brotli")
                .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response content encoding is gzip"
        response
            .then()
            .header("content-encoding", Matchers.is("brotli"))

        and: "response body correctly decoded messages"
        messageFromJsonArray(decodedString(response.getBody().asByteArray())) == [message1, message2]
    }

    private String decodedString(byte[] bytes) {
        new BrotliCompressorInputStream(new ByteArrayInputStream(bytes)).text
    }

    private ZonedDateTime zoned(String dateTimeFormat) {
        ZonedDateTime.parse(dateTimeFormat)
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