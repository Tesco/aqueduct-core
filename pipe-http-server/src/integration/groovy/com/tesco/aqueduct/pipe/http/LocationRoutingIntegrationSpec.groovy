package com.tesco.aqueduct.pipe.http

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

    private final static String ISSUE_TOKEN_PATH = "/v4/issue-token/token"
    private final static String ACCESS_TOKEN = UUID.randomUUID().toString()

    private final static String LOCATION_BASE_PATH = "/tescolocation"
    private final static String LOCATION_CLUSTER_PATH = "/v4/clusters/locations"

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


        // set the environment variables
        System.setProperty("IDENTITY_URL", identityMockService.getHttpUrl())
        System.setProperty("IDENTITY_VALIDATE_TOKEN_PATH", "${VALIDATE_TOKEN_BASE_PATH}?client_id=${CLIENT_ID_AND_SECRET}")
        System.setProperty("IDENTITY_ISSUE_TOKEN_PATH", ISSUE_TOKEN_PATH)
        System.setProperty("IDENTITY_CLIENT_ID", CLIENT_ID)
        System.setProperty("IDENTITY_CLIENT_SECRET", CLIENT_SECRET)

        System.setProperty("LOCATION_URL", locationMockService.getHttpUrl() + "$LOCATION_BASE_PATH/")

        context = ApplicationContext
                .build()
                .mainClass(EmbeddedServer)
                .environments("integration")
                .build()
                .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("postgres"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void setup() {
        setupPostgres()
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

    def "messages are routed correctly for the given location when exist in storage"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationServiceReturningListOfClustersForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = insertCluster("Cluster_A")
        Long clusterB = insertCluster("Cluster_B")

        and: "messages in the storage for the clusters"
        def message1_A = message(1, "type1", "A", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")
        def message4_A = message(4, "type2", "D", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")
        def message5_A = message(5, "type1", "E", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")
        def message6_A = message(6, "type3", "F", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")
        def message2_B = message(2, "type2", "B", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")
        def message3_B = message(3, "type3", "C", "content-type", ZonedDateTime.parse("2000-12-01T10:00:00Z"), "data")

        insertWithCluster(message1_A, clusterA)
        insertWithCluster(message2_B, clusterB)
        insertWithCluster(message3_B, clusterB)
        insertWithCluster(message4_A, clusterA)
        insertWithCluster(message5_A, clusterA)
        insertWithCluster(message6_A, clusterA)

        when: "read messages for the given location"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body has messages only for the given location"
        Arrays.asList(response.getBody().as(Message[].class)) == [message1_A, message4_A, message5_A, message6_A]
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

    private void setupPostgres() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)
        sql.execute("""
        DROP TABLE IF EXISTS EVENTS;
        DROP TABLE IF EXISTS CLUSTERS;
          
        CREATE TABLE EVENTS(
            msg_offset bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL,
            tags JSONB NULL, 
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

        def clusterString = clusters.stream().map {
            """{
                "id": "$it",
                "name": "Cluster $it",
                "origin": "ORIGIN $it"
            }"""
        }.collect(joining(","))

        locationMockService.expectations {
            get(LOCATION_BASE_PATH + LOCATION_CLUSTER_PATH + "/$locationUuid") {
                header("Authorization", "Bearer ${ACCESS_TOKEN}")
                called(1)

                responder {
                    header("Content-Type", "application/json")
                    body("""
                    {
                        "clusters": [$clusterString],
                        "totalCount": 2
                    }
               """)
                }
            }
        }
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
    }

}
