package com.tesco.aqueduct.pipe

import Helper.IdentityMock
import Helper.LocationMock
import Helper.SqlWrapper
import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.JsonHelper
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.pipe.codec.PipeCodecException
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
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class CodecIntegrationSpec extends Specification {

    private static final String CLIENT_ID = UUID.randomUUID().toString()
    private static final String CLIENT_SECRET = UUID.randomUUID().toString()
    private static final String CLIENT_ID_AND_SECRET = "trn:tesco:cid:${CLIENT_ID}:${CLIENT_SECRET}"

    private final static String ACCESS_TOKEN = UUID.randomUUID().toString()

    @Shared IdentityMock identityMockService
    @Shared LocationMock locationMockService
    @Shared @AutoCleanup ApplicationContext context
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()
    SqlWrapper sql

    def setupSpec() {
        identityMockService = new IdentityMock(CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN)
        locationMockService = new LocationMock(ACCESS_TOKEN)

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url":                              "http://cloud.pipe",
                "persistence.read.limit":                       1000,
                "persistence.read.retry-after":                 10000,
                "persistence.read.max-batch-size":              "10485760",
                "persistence.read.expected-node-count":         2,
                "persistence.read.cluster-db-pool-size":        10,

                "authentication.identity.url":                  "${identityMockService.getUrl()}",
                "authentication.identity.validate.token.path":  "/v4/access-token/auth/validate?client_id=${CLIENT_ID_AND_SECRET}",
                "authentication.identity.client.id":            "$CLIENT_ID",
                "authentication.identity.client.secret":        "$CLIENT_SECRET",
                "authentication.identity.issue.token.path":     "/v4/issue-token/token",
                "authentication.identity.attempts":             "3",
                "authentication.identity.delay":                "10ms",
                "authentication.identity.consumes":             "application/token+json",
                "authentication.identity.users.userA.clientId": "someClientUserId",
                "authentication.identity.users.userA.roles":    "PIPE_READ",

                "location.url":                                 "${locationMockService.getUrl()}",
                "location.attempts":                            3,
                "location.delay":                               "2ms",
                "location.reset":                               "10ms",

                "compression.threshold-in-bytes":               1024,
                "compression.logging":                          false
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("pipe"))
            .registerSingleton(DataSource, pg.embeddedPostgres.postgresDatabase, Qualifiers.byName("registry"))
            .registerSingleton(BrotliCodec, new ErrorProneCodec())

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    private static class ErrorProneCodec extends BrotliCodec {

        ErrorProneCodec() {
            super(4, false)
        }

        @Override
        byte[] encode(byte[] input) {
            if ( new String(input).containsIgnoreCase("error")) {
                throw new PipeCodecException("some error", new Exception())
            }
            return super.encode(input)
        }
    }


    void setup() {
        sql = new SqlWrapper(pg.embeddedPostgres.postgresDatabase)
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

    def "messages can be read from pipe in gzip codec format as specified in the request header"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = sql.insertCluster("Cluster_A")

        and: "some messages in the storage"
        def message1 = message(1, "type1", "A", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        def message2 = message(2, "type2", "B", "content-type", zoned("2000-12-01T10:00:00Z"), "data")
        sql.insertWithCluster(message1, clusterA)
        sql.insertWithCluster(message2, clusterA)

        when: "read messages for the given location with codec gzip"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .header("Accept-Encoding", "gzip")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response body correctly decoded messages"
        JsonHelper.messageFromJsonArray(response.getBody().asString()) == [message1, message2]
    }

    def "messages can be read from pipe in brotli codec format as specified in the request header"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = sql.insertCluster("Cluster_A")

        and: "some large messages in the storage"
        def someLargePayload = "data" * 300
        def message1 = message(1, "type1", "A", "content-type", zoned("2000-12-01T10:00:00Z"), someLargePayload)
        def message2 = message(2, "type2", "B", "content-type", zoned("2000-12-01T10:00:00Z"), someLargePayload)
        sql.insertWithCluster(message1, clusterA)
        sql.insertWithCluster(message2, clusterA)

        when: "read messages for the given location with codec brotli"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .header("Accept-Encoding", "br")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(200)

        and: "response content encoding is gzip"
        response
            .then()
            .header("X-Content-Encoding", Matchers.is("br"))

        and: "response body correctly decoded messages"
        JsonHelper.messageFromJsonArray(decodedString(response.getBody().asByteArray())) == [message1, message2]
    }

    def "Pipe fails with Internal server error when brotli codec errors out"() {
        given: "a location UUID"
        def locationUuid = UUID.randomUUID().toString()

        and: "location service returning clusters for the location uuid"
        locationMockService.getClusterForGiven(locationUuid, ["Cluster_A"])

        and: "clusters in the storage"
        Long clusterA = sql.insertCluster("Cluster_A")

        and: "some messages in the storage"
        def someLargePayload = "data" * 300
        def message1 = message(1, "error", "A", "content-type", zoned("2000-12-01T10:00:00Z"), someLargePayload)
        sql.insertWithCluster(message1, clusterA)

        when: "read messages for the given location with codec gzip"
        def response = RestAssured.given()
            .header("Authorization", "Bearer $ACCESS_TOKEN")
            .header("Accept-Encoding", "br")
            .get("/pipe/0?location=$locationUuid")

        then: "http ok response code"
        response
            .then()
            .statusCode(500)
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
}
