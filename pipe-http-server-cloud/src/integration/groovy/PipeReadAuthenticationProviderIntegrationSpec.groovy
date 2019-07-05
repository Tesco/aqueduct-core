

import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.pipe.http.PipeReadController
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import com.tesco.aqueduct.registry.NodeRegistry
import com.tesco.aqueduct.registry.PostgreSQLNodeRegistry
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.DriverManager
import java.time.Duration

import static io.restassured.RestAssured.given
import static org.hamcrest.Matchers.equalTo

@Newify(Message)
class PipeReadAuthenticationProviderIntegrationSpec extends Specification {

    static final int RETRY_AFTER_SECONDS = 600
    static final String cloudPipeUrl = "http://cloud.pipe"

    static final String USERNAME = "username"
    static final String PASSWORD = "password"

    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

    static final String SUPPORT_USERNAME = "support-username"
    static final String SUPPORT_PASSWORD = "support-password"

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    DataSource dataSource
    NodeRegistry registry

    static InMemoryStorage storage = new InMemoryStorage(10, RETRY_AFTER_SECONDS)


    def setupDatabase() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
            DROP TABLE IF EXISTS registry;
            
            CREATE TABLE registry(
            group_id VARCHAR PRIMARY KEY NOT NULL,
            entry JSON NOT NULL,
            version integer NOT NULL
            );
        """)

        registry = new PostgreSQLNodeRegistry(dataSource, new URL(cloudPipeUrl), Duration.ofDays(1))
    }

    void setupSpec() {
        context = ApplicationContext
                .build()
                .properties(
                    "micronaut.security.enabled": true,
                    "authentication.read-pipe.username": USERNAME,
                    "authentication.read-pipe.password": PASSWORD,
                    "authentication.read-pipe.runscope-username": RUNSCOPE_USERNAME,
                    "authentication.read-pipe.runscope-password": RUNSCOPE_PASSWORD,
                )
                //.mainClass(PipeReadController)
                .build()
                .registerSingleton(NodeRegistry, new PostgreSQLNodeRegistry(dataSource, new URL(cloudPipeUrl), Duration.ofDays(1)))
                .registerSingleton(MessageReader, storage, Qualifiers.byName("local"))
                .start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void setup() {
        storage.clear()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'expect unauthorized when not providing username and password'(){
        expect:
        RestAssured.get("/pipe/0")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def 'username and password authentication allows access to the data on the pipe'(){
        given: "a message on the pipe"
        storage.write(Message("type", "a", "ct", 100, null, null))

        expect: "to receive the message when authorized"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0")
            .then()
            .statusCode(HttpStatus.OK.code)
            .content(equalTo('[{"type":"type","key":"a","contentType":"ct","offset":"100"}]'))
    }

    def 'runscope username and password authentication allows access to the data on the pipe'(){
        given: "a message on the pipe"
        storage.write(Message("type", "a", "ct", 100, null, null))

        expect: "to receive the message when authorized"
        def encodedCredentials = "${RUNSCOPE_USERNAME}:${RUNSCOPE_PASSWORD}".bytes.encodeBase64().toString()
        RestAssured.given()
                .header("Authorization", "Basic $encodedCredentials")
                .get("/pipe/0")
                .then()
                .statusCode(HttpStatus.OK.code)
                .content(equalTo('[{"type":"type","key":"a","contentType":"ct","offset":"100"}]'))
    }

    def 'role based access'(){
        given: "a message on the pipe"
        storage.write(Message("type", "a", "ct", 100, null, null))

        when: "role based access control is implemented"

        then: "to receive the message when authorized (all three roles)"
        getMessageWithCreds(USERNAME, PASSWORD)
        getMessageWithCreds(RUNSCOPE_USERNAME, RUNSCOPE_PASSWORD)
        //getMessageWithCreds(SUPPORT_USERNAME, SUPPORT_PASSWORD)

        and: "to write node when authorised (till and support)"
        registerNodeWithCreds(USERNAME, PASSWORD, 6735, "http://1.1.1.1:1001", 123, "status", ["http://x"], 200)
        registerNodeWithCreds(SUPPORT_USERNAME, SUPPORT_PASSWORD, 6735, "http://1.1.1.1:1002", 123, "status", ["http://x"], 200)

        and: "to not node changes when unauthorised (runscope)"
        registerNodeWithCreds(RUNSCOPE_USERNAME, RUNSCOPE_PASSWORD, 6735, "http://1.1.1.1:1003", 123, "status", ["http://x"], 403)

        and: "to delete nodes when authorised (support)"
        deleteNodeWithCreds(SUPPORT_USERNAME, SUPPORT_PASSWORD, 6375,"6375|http://1.1.1.1:1001|",200)

        and: "to not delete nodes when unauthorised (till and runscope)"
        deleteNodeWithCreds(USERNAME, PASSWORD, 6375,"6375|http://1.1.1.1:1002|",403)
        deleteNodeWithCreds(RUNSCOPE_USERNAME, RUNSCOPE_PASSWORD, 6375,"6375|http://1.1.1.1:1002|",403)
    }

    private static void getMessageWithCreds(username, password) {
        def encodedCredentials = "${username}:${password}".bytes.encodeBase64().toString()
        given()
                .header("Authorization", "Basic $encodedCredentials")
                .get("/pipe/0")
                .then()
                .statusCode(HttpStatus.OK.code)
                .content(equalTo('[{"type":"type","key":"a","contentType":"ct","offset":"100"}]'))
    }

    private static void registerNodeWithCreds(username, password, group, url, offset=0, status="initialising", following=[cloudPipeUrl], expectedStatusCode) {
        def encodedCredentials = "${username}:${password}".bytes.encodeBase64().toString()
        given()
                .contentType("application/json")
                .header("Authorization", "Basic $encodedCredentials")
                .body("""{
                "group": "$group",
                "localUrl": "$url",
                "offset": "$offset",
                "status": "$status",
                "following": ["${following.join('", "')}"]
            }""")
                .when()
                .post("/registry")
                .then()
                .statusCode(expectedStatusCode)
    }

    private static void deleteNodeWithCreds(username, password, group, id, expectedStatusCode) {
        def encodedCredentials = "${username}:${password}".bytes.encodeBase64().toString()
        given()
                .contentType("application/json")
                .header("Authorization", "Basic $encodedCredentials")
                .when()
                .delete("/registry/$group/$id")
                .then()
                .statusCode(expectedStatusCode)
    }
}
