import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.TestAppender
import com.tesco.aqueduct.pipe.api.OffsetName
import com.tesco.aqueduct.pipe.api.Reader
import com.tesco.aqueduct.registry.model.NodeRegistry
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry
import com.tesco.aqueduct.registry.model.NodeRequestStorage
import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRequestStorage
import groovy.json.JsonOutput
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.hamcrest.Matcher
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.DriverManager
import java.time.Duration

import static com.tesco.aqueduct.pipe.api.PipeState.UP_TO_DATE
import static com.tesco.aqueduct.registry.model.Status.FOLLOWING
import static com.tesco.aqueduct.registry.model.Status.INITIALISING
import static com.tesco.aqueduct.registry.model.Status.OFFLINE
import static com.tesco.aqueduct.registry.model.Status.OK
import static com.tesco.aqueduct.registry.model.Status.PENDING
import static io.restassured.RestAssured.given
import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.*

class NodeRegistryControllerV2IntegrationSpec extends Specification {
    private static final String VALIDATE_TOKEN_BASE_PATH = '/v4/access-token/auth/validate'
    private static final String CLOUD_PIPE_URL = "http://cloud.pipe"
    private static final String USERNAME = "username"
    private static final String PASSWORD = "password"
    private static final String USERNAME_ENCODED_CREDENTIALS = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
    private static final String USERNAME_TWO = "username-two"
    private static final String PASSWORD_TWO = "password-two"
    private static final int SERVER_TIMEOUT_MS = 5000
    private static final int SERVER_SLEEP_TIME_MS = 500
    private static final String NODE_A_CLIENT_UID = "random"

    private static final String clientId = UUID.randomUUID().toString()
    private static final String secret = UUID.randomUUID().toString()
    private static final String clientIdAndSecret = "${clientId}:${secret}"

    private static final String userUIDA = UUID.randomUUID()
    private static final String clientUserUIDA = "trn:tesco:uid:uuid:${userUIDA}"
    private static final String validateTokenPath = "${VALIDATE_TOKEN_BASE_PATH}?client_id={clientIdAndSecret}"

    @Shared @AutoCleanup ErsatzServer identityMock
    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup Sql sql

    DataSource dataSource
    NodeRegistry registry
    NodeRequestStorage nodeRequestStorage

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

        sql.execute("""
            DROP TABLE IF EXISTS node_requests;
            
            CREATE TABLE node_requests(
            host_id VARCHAR PRIMARY KEY NOT NULL,
            bootstrap_requested timestamp NOT NULL,
            bootstrap_type VARCHAR NOT NULL,
            bootstrap_received timestamp
            );
        """)

        nodeRequestStorage = new PostgreSQLNodeRequestStorage(dataSource)
        registry = new PostgreSQLNodeRegistry(dataSource, new URL(CLOUD_PIPE_URL), Duration.ofDays(1))
    }

    void setupSpec() {
        identityMock = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        identityMock.start()
    }

    void setup() {

        setupDatabase()

        Reader reader = Mock(Reader) {
            getOffset(OffsetName.GLOBAL_LATEST_OFFSET) >> OptionalLong.of(100L);
        }

        context = ApplicationContext
            .build()
            .properties(
            // enabling security to prove that registry is accessible anyway
            parseYamlConfig(
                    """
                micronaut.security.enabled: true
                micronaut.server.port: -1
                micronaut.caches.identity-cache.expire-after-write: 1m
                micronaut.security.token.jwt.enabled: true
                micronaut.security.token.jwt.bearer.enabled: true
                authentication:
                  users:
                    $USERNAME:
                      password: $PASSWORD
                      roles:
                        - REGISTRY_DELETE
                        - BOOTSTRAP_NODE
                        - REGISTRY_WRITE
                    $USERNAME_TWO:
                      password: $PASSWORD_TWO
                  identity:
                    url: ${identityMock.getHttpUrl()}
                    validate.token.path: $validateTokenPath
                    client:
                        id: $clientId
                        secret: $secret
                    users:
                      nodeA:
                        clientId: "${NODE_A_CLIENT_UID}"
                        roles:
                          - PIPE_READ
                          - REGISTRY_WRITE
                """
            )
            )
            .build()
            .registerSingleton(NodeRegistry, registry)
            .registerSingleton(Reader, reader)
            .registerSingleton(NodeRequestStorage, nodeRequestStorage)
            .start()

        identityMock.clearExpectations()

        server = context.getBean(EmbeddedServer)

        RestAssured.port = server.port
        server.start()
        def time = 0
        while (!server.isRunning() && time < SERVER_TIMEOUT_MS) {
            println("Server not yet running...")
            sleep SERVER_SLEEP_TIME_MS
            time += SERVER_SLEEP_TIME_MS
        }
        println("Test setup complete")
        TestAppender.clearEvents()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'expect unauthorized when not providing username and password to registry'() {
        expect:
        given()
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$CLOUD_PIPE_URL"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def "Can post to registry"() {
        expect: "We can post info to the registry"
        given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "pipe": {"pipeState" : "$UP_TO_DATE", "v":"1.0"},
                "status": "$INITIALISING",
                "following": ["$CLOUD_PIPE_URL"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(200)
            .body(
                "bootstrapType", equalTo("NONE"),
                "requestedToFollow", contains(CLOUD_PIPE_URL)
            )
    }

    @Ignore
    def "post to registry without version is a bad request"() {
        expect: "posting to registry without version fails with 422"
        given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "pipe": {"pipeState" : "$UP_TO_DATE"},
                "status": "$INITIALISING",
                "following": ["$CLOUD_PIPE_URL"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(422)
            .body("message", equalTo("Sub group id needs to be available for localhost"))
    }

    def "Can get registry summary"() {
        expect: "We can get info from registry"
        given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
        .when()
            .get("/v2/registry")
        .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo(OK.toString())
            )
    }

    def "Registered nodes are returned"() {
        given: "We register a node"
        registerNode(6735, "http://1.1.1.1:1234", 123, FOLLOWING, ["http://x"])

        when: "we get summary"
        def request =
            given()
                .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            when()
                .get("/v2/registry")

        then:
        request.then()
            .statusCode(200)
            .body(
                "followers[0].group", equalTo("6735"),
                "followers[0].localUrl", equalTo("http://1.1.1.1:1234"),
                "followers[0].offset", equalTo("123"),
                "followers[0].status", equalTo(FOLLOWING.toString()),
                "followers[0].following", contains("http://x"),
                "followers[0].requestedToFollow", contains("http://cloud.pipe")
            )
    }

    @Unroll
    def "Summary can filter by stores - #comment"() {
        given: "We register nodes from different groups"
        registerNode("a", "http://a")
        registerNode("b", "http://b")
        registerNode("c", "http://c")

        when: "we get summary"
        def request = given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .urlEncodingEnabled(false) // to disable changing ',' to %2Cc as this ',' is special character here, not part of the value
            .when().get("/v2/registry$query")

        then:
        request.then()
            .statusCode(200)
            .body("followers*.group", containsInAnyOrder(result.toArray()))

        where:
        query                         | result          | comment
        ""                            | ["a", "b", "c"] | "no filters"
        "?groups=b"                   | ["b"]           | "one store"
        "?groups=a&groups=c&groups=b" | ["a", "b", "c"] | "all stores, repeated param"
        "?groups=b&groups=c"          | ["b", "c"]      | "some stores, repeated param"

        //Below cases not currently supported, uncomment once they are supported !
        //"?groups=a,c,b"             | ["a", "b", "c"] | "all stores, comma separated"
        //"?groups=a,c"               | ["a", "c"]      | "some stores, comma separated"
    }

    def "deleting a single node from the registry"() {
        given: "We register two nodes"
        registerNode(1234, "http://1.1.1.1:1234", 123, FOLLOWING, ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, INITIALISING, ["http://y"])

        when: "node is deleted"
        given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .contentType("application/json")
        .when()
            .delete("/v2/registry/1234/1.1.1.1")
        .then()
            .statusCode(200)

        then: "node has been deleted, and other groups are unaffected"
        def request =
            given()
                .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .when()
                .get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        !request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    def "deleting a node from the registry and rebalancing a group"() {
        given: "We register multiple nodes"
        registerNode(1234, "http://1.1.1.1:0001", 123, FOLLOWING)
        registerNode(1234, "http://1.1.1.2:0002", 123, FOLLOWING)
        registerNode(1234, "http://1.1.1.3:0003", 123, FOLLOWING)
        registerNode(1234, "http://1.1.1.4:0004", 123, FOLLOWING)
        registerNode(1234, "http://1.1.1.5:0005", 123, FOLLOWING)

        when: "first node is deleted"
        given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .contentType("application/json")
        .when()
            .delete("/v2/registry/1234/1.1.1.1")
        .then()
            .statusCode(200)

        then: "registry has been rebalanced"
        def request = given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .when().get("/v2/registry")

        request.then().body(
            "followers[0].localUrl", equalTo("http://1.1.1.2:0002"),
            "followers[1].localUrl", equalTo("http://1.1.1.3:0003"),
            "followers[2].localUrl", equalTo("http://1.1.1.4:0004"),
            "followers[3].localUrl", equalTo("http://1.1.1.5:0005"),
            //following lists
            "followers[0].requestedToFollow", equalTo(["http://cloud.pipe"]),
            "followers[1].requestedToFollow", equalTo(["http://1.1.1.2:0002", "http://cloud.pipe"]),
            "followers[2].requestedToFollow", equalTo(["http://1.1.1.2:0002", "http://cloud.pipe"]),
            "followers[3].requestedToFollow", equalTo(["http://1.1.1.3:0003", "http://1.1.1.2:0002", "http://cloud.pipe"])
        )

        request.then().statusCode(200)
    }

    def "if some registered nodes are offline, they are sorted to the base of the hierarchy"() {
        given: "six nodes with varying state"
        registerNode(1234, "http://1.1.1.1:0001", 123, OFFLINE)
        registerNode(1234, "http://1.1.1.2:0002", 123, OFFLINE)
        registerNode(1234, "http://1.1.1.3:0003", 123, PENDING)
        registerNode(1234, "http://1.1.1.4:0004", 123, FOLLOWING)
        registerNode(1234, "http://1.1.1.5:0005", 123, INITIALISING)
        registerNode(1234, "http://1.1.1.6:0006", 123, OFFLINE)

        when: "We get the hierarchy"
        def request = given()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .when().get("/v2/registry")

        then: "Nodes are sorted as expected"
        request.then().body(
            "followers[0].localUrl", equalTo("http://1.1.1.3:0003"),
            "followers[1].localUrl", equalTo("http://1.1.1.4:0004"),
            "followers[2].localUrl", equalTo("http://1.1.1.5:0005"),
            "followers[3].localUrl", equalTo("http://1.1.1.1:0001"),
            "followers[4].localUrl", equalTo("http://1.1.1.2:0002"),
            "followers[5].localUrl", equalTo("http://1.1.1.6:0006"),
            //following lists
            "followers[0].requestedToFollow", equalTo(["http://cloud.pipe"]),
            "followers[1].requestedToFollow", equalTo(["http://1.1.1.3:0003", "http://cloud.pipe"]),
            "followers[2].requestedToFollow", equalTo(["http://1.1.1.3:0003", "http://cloud.pipe"]),
            "followers[3].requestedToFollow", equalTo(["http://1.1.1.4:0004", "http://1.1.1.3:0003", "http://cloud.pipe"]),
            "followers[4].requestedToFollow", equalTo(["http://1.1.1.4:0004", "http://1.1.1.3:0003", "http://cloud.pipe"]),
            "followers[5].requestedToFollow", equalTo(["http://1.1.1.5:0005", "http://1.1.1.3:0003", "http://cloud.pipe"]),
        )
    }

    def "authenticated user without deletion role cannot delete from the database"() {
        given: "We register two nodes"
        registerNode(1234, "http://1.1.1.1:1234", 123, FOLLOWING, ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, FOLLOWING, ["http://y"])

        when: "node is deleted"
        def encodedCredentials = "${USERNAME_TWO}:${PASSWORD_TWO}".bytes.encodeBase64().toString()
        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
        .when()
            .delete("/v2/registry/1234/1.1.1.1")
        .then()
            .statusCode(403)

        then: "registry is unaffected by request"
        def request =
            given()
                .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .when()
                .get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    def "anonymous user without deletion role cannot delete from the database"() {
        given: "We register two nodes"
        registerNode(1234, "http://1.1.1.1:1234", 123, FOLLOWING, ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, FOLLOWING, ["http://y"])

        when: "node is deleted"
        given()
            .contentType("application/json")
        .when()
            .delete("/v2/registry/1234/1.1.1.1")
        .then()
            .statusCode(401)

        then: "registry is unaffected by request"
        def request =
            given()
                .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .when()
                .get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    @Unroll
    def "when a bootstrap is requested, a bootstrap request is saved for that node"() {
        when: "bootstrap is called"
        given()
            .contentType("application/json")
        .when()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .body("""{
                "nodeRequests": ["0000", "1111", "2222"], 
                "bootstrapType": "$bootstrapString"
            }""")
            .post("/v2/registry/bootstrap")
        .then()
            .statusCode(statusCode)

        then: "node request is saved"
        def rows = sql.rows("SELECT * FROM node_requests;")

        rows.get(0).getProperty("host_id") == "0000"
        rows.get(0).getProperty("bootstrap_requested") != null
        rows.get(0).getProperty("bootstrap_type") == bootstrapType
        rows.get(0).getProperty("bootstrap_received") == null

        rows.get(1).getProperty("host_id") == "1111"
        rows.get(1).getProperty("bootstrap_requested") != null
        rows.get(1).getProperty("bootstrap_type") == bootstrapType
        rows.get(1).getProperty("bootstrap_received") == null

        rows.get(2).getProperty("host_id") == "2222"
        rows.get(2).getProperty("bootstrap_requested") != null
        rows.get(2).getProperty("bootstrap_type") == bootstrapType
        rows.get(2).getProperty("bootstrap_received") == null

        where:
        bootstrapString     | statusCode | bootstrapType
        "PROVIDER"          | 200        | BootstrapType.PROVIDER.toString()
        "PIPE_AND_PROVIDER" | 200        | BootstrapType.PIPE_AND_PROVIDER.toString()
    }

    def "when bootstrap is called with invalid bootstrap type, a 400 is returned"() {
        when: "bootstrap is called"
        given()
            .contentType("application/json")
        .when()
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .body("""{
                "nodeRequests": ["0000", "1111", "2222"], 
                "bootstrapType": "INVALID"
             }""")
            .post("/v2/registry/bootstrap")
        .then()
            .statusCode(400)

        then: "node request is not saved"
        def rows = sql.rows("SELECT * FROM node_requests;")
        rows.size() == 0
    }

    def "registry endpoint called by the UI accepts identity tokens"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA, equalTo("someTraceId"))

        when: "We can get info from registry with an identity token"
        given()
            .header("Authorization", "Bearer $identityToken")
            .header("TraceId", "someTraceId")
        .when()
            .get("/v2/registry")
        .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo(OK.toString())
        )

        then: 'identity was called'
        identityMock.verify()
    }

    def "logs have trace_ids in them"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA, equalTo("someTraceId"))

        when: "We can get info from registry with an identity token"
        given()
            .header("Authorization", "Bearer $identityToken")
            .header("TraceId", "someTraceId")
            .when()
            .get("/v2/registry")
            .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo(OK.toString())
            )

        then: "logs contain trace_id in them"
        TestAppender.getEvents().stream()
            .filter { it.loggerName.contains("com.tesco.aqueduct") }
            .allMatch() { it.MDCPropertyMap.get("trace_id") == "someTraceId" }
    }

    def "when no trace id is provided logs have a generated trace_id in them with the correct prefix"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA, startsWith("aq-"))

        when: "We can get info from registry with an identity token"
        given()
            .header("Authorization", "Bearer $identityToken")
            .when()
            .get("/v2/registry")
            .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo(OK.toString())
            )

        then: "logs contain trace_id in them with the correct prefix"
        TestAppender.getEvents().stream()
            .filter { it.loggerName.contains("com.tesco.aqueduct") }
            .allMatch() { it.MDCPropertyMap.get("trace_id").startsWith("aq-") }
    }

    def acceptSingleIdentityTokenValidationRequest(
            String clientIdAndSecret, String identityToken, String clientUserUID, Matcher<String> traceIdMatcher) {
        def json = JsonOutput.toJson([access_token: identityToken])

        identityMock.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                queries("client_id": [clientIdAndSecret])
                body(json, "application/json")
                .header("TraceId", everyItem(traceIdMatcher))
                called(1)

                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "UserId": "${clientUserUID}",
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

    private static void registerNode(group, url, offset = 0, status = INITIALISING.toString(), following = [CLOUD_PIPE_URL]) {
        given()
            .contentType("application/json")
            .header("Authorization", "Basic $USERNAME_ENCODED_CREDENTIALS")
            .body("""{
                "group": "$group",
                "localUrl": "$url",
                "offset": "$offset",
                "pipe": {"pipeState" : "$UP_TO_DATE", "v":"1.0"},
                "status": "$status",
                "following": ["${following.join('", "')}"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(200)
    }

    private static Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
