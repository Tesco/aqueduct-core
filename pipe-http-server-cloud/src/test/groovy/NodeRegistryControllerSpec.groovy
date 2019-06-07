import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.registry.InMemoryNodeRegistry
import com.tesco.aqueduct.registry.NodeRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

import static io.restassured.RestAssured.given
import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.*

class NodeRegistryControllerSpec extends Specification {

    static String cloudPipeUrl = "http://cloud.pipe"
    static final String USERNAME = "username"
    static final String PASSWORD = "password"
    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    void setup() {
        context = ApplicationContext
            .build()
            .properties(
                // enabling security to prove that registry is accessible anyway
                "micronaut.security.enabled": true,
                "authentication.read-pipe.username": USERNAME,
                "authentication.read-pipe.password": PASSWORD,
                "authentication.read-pipe.runscope-username": RUNSCOPE_USERNAME,
                "authentication.read-pipe.runscope-password": RUNSCOPE_PASSWORD
            )
            .build()
            .registerSingleton(NodeRegistry, new InMemoryNodeRegistry(new URL(cloudPipeUrl), Duration.ofDays(1)))
            .registerSingleton(MessageReader, Mock(MessageReader))
            .start()

        server = context.getBean(EmbeddedServer)

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'expect unauthorized when not providing username and password to registry'(){
        given:
        server.start()

        expect:
        given()
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$cloudPipeUrl"]
            }""")
        .when()
            .post("/registry")
        .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    void "Can post to registry"() {
        setup: "A started server"
        server.start()

        expect: "We can post info to the registry"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()

        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$cloudPipeUrl"]
            }""")
        .when()
            .post("/registry")
        .then()
            .statusCode(200)
            .body("", equalTo([cloudPipeUrl]))
    }

    void "Can get registry summary"() {
        setup: "A started server"
        server.start()

        expect: "We can get info from registry"
        given()
            .when()
            .get("/registry")
            .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo("ok")
            )
    }

    void "Registered nodes are returned"() {
        given: "We register a node"
        server.start()
        registerNode(6735, "http://1.1.1.1:1234", 123, "status", ["http://x"])

        when: "we get summary"
        def request = when().get("/registry")

        then:
        request.then()
            .statusCode(200)
            .body(
                "followers[0].group", equalTo("6735"),
                "followers[0].localUrl", equalTo("http://1.1.1.1:1234"),
                "followers[0].offset", equalTo("123"),
                "followers[0].status", equalTo("status"),
                "followers[0].following", contains("http://x"),
                "followers[0].requestedToFollow", contains("http://cloud.pipe")
            )
    }

    @Unroll
    void "Summary can filter by stores - #comment"() {
        given: "We register nodes from different groups"
        server.start()
        registerNode("a", "http://a")
        registerNode("b", "http://b")
        registerNode("c", "http://c")

        when: "we get summary"
        def request = given()
            .urlEncodingEnabled(false) // to disable changing ',' to %2Cc as this ',' is special character here, not part of the value
            .when().get("/registry$query")

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

    private static void registerNode(group, url, offset=0, status="initialising", following=[cloudPipeUrl]) {
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
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
            .statusCode(200)
    }
}
