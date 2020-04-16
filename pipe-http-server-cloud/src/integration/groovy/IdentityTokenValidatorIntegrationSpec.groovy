import com.stehno.ersatz.Decoders
import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.Cluster
import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.pipe.api.Reader
import com.tesco.aqueduct.pipe.http.PipeStateProvider
import com.tesco.aqueduct.pipe.storage.CentralInMemoryStorage
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class IdentityTokenValidatorIntegrationSpec extends Specification {

    static final int CACHE_EXPIRY_SECONDS = 1
    static final String VALIDATE_TOKEN_BASE_PATH = '/v4/access-token/auth/validate'
    static final String USERNAME = "username"
    static final String PASSWORD = "password"
    static final String encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
    static final InMemoryStorage storage = new CentralInMemoryStorage(10, 600)

    static final String clientId = UUID.randomUUID().toString()
    static final String secret = UUID.randomUUID().toString()
    static final String clientIdAndSecret = "trn:tesco:cid:${clientId}:${secret}"

    static final String userUIDA = UUID.randomUUID()
    static final String clientUserUIDA = "trn:tesco:uid:uuid:${userUIDA}"
    static final String userUIDB = UUID.randomUUID()
    static final String clientUserUIDB = "trn:tesco:uid:uuid:${userUIDB}"

    static final String validateTokenPath = "${VALIDATE_TOKEN_BASE_PATH}?client_id=${clientIdAndSecret}"

    @Shared @AutoCleanup ErsatzServer identityMock
    @Shared @AutoCleanup ApplicationContext context

    def setupSpec() {
        identityMock = new ErsatzServer({
            decoder('application/json', Decoders.utf8String)
            reportToConsole()
        })

        def pipeStateProvider = Mock(PipeStateProvider) {
            getState(_ as List, _ as Reader) >> new PipeStateResponse(true, 100)
        }

        def locationResolver = Mock(LocationResolver) {
            resolve(_) >> [new Cluster("cluster1")]
        }


        identityMock.start()

        context = ApplicationContext
            .build()
            .mainClass(EmbeddedServer)
            .properties(
                parseYamlConfig(
                    """
                micronaut.security.enabled: true
                micronaut.security.token.jwt.enabled: true
                micronaut.security.token.jwt.bearer.enabled: true
                micronaut.caches.identity-cache.expire-after-write: ${CACHE_EXPIRY_SECONDS}s
                authentication:
                  users:
                    $USERNAME:
                      password: $PASSWORD
                      roles:
                        - PIPE_READ
                  identity:
                    url: ${identityMock.getHttpUrl()}
                    validate.token.path: $validateTokenPath
                    client:
                        id: "someClientId"
                        secret: "someClientSecret"
                    users:
                      userA:
                        clientId: $clientUserUIDA
                        roles:
                          - PIPE_READ
                      userB:
                        clientId: $clientUserUIDB
                        roles:
                          - NOT_A_REAL_ROLE
                """
                )
            )
            .build()

        context
            .registerSingleton(Reader, storage, Qualifiers.byName("local"))
            .registerSingleton(pipeStateProvider)
            .registerSingleton(locationResolver)
            .start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def setup() {
        identityMock.clearExpectations()
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'Http status OK when using a valid identity token with the correct role'() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA)

        when: 'A secured URL is accessed with the identity token as Bearer'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.OK.code)

        then: 'identity was called'
        identityMock.verify()
    }

    def 'Http status unauthorised when using a valid identity token without correct role'() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDB)

        when: 'A secured URL is accessed with the identity token as Bearer'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.FORBIDDEN.code)

        then: 'identity was called'
        identityMock.verify()
    }

    def 'Http status OK when using valid basic auth credentials'() {
        expect: 'A secured URL is accessed with the basic auth'
        RestAssured.given()
            .header("Authorization", "Basic $encodedCredentials")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def "Client receives unauthorised if no identity token provided."() {
        expect: 'Accessing a secured URL without authenticating'
        RestAssured.given()
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def 'Returns unauthorised when using an invalid identity token'() {
        given: 'Identity denies a validateToken request'
        denySingleIdentityTokenValidationRequest()

        when: 'A secured URL is accessed with the identity token as Bearer'
        def identityToken = UUID.randomUUID().toString()
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)

        then: 'identity was called'
        identityMock.verify()
    }

    def 'Returns unauthorised when using incorrect basic auth credentials'() {
        when: 'Identity denies a validateToken request'
        denySingleIdentityTokenValidationRequest()

        then: 'A secured .URL is accessed with the basic auth'
        def incorrectEncodedCredentials = "incorrectUser:incorrectPassword".bytes.encodeBase64().toString()

        RestAssured.given()
            .header("Authorization", "Basic $incorrectEncodedCredentials")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def "Token validation requests are cached for the duration specified in the config"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA)

        when: 'A secured URL is multiple times with the identity token as Bearer'
        makeValidRequest(identityToken)
        makeValidRequest(identityToken)
        makeValidRequest(identityToken)

        then: 'Identity is only called once'
        identityMock.verify()

        when: 'Identity is called after the expiry period'
        identityMock.clearExpectations()
        sleep CACHE_EXPIRY_SECONDS * 1000

        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA)
        makeValidRequest(identityToken)

        then: 'Identity is called again'
        identityMock.verify()
    }

    @Unroll
    def "A #valid identity token is used to call pipe"() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUID)

        when: 'A secured URL is accessed with the identity token as Bearer'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(statusCode)

        then: 'identity was called'
        identityMock.verify()

        where:
        valid             | clientUID      | statusCode
        "whitelisted"     | clientUserUIDA | HttpStatus.OK.code
        "non whitelisted" | "incorrectUID" | HttpStatus.FORBIDDEN.code
    }

    def acceptSingleIdentityTokenValidationRequest(String clientIdAndSecret, String identityToken, String clientUserUID) {
        def json = JsonOutput.toJson([access_token: identityToken])

        identityMock.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                queries("client_id": [clientIdAndSecret])
                body(json, "application/json")
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

    def denySingleIdentityTokenValidationRequest() {
        identityMock.expectations {
            post(VALIDATE_TOKEN_BASE_PATH) {
                called(1)
                responder {
                    header("Content-Type", "application/json;charset=UTF-8")
                    body("""
                        {
                          "Status": "INVALID"
                        }
                    """)
                }
            }
        }
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }

    def makeValidRequest(String identityToken) {
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/pipe/0?location=someLocation")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def acceptSingleIdentityTokenValidationRequestPerToken(String clientIdAndSecret, String... identityTokens) {
        identityTokens.each { token ->
            acceptSingleIdentityTokenValidationRequest(clientIdAndSecret, token)
        }
    }
}
