package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.client.PipeServiceInstance
import com.tesco.aqueduct.registry.client.ServiceList
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.reactivex.Single
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class AuthenticatePipeReadFilterIntegrationSpec extends Specification {

    @Shared @AutoCleanup ErsatzServer server
    @Shared @AutoCleanup("stop") ApplicationContext context

    def type = "type"
    def location = "someLocation"
    def offset = 100L

    InternalHttpPipeClient client
    def identityToken= Mock(IdentityToken) {
        getAccessToken() >> "someToken"
    }
    def tokenProvider = Mock(TokenProvider) {
        retrieveIdentityToken() >> Single.just(identityToken)
    }

    def "identity token is used when settings are provided calling the cloud"() {
        given: "latest offset requiring authentication"
        server = new ErsatzServer({
            expectations {
                get("/pipe/100") {
                    queries(type: type, location: location)
                    called(1)
                    header("Authorization", "Bearer someToken")

                    responder {
                        contentType('application/json')
                        body("""[
                        {
                            "type": "$type",
                            "key": "x",
                            "contentType": "contentType",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""".bytes)
                    }
                }
            }
        })
        server.start()

        and: "a authorized client"
        def config = new DefaultHttpClientConfiguration()
        context = ApplicationContext
            .build()
            .properties(
                "authentication.read-pipe.username": "admin",
                "authentication.read-pipe.password": "my-password",
                "pipe.http.client.attempts": 1,
                "pipe.http.client.delay": "500ms",
                "pipe.http.client.reset": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl()
            )
            .build()
            .registerSingleton(tokenProvider)
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(config, new URL(server.getHttpUrl())),
                File.createTempFile("provider", "properties")
        ))
            .start()

        client = context.getBean(InternalBrotliHttpPipeClient)

        when:
        client.httpRead([type], offset, location).body()

        then:
        server.verify()
    }

    def "basic auth is used when settings are provided calling another node"() {
        given: "latest offset requiring authentication"
        server = new ErsatzServer({
            authentication {
                basic 'admin', 'my-password'
            }
            expectations {
                get("/pipe/100") {
                    queries(type: type, location: location)
                    called(1)

                    responder {
                        contentType('application/json')
                        body("""[
                        {
                            "type": "$type",
                            "key": "x",
                            "contentType": "contentType",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""".bytes)
                    }
                }
            }
        })
        server.start()

        and: "a authorized client"
        def config = new DefaultHttpClientConfiguration()
        context = ApplicationContext
                .build()
                .properties(
                    "authentication.read-pipe.username": "admin",
                    "authentication.read-pipe.password": "my-password",
                    "pipe.http.client.attempts": 1,
                    "pipe.http.client.delay": "500ms",
                    "pipe.http.client.reset": "1s",
                    "pipe.http.client.url": "cloudIP",
                    "registry.http.client.url": "cloudIP"
                )
                .build()
                .registerSingleton(TokenProvider, Mock(TokenProvider))
                .registerSingleton(new ServiceList(
                        new DefaultHttpClientConfiguration(),
                        new PipeServiceInstance(config, new URL(server.getHttpUrl())),
                        File.createTempFile("provider", "properties")
                ))
                .start()

        client = context.getBean(InternalBrotliHttpPipeClient)

        when:
        client.httpRead([type], offset, location).body()

        then:
        server.verify()
    }
}
