package com.tesco.aqueduct.registry

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.registry.model.Node
import io.micronaut.context.ApplicationContext
import spock.lang.Specification

import java.time.ZonedDateTime

class AuthenticateNodeRegistryFilterSpec extends Specification {

    private static final URL MY_HOST = new URL("http://localhost")
    String host1 = "http://host1"
    String host2 = "http://host2"

    def "basic auth is used when config is provided"() {
        given: "Server expecting auth"

        def server = new ErsatzServer({
            authentication {
                basic 'admin', 'my-password'
            }
            expectations {
                post("/v2/registry") {
                    called(1)

                    responder {
                        contentType('application/json')
                        body(""" [ "$host1", "$host2" ]""")
                    }
                }
            }
        })

        and: "client configured with auth"
        def context = ApplicationContext
            .build()
            .properties(
                "authentication.read-pipe.username": "admin",
                "authentication.read-pipe.password": "my-password",
                "pipe.http.register.retry.interval": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl() + "/v2"
            )
            .build()
            .start()

        def client = context.getBean(RegistryClient)

        and: "a node to register"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status("initialising")
            .lastSeen(ZonedDateTime.now())
            .build()

        when: "the node is registered"
        client.register(myNode)

        then: "the server receives the auth"
        server.verify()
    }

    def "basic auth used when server isn't expecting it still works"() {
        given: "Server expecting auth"
        def server = new ErsatzServer({
            expectations {
                post("/v2/registry") {
                    called(1)

                    responder {
                        contentType('application/json')
                        body(""" [ "$host1", "$host2" ]""")
                    }
                }
            }
        })

        and: "client configured with auth"
        def context = ApplicationContext
            .build()
            .properties(
                "authentication.read-pipe.username": "admin",
                "authentication.read-pipe.password": "my-password",
                "pipe.http.register.retry.interval": "1s",
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl() + "/v2"
            )
            .build()
            .start()

        def client = context.getBean(RegistryClient)

        and: "a node to register"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status("initialising")
            .lastSeen(ZonedDateTime.now())
            .build()

        when: "the node is registered"
        client.register(myNode)

        then: "the server receives the auth"
        server.verify()
    }

    def 'calling the registry will pass the authentication credentials using a http client filter'() {
        given: "authentication credentials are provided"
        def username = "username"
        def password = "password"

        and: "an endpoint with a base path exists"
        def encodedCredentials = "$username:$password".bytes.encodeBase64().toString()

        def server = new ErsatzServer({
            expectations {
                post("/v2/registry") {
                    header("Accept-Encoding", "gzip, deflate")
                    header("Authorization", "Basic $encodedCredentials")
                    called(1)
                    responder {
                        contentType("application/json")
                        body(""" [ "$host1", "$host2" ]""")
                    }
                }
            }
        })

        and: 'an application context is created'
        def context = ApplicationContext
            .build()
            .properties(
                "pipe.http.client.url": server.getHttpUrl(),
                "registry.http.client.url": server.getHttpUrl() + "/v2",
                "pipe.http.register.retry.interval": "1s",
                "authentication.read-pipe.username": username,
                "authentication.read-pipe.password": password
            )
            .build()
            .start()

        when: "calling the registry endpoint with a given node"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status("initialising")
            .lastSeen(ZonedDateTime.now())
            .build()

        def client = context.getBean(RegistryClient)
        def urls = client.register(myNode)

        then: "a list of expected urls are returned"
        urls == [new URL(host1), new URL(host2)]

        and: "an exception is not raised"
        noExceptionThrown()
    }

    def 'the base path of the client is respected when authenticating using a http client filter'() {
        given: "a base path exists"
        def basePath = "/messaging"

        and: "authentication credentials are provided"
        def username = "username"
        def password = "password"

        and: "an endpoint with a base path exists"
        def encodedCredentials = "$username:$password".bytes.encodeBase64().toString()

        def server = new ErsatzServer({
            expectations {
                post("/messaging/v2/registry") {
                    header("Accept-Encoding", "gzip, deflate")
                    header("Authorization", "Basic $encodedCredentials")
                    called(1)
                    responder {
                        contentType("application/json")
                        body(""" [ "$host1", "$host2" ]""")
                    }
                }
            }
        })

        and: 'an application context is created'
        def context = ApplicationContext
            .build()
            .properties(
                "pipe.http.client.url": server.getHttpUrl() + basePath,
                "registry.http.client.url": server.getHttpUrl() + basePath + "/v2",
                "pipe.http.register.retry.interval": "1s",
                "authentication.read-pipe.username": username,
                "authentication.read-pipe.password": password
            )
            .build()
            .start()

        when: "calling the registry endpoint with a given node"
        def myNode = Node.builder()
            .group("1234")
            .localUrl(MY_HOST)
            .offset(0)
            .status("initialising")
            .lastSeen(ZonedDateTime.now())
            .build()

        def client = context.getBean(RegistryClient)
        def urls = client.register(myNode)

        then: "a list of expected urls are returned"
        urls == [new URL(host1), new URL(host2)]

        and: "an exception is not raised"
        noExceptionThrown()
    }
}
