package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.registry.SelfRegistrationTask
import com.tesco.aqueduct.registry.PipeLoadBalancer
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.exceptions.HttpClientException
import spock.lang.AutoCleanup
import spock.lang.Specification

import static org.hamcrest.Matchers.greaterThanOrEqualTo

/**
 * Tests integration between PipeErrorInterceptor on HttpPipeClient with PipeLoadBalancer from registry-client
 */
@Newify(URL)
class PipeLoadBalancerIntegrationSpec extends Specification {

    @AutoCleanup ErsatzServer serverA
    @AutoCleanup("stop") ApplicationContext context

    HttpPipeClient client
    PipeLoadBalancer loadBalancer

    def setup() {
        serverA = new ErsatzServer()
        serverA.start()
    }

    void setupContext(Map properties=[:]) {
        context = ApplicationContext
            .build()
            .properties(
                [
                    "pipe.http.client.url": "http://does.not.exist",
                    "pipe.tags": [:]
                ] + properties
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .start()

        client = context.getBean(HttpPipeClient)
        loadBalancer = context.getBean(PipeLoadBalancer)
    }

    def "client successfully calls first option, then falls back to second on failure"() {
        given: "two urls in the registry"
        setupContext()

        ErsatzServer serverB = new ErsatzServer()

        serverB.start()
        loadBalancer.update([URL(serverA.httpUrl), URL(serverB.httpUrl)])

        serverA.expectations {
            def offset = 0
            get("/pipe/$offset") {
                called(1)

                responder {
                    header("Retry-After", "0")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "tags": { "example":"value"}, 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
            get("/pipe/${offset + 1}") {
                called(greaterThanOrEqualTo(1))

                responder {
                    code(500)
                }
            }
        }

        serverB.expectations {
            def offset = 1
            get("/pipe/$offset") {
                called(1)

                responder {
                    header("Retry-After", "0")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "tags": { "example":"value"}, 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        when: "messages are read from server A until retry fails"
        def firstMessages = client.read([:], 0)
        client.read([:], 1)

        then: "messages are received and an exception is thrown"
        thrown(HttpClientException)
        firstMessages.messages*.key == ["x0"]
        serverA.verify()

        when: "client reads again"
        def secondMessages = client.read([:], 1)

        then: "second server is called"
        secondMessages.messages*.key == ["x1"]
        serverB.verify()
    }

    def "a client respects the base path of the service it is calling"() {
        given: "A aqueduct pipe node with a specified base path"
        setupContext()
        def basePath = "/foo"
        serverA.expectations {
            def offset = 0
            get("$basePath/pipe/$offset") {
                called(1)
                responder {
                    header("Retry-After", "0")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "tags": { "example":"value"}, 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        and: "loadbalancer is updated with the server url including the base path"
        def serverUrl = serverA.getHttpUrl() + basePath
        loadBalancer.update([ URL(serverUrl) ])

        when: "the client calls the server"
        client.read([:], 0)

        then: "the server has been called on the right path"
        serverA.verify()
    }

    def "An unhealthy server is retried after the unhealthy expiry duration has passed"() {
        given: "a load balancer with a list of urls"
        setupContext("pipe.http.client.healthcheck.interval": "1s")
        ErsatzServer server = new ErsatzServer()
        server.start()

        server.expectations {
            get("/pipe/_status") {
                called(greaterThanOrEqualTo(1))

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(200)
                }
            }
        }

        loadBalancer.update([URL(server.httpUrl)])

        when: "we marked server as unhealthy"
        loadBalancer.recordError()

        then: "There should be no UP servers"
        loadBalancer.getFollowing().isEmpty()

        when: "after the health check duration"
        sleep(2000)

        then: "the first server is updated as healthy"
        server.verify()
        loadBalancer.getFollowing().first().toString() == server.httpUrl

        cleanup:
        server
    }
}
