package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.HttpHeaders
import com.tesco.aqueduct.pipe.api.PipeState
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.pipe.codec.BrotliCodec
import com.tesco.aqueduct.registry.client.PipeLoadBalancer
import com.tesco.aqueduct.registry.client.PipeServiceInstance
import com.tesco.aqueduct.registry.client.SelfRegistrationTask
import com.tesco.aqueduct.registry.client.ServiceList
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.exceptions.HttpClientResponseException
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
    ServiceList serviceList

    BrotliCodec brotliCodec = new BrotliCodec(4, false)

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
                    "registry.http.client.url": "http://does.not.exist",
                    "micronaut.caches.health-check.maximum-size": 20,
                    "micronaut.caches.health-check.expire-after-write": "5s",
                    "pipe.http.client.attempts": 1,
                    "pipe.http.client.delay": "1ms",
                    "pipe.http.client.reset": "5ms",
                    "pipe.http.latest-offset.attempts": 1,
                    "pipe.http.latest-offset.delay": "1s"
                ] + properties
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .registerSingleton(Mock(TokenProvider))
            .registerSingleton(BrotliCodec, brotliCodec)
            .registerSingleton(new ServiceList(
                new DefaultHttpClientConfiguration(),
                new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL("http://does.not.exist")),
                File.createTempFile("provider", "properties")
            ))
            .start()

        def brotliClient = context.getBean(InternalBrotliHttpPipeClient)
        context.registerSingleton(new HttpPipeClient(brotliClient, brotliCodec, 240))

        client = context.getBean(HttpPipeClient)
        loadBalancer = context.getBean(PipeLoadBalancer)
        serviceList = context.getBean(ServiceList)
    }

    def "client successfully calls first option, then falls back to second on failure"() {
        given: "two urls in the registry"
        setupContext()

        ErsatzServer serverB = new ErsatzServer()

        serverB.start()
        serviceList.update([URL(serverA.httpUrl), URL(serverB.httpUrl)])

        serverA.expectations {
            def offset = 0
            get("/pipe/$offset") {
                called(1)

                responder {
                    header(HttpHeaders.RETRY_AFTER, "0")
                    header(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.toString())
                    header(HttpHeaders.GLOBAL_LATEST_OFFSET, "10")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z",
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
                    header(HttpHeaders.RETRY_AFTER, "0")
                    header(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.toString())
                    header(HttpHeaders.GLOBAL_LATEST_OFFSET, "10")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z",
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        when: "messages are read from server A until retry fails"
        def firstMessages = client.read([], 0, ["locationUuid"])
        client.read([], 1, ["locationUuid"])

        then: "messages are received and an exception is thrown"
        thrown(HttpClientResponseException)
        firstMessages.messages*.key == ["x0"]
        serverA.verify()

        when: "the error has been recorded"
        serviceList.stream().findFirst().ifPresent({ c -> c.isUp(false) })

        and: "client reads again"
        def secondMessages = client.read([], 1, ["locationUuid"])

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
                    header(HttpHeaders.RETRY_AFTER, "0")
                    header(HttpHeaders.PIPE_STATE, PipeState.UP_TO_DATE.toString())
                    header(HttpHeaders.GLOBAL_LATEST_OFFSET, "10")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "type",
                            "key": "x$offset",
                            "contentType": "application/json",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        and: "loadbalancer is updated with the server url including the base path"
        def serverUrl = serverA.getHttpUrl() + basePath
        serviceList.update([ URL(serverUrl) ])

        when: "the client calls the server"
        client.read([], 0, ["locationUuid"])

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

        serviceList.update([URL(server.httpUrl)])

        when: "we marked server as unhealthy"
        serviceList.stream().findFirst().ifPresent({ c -> c.isUp(false) })

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
